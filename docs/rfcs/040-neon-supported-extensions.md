## Goals:

- Support multiple versions of extensions available in compute image
    - This is needed to support installed extensions that are not backward compatible.
    - This will be useful for [multi-version compute images](https://github.com/neondatabase/neon/issues/6685).
    - This will be useful to test new versions of extensions before updating them in the compute image.

- Allow overriding extension version for a specific project/user.

- Uncouple extension version from compute build_tag to be able to release them with different pace

- Support private extensions.

- Improve visibility of extension versions used in our compute images.

- Automate extension updates as much as possible: 
    - track available versions
    - run backward compatibility tests
    - prepare update PR (if tests pass)

## Out of scope goals:

- Ensure compute image rollbacks are safe.

    Imagine, there is extension that adds function `foo_2()` in release 1.2. If user creates extension with version 1.2 and after that we rollback to compute image that only contains extension 1.1, `foo_2()` call will fail in a newly started compute. We only rollback compute if release went wrong, and rollback is supposed to be short-living, so this is not an urgent isue.
    Though, new design, allows manual cplane table editing to map extension version to compute version.

- Maintain diffferent system library versions that are needed for different extension versions. I.e. `my_ext-1.0` depends on `libicu42` and `my_ext-2.0` depends on `libicu52` (numbers are random).
    TODO: find at least one example of such issue.

## Involved components:

- cplane
- compute
- postgres
- build compute image CI job
- build-custom-extensions CI job
- remote extensions s3 bucket
- extension testing infrastructure
- backward compatibility testing infrastructure


## Existing architecture

We have 2 ways to support extensions:
1. Put extensions directly into [Dockerfile.compute-node](https://github.com/neondatabase/neon/blob/main/Dockerfile.compute-node)
2. use  [custom extensions](https://github.com/neondatabase/build-custom-extensions/blob/main/README.md)

## Proposed solution:

TLDR: Improve metadata handling and testing for custom extensions and move all extensions to this workflow.

### 1. Keep mapping of supported extension versions in a cplane table:

Create new cplane table:

```
create table neon_supported_extension(
    id serial primary key,
    neon_available_extension_id integer references neon_available_extension(id),
    compute_image_version integer references compute_releases(version), -- based on independent compute releases RFC
    compute_major_version integer references compute_releases(major_version) -- TODO: there is no such column yet.
    custom_extension_spec -- TODO - define this
    is_available_for_all boolean default true -- if false, we need to explicity enable it for a project/user,
    control text, -- control file content. TODO: it doesn't really belong here, but where to put it?
    library_index text[], -- some extensions contain libraries that do not match extension_name. To implement on demand download, we need to map library_name->id.
); 
```

1. When new version of extension is built add a record to `neon_supported_extension` table and push it to remote_extensions s3 with a path `s3://neon-extensions/<neon_supported_extension.id>`. Q: Do we want to make it more redundant, or just ID is enough? i.e <compute_major_version>/<neon_supported_extension.name>/<neon_supported_extension.id>.

    We may keep some extensions in the compute image, i.e. popular extensions or ones that are included in `shared_preload_libraries`, but this will be only a cache. Eventually, we must be able to serve all extensions as remote_extensions.


2. When compute is starting, cplane collects all extensions that are available for this compute image version and adds their `neon_supported_extension.id`, `neon_supported_extension.control` and `neon_supported_extension.library_index`, down to compute spec. 

    To find available extensions for compute with `$current_image_version` and `$current_major_version`:

    ```
    SELECT id, control, library_index
    FROM neon_supported_extension
    WHERE  $current_major_version = compute_major_version -- for multi-version compute images, this will be an array of versions
    AND is_available_for_all
    AND  $current_image_version >= compute_image_version
    ORDER BY (compute_image_version, id) DESC
    LIMIT 1;
    ```

    Q: do we really need control file in the compute spec? Can we just add them as files to compute image when image is built?

    To override extension version (or enable private extension) for a specific project/user, we can add a column to the project/user table, i.e. `extension_ids array references neon_supported_extension(id)`. This column will replace currently existing `remote_extensions` part of `pg_settings`.

3. Inside compute image, we will build path to custom extensions s3 based on provided spec and download them into the respective library_path locasion in compute image.
    Almost the same way as we do now, but use `s3://neon-extensions/<neon_supported_extension.id>` as archive path.

4. When `CREATE EXTENSION myextension;` is called, we will intercept the request to the file and load the extension from the custom path, based on compute version and extension name. This will allow support of multi-version compute images. This is also a good place to track extension usage statistics.

    Alternatively we can just symlink `/usr/local/pgsql/lib/extension/<neon_supported_extension.id>/*` files to default path `/usr/local/pgsql/lib/` as soon as we know, which version of postgres is runnnig. This approach doesn't require postgres patch. Will it be enough to support multiple versions of extenension?

5. There is a special case of backward incompatibe minor version upgrades of extension that use the same library name. I.e. `my_ext--1.0` and `my_ext--1.1` both use `my_ext.so` and version 1.1 drops some function that was present in 1.0. This is a bad practice and shouldn't happen often.

    In this case, we will have to add special rule to `download_extension_file_hook()` to check installed version of extension and map requested `my_ext.so` to `my_ext-1.0.so` or `my_ext-1.1.so`. Build extension step must be aware of this change too - it will add suffix to extension library file and reflect this in `library_index[]`.

    Note that this will also work if an extension is actually a pack of extensions with different versions (i.e. `postgis 3.3` that includes `postgis_sfcgal v 1.3.8`).

    TODO: Find at least one example of such backward incompatible extension.

### 2. Keep track of available versions of extensions:

Create new cplane table that tracks data needed to build the extension:

```
create table neon_available_extension(
    id serial primary key,
    name text not null,
    version text not null,
    release_date date not null,
    download_url text not null,
    neon_compatibility_patch_filename text -- We sometimes patch extensions, see `Dockerfile.compute-node`, i.e. pgvector.patch.
);
```

We can fill it manually at first with the same data we now have in `Dockerfile.compute-node`.
Later, we can automate this and periodically collect new versions of available extensions using github API, i.e.
```
  https://api.github.com/repos/OWNER/REPO/releases/latest
```

### 3. Update extensions when new version of extension is available:

1. Run backward compatibility tests for the new version of the extension.

    Connect to the project that has previous version of the extension installed, using compute image with the new version of the extension.
    Run `ALTER EXTENSION my_ext UPDATE` and run regression tests. If everything is fine, we can safely update the extension. If not, see workaround in 1.5.

2. Update extension path in build extensions CI

3. Add new row to the `neon_supported_extension` table

    Compute will pick up new version automatically on next release.
