# Push and pull between pageservers

Here is a proposal about implementing push/pull mechanics between pageservers. We also want to be able to push/pull to S3 but that would depend on the exact storage format so we don't touch that in this proposal.

## Origin management

The origin represents connection info for some remote pageserver. Let's use here same commands as git uses except using explicit list subcommand (git uses `origin -v` for that).

```
zenith origin add <name> <connection_uri>
zenith origin list
zenith origin remove <name>
```

Connection URI a string of form `postgresql://user:pass@hostname:port` (https://www.postgresql.org/docs/13/libpq-connect.html#id-1.7.3.8.3.6). We can start with libpq password auth and later add support for client certs or require ssh as transport or invent some other kind of transport.

Behind the scenes, this commands may update toml file inside .zenith directory.

## Push

### Pushing branch

```
zenith push mybranch cloudserver # push to eponymous branch in cloudserver
zenith push mybranch cloudserver:otherbranch # push to a different branch in cloudserver
```

Exact mechanics would be slightly different in the following situations:

1) Destination branch does not exist.

    That is the simplest scenario. We can just create an empty branch (or timeline in internal terminology) and transfer all the pages/records that we have in our timeline. Right now each timeline is quite independent of other timelines so I suggest skipping any checks that there is a common ancestor and just fill it with data. Later when CoW timelines will land to the pageserver we may add that check and decide whether this timeline belongs to this pageserver repository or not [*].

    The exact mechanics may be the following:

    * CLI asks local pageserver to perform push and hands over connection uri: `perform_push <branch_name> <uri>`.
    * local pageserver connects to the remote pageserver and runs `branch_push <branch_name> <timetine_id>`
        Handler for branch_create would create destination timeline and switch connection to copyboth mode.
    * Sending pageserver may start iterator on that timeline and send all the records as copy messages.

2) Destination branch exists and latest_valid_lsn is less than ours.

    In this case, we need to send missing records. To do that we need to find all pages that were changed since that remote LSN. Right now we don't have any tracking mechanism for that, so let's just iterate over all records and send ones that are newer than remote LSN. Later we probably should add a sparse bitmap that would track changed pages to avoid full scan.

3) Destination branch exists and latest_valid_lsn is bigger than ours.

    In this case, we can't push to that branch. We can only pull.

### Pulling branch

Here we need to handle the same three cases, but also keep in mind that local pageserver can be behind NAT and we can't trivially reuse pushing by asking remote to 'perform_push' to our address. So we would need a new set of commands:

* CLI calls `perform_pull <branch_name> <uri>` on local pageserver.
* local pageserver calls `branch_pull <branch_name> <timetine_id>` on remote pageserver.
* remote pageserver sends records in our direction

But despite the different set of commands code that performs iteration over records and receiving code that inserts that records can be the same for both pull and push.



[*] It looks to me that there are two different possible approaches to handling unrelated timelines:

1) Allow storing unrelated timelines in one repo. Some timelines may have parents and some may not.
2) Transparently create and manage several repositories in one pageserver.

But that is the topic for a separate RFC/discussion.
