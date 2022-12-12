#!/bin/bash

# If you save this in your path under the name "cargo-zclippy" (or whatever
# name you like), then you can run it as "cargo zclippy" from the shell prompt.
#
# If your text editor has rust-analyzer integration, you can also use this new
# command as a replacement for "cargo check" or "cargo clippy" and see clippy
# warnings and errors right in the editor.
# In vscode, this setting is Rust-analyzer>Check On Save:Command


# Not every feature is supported in macOS builds, e.g. `profiling`,
# avoid running regular linting script that checks every feature.
if [[ "$OSTYPE" == "darwin"* ]]; then
    # no extra features to test currently, add more here when needed
    cargo clippy --locked --all --all-targets -- -A unknown_lints -D warnings
else
    # * `-A unknown_lints` – do not warn about unknown lint suppressions
    #                        that people with newer toolchains might use
    # * `-D warnings`      - fail on any warnings (`cargo` returns non-zero exit status)
    cargo clippy --locked --all --all-targets --all-features -- -A unknown_lints -D warnings
fi
