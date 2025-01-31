#! /usr/bin/env python3

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Iterable


def scan_pageserver_log_for_errors(
    input: Iterable[str], allowed_errors: list[str]
) -> list[tuple[int, str]]:
    error_or_warn = re.compile(r"\s(ERROR|WARN)")
    errors = []
    for lineno, line in enumerate(input, start=1):
        if len(line) == 0:
            continue

        if error_or_warn.search(line):
            # Is this a torn log line?  This happens when force-killing a process and restarting
            # Example: "2023-10-25T09:38:31.752314Z  WARN deletion executo2023-10-25T09:38:31.875947Z  INFO version: git-env:0f9452f76e8ccdfc88291bccb3f53e3016f40192"
            if re.match("\\d{4}-\\d{2}-\\d{2}T.+\\d{4}-\\d{2}-\\d{2}T.+INFO version.+", line):
                continue

            # It's an ERROR or WARN. Is it in the allow-list?
            for a in allowed_errors:
                try:
                    if re.match(a, line):
                        break
                # We can switch `re.error` with `re.PatternError` after 3.13
                # https://docs.python.org/3/library/re.html#re.PatternError
                except re.error:
                    print(f"Invalid regex: '{a}'", file=sys.stderr)
                    raise
            else:
                errors.append((lineno, line))
    return errors


DEFAULT_PAGESERVER_ALLOWED_ERRORS = (
    # All tests print these, when starting up or shutting down
    ".*wal receiver task finished with an error: walreceiver connection handling failure.*",
    ".*Shutdown task error: walreceiver connection handling failure.*",
    ".*wal_connection_manager.*tcp connect error: Connection refused.*",
    ".*query handler for .* failed: Socket IO error: Connection reset by peer.*",
    ".*serving compute connection task.*exited with error: Postgres connection error.*",
    ".*serving compute connection task.*exited with error: Connection reset by peer.*",
    ".*serving compute connection task.*exited with error: Postgres query error.*",
    ".*Connection aborted: error communicating with the server: Transport endpoint is not connected.*",
    # FIXME: replication patch for tokio_postgres regards  any but CopyDone/CopyData message in CopyBoth stream as unexpected
    ".*Connection aborted: unexpected message from server*",
    ".*kill_and_wait_impl.*: wait successful.*",
    ".*query handler for 'pagestream.*failed: Broken pipe.*",  # pageserver notices compute shut down
    ".*query handler for 'pagestream.*failed: Connection reset by peer.*",  # pageserver notices compute shut down
    # safekeeper connection can fail with this, in the window between timeline creation
    # and streaming start
    ".*Failed to process query for timeline .*: state uninitialized, no data to read.*",
    # Tests related to authentication and authorization print these
    ".*Error processing HTTP request: Forbidden",
    # intentional failpoints
    ".*failpoint ",
    # Tenant::delete_timeline() can cause any of the four following errors.
    # FIXME: we shouldn't be considering it an error: https://github.com/neondatabase/neon/issues/2946
    ".*could not flush frozen layer.*queue is in state Stopped",  # when schedule layer upload fails because queued got closed before compaction got killed
    ".*wait for layer upload ops to complete.*",  # .*Caused by:.*wait_completion aborted because upload queue was stopped
    ".*gc_loop.*Gc failed, retrying in.*timeline is Stopping",  # When gc checks timeline state after acquiring layer_removal_cs
    ".*gc_loop.*Gc failed, retrying in.*: Cannot run GC iteration on inactive tenant",  # Tenant::gc precondition
    ".*compaction_loop.*Compaction failed.*, retrying in.*timeline or pageserver is shutting down",  # When compaction checks timeline state after acquiring layer_removal_cs
    ".*query handler for 'pagestream.*failed: Timeline .* was not found",  # postgres reconnects while timeline_delete doesn't hold the tenant's timelines.lock()
    ".*query handler for 'pagestream.*failed: Timeline .* is not active",  # timeline delete in progress
    ".*task iteration took longer than the configured period.*",
    # these can happen anytime we do compactions from background task and shutdown pageserver
    ".*could not compact.*cancelled.*",
    # this is expected given our collaborative shutdown approach for the UploadQueue
    ".*Compaction failed.*, retrying in .*: Other\\(queue is in state Stopped.*",
    ".*Compaction failed.*, retrying in .*: ShuttingDown",
    ".*Compaction failed.*, retrying in .*: Other\\(timeline shutting down.*",
    # Pageserver timeline deletion should be polled until it gets 404, so ignore it globally
    ".*Error processing HTTP request: NotFound: Timeline .* was not found",
    ".*took more than expected to complete.*",
    # these can happen during shutdown, but it should not be a reason to fail a test
    ".*completed, took longer than expected.*",
    # AWS S3 may emit 500 errors for keys in a DeleteObjects response: we retry these
    # and it is not a failure of our code when it happens.
    ".*DeleteObjects.*We encountered an internal error. Please try again.*",
    # During shutdown, DownloadError::Cancelled may be logged as an error.  Cleaning this
    # up is tracked in https://github.com/neondatabase/neon/issues/6096
    ".*Cancelled, shutting down.*",
    # Open layers are only rolled at Lsn boundaries to avoid name clashses.
    # Hence, we can overshoot the soft limit set by checkpoint distance.
    # This is especially pronounced in tests that set small checkpoint
    # distances.
    ".*Flushed oversized open layer with size.*",
    # During teardown, we stop the storage controller before the pageservers, so pageservers
    # can experience connection errors doing background deletion queue work.
    ".*WARN deletion backend: calling control plane generation validation API failed.*error sending request.*",
    # Can happen when the test shuts down the storage controller while it is calling the utilization API
    ".*WARN.*path=/v1/utilization .*request was dropped before completing",
    # Can happen during shutdown
    ".*scheduling deletion on drop failed: queue is in state Stopped.*",
    # L0 flush backpressure delays are expected under heavy ingest load. We want to exercise
    # this backpressure in tests.
    ".*delaying layer flush by \\S+ for compaction backpressure.*",
    ".*stalling layer flushes for compaction backpressure.*",
    ".*layer roll waiting for flush due to compaction backpressure.*",
)


DEFAULT_STORAGE_CONTROLLER_ALLOWED_ERRORS = [
    # Many tests will take pageservers offline, resulting in log warnings on the controller
    # failing to connect to them.
    ".*Call to node.*management API.*failed.*receive body.*",
    ".*Call to node.*management API.*failed.*ReceiveBody.*",
    ".*Failed to update node .+ after heartbeat round.*error sending request for url.*",
    # Many tests will start up with a node offline
    ".*startup_reconcile: Could not scan node.*",
    # Tests run in dev mode
    ".*Starting in dev mode.*",
    # Tests that stop endpoints & use the storage controller's neon_local notification
    # mechanism might fail (neon_local's stopping and endpoint isn't atomic wrt the storage
    # controller's attempts to notify the endpoint).
    ".*reconciler.*neon_local notification hook failed.*",
    ".*reconciler.*neon_local error.*",
]


def _check_allowed_errors(input):
    allowed_errors: list[str] = list(DEFAULT_PAGESERVER_ALLOWED_ERRORS)

    # add any test specifics here; cli parsing is not provided for the
    # difficulty of copypasting regexes as arguments without any quoting
    # errors.

    errors = scan_pageserver_log_for_errors(input, allowed_errors)

    for lineno, error in errors:
        print(f"-:{lineno}: {error.strip()}", file=sys.stderr)

    print(f"\n{len(errors)} not allowed errors", file=sys.stderr)

    return errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="check input against pageserver global allowed_errors"
    )
    parser.add_argument(
        "-i",
        "--input",
        type=argparse.FileType("r"),
        help="Pageserver logs file. Use '-' for stdin.",
        required=True,
    )

    args = parser.parse_args()
    errors = _check_allowed_errors(args.input)

    sys.exit(len(errors) > 0)
