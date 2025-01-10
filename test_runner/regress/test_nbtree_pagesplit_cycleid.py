import threading
import time

from fixtures.neon_fixtures import NeonEnv

BTREE_NUM_CYCLEID_PAGES = """
    WITH raw_pages AS (
        SELECT blkno, get_raw_page_at_lsn('t_uidx', 'main', blkno, NULL, NULL) page
        FROM generate_series(1, pg_relation_size('t_uidx'::regclass) / 8192) blkno
    ),
    parsed_pages AS (
        /* cycle ID is the last 2 bytes of the btree page */
        SELECT blkno, SUBSTRING(page FROM 8191 FOR 2) as cycle_id
        FROM raw_pages
    )
    SELECT count(*),
           encode(cycle_id, 'hex')
     FROM parsed_pages
    WHERE encode(cycle_id, 'hex') != '0000'
    GROUP BY encode(cycle_id, 'hex');
    """


def test_nbtree_pagesplit_cycleid(neon_simple_env: NeonEnv):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    ses1 = endpoint.connect().cursor()
    ses1.execute("ALTER SYSTEM SET autovacuum = off;")
    ses1.execute("ALTER SYSTEM SET enable_seqscan = off;")
    ses1.execute("ALTER SYSTEM SET full_page_writes = off;")
    ses1.execute("SELECT pg_reload_conf();")
    ses1.execute("CREATE EXTENSION neon_test_utils;")
    # prepare a large index
    ses1.execute("CREATE TABLE t(id integer GENERATED ALWAYS AS IDENTITY, txt text);")
    ses1.execute("CREATE UNIQUE INDEX t_uidx ON t(id);")
    ses1.execute("INSERT INTO t (txt) SELECT i::text FROM generate_series(1, 2035) i;")

    ses1.execute("SELECT neon_xlogflush();")
    ses1.execute(BTREE_NUM_CYCLEID_PAGES)
    pages = ses1.fetchall()
    assert (
        len(pages) == 0
    ), f"0 back splits with cycle ID expected, real {len(pages)} first {pages[0]}"
    # Delete enough tuples to clear the first index page.
    # (there are up to 407 rows per 8KiB page; 406 for non-rightmost leafs.
    ses1.execute("DELETE FROM t WHERE id <= 406;")
    # Make sure the page is cleaned up
    ses1.execute("VACUUM (FREEZE, INDEX_CLEANUP ON) t;")

    # Do another delete-then-indexcleanup cycle, to move the pages from
    # "dead" to "reusable"
    ses1.execute("DELETE FROM t WHERE id <= 446;")
    ses1.execute("VACUUM (FREEZE, INDEX_CLEANUP ON) t;")

    # Make sure the vacuum we're about to trigger in s3 has cleanup work to do
    ses1.execute("DELETE FROM t WHERE id <= 610;")

    # Flush wal, for checking purposes
    ses1.execute("SELECT neon_xlogflush();")
    ses1.execute(BTREE_NUM_CYCLEID_PAGES)
    pages = ses1.fetchall()
    assert len(pages) == 0, f"No back splits with cycle ID expected, got batches of {pages} instead"

    ses2 = endpoint.connect().cursor()
    ses3 = endpoint.connect().cursor()

    # Session 2 pins a btree page, which prevents vacuum from processing that
    # page, thus allowing us to reliably split pages while a concurrent vacuum
    # is running.
    ses2.execute("BEGIN;")
    ses2.execute(
        "DECLARE foo NO SCROLL CURSOR FOR SELECT row_number() over () FROM t ORDER BY id ASC"
    )
    ses2.execute("FETCH FROM foo;")  # pins the leaf page with id 611
    wait_evt = threading.Event()

    # Session 3 runs the VACUUM command. Note that this will block, and
    # therefore must run on another thread.
    # We rely on this running quickly enough to hit the pinned page from
    # session 2 by the time we start other work again in session 1, but
    # technically there is a race where the thread (and/or PostgreSQL process)
    # don't get to that pinned page with vacuum until >2s after evt.set() was
    # called, and session 1 thus might already have split pages.
    def vacuum_freeze_t(ses3, evt: threading.Event):
        # Begin parallel vacuum that should hit the index
        evt.set()
        # this'll hang until s2 fetches enough new data from its cursor.
        # this is technically a race with the time.sleep(2) below, but if this
        # command doesn't hit
        ses3.execute("VACUUM (FREEZE, INDEX_CLEANUP on, DISABLE_PAGE_SKIPPING on) t;")

    ses3t = threading.Thread(target=vacuum_freeze_t, args=(ses3, wait_evt))
    ses3t.start()
    wait_evt.wait()
    # Make extra sure we got the thread started and vacuum is stuck, by waiting
    # some time even after wait_evt got set. This isn't truly reliable (it is
    # possible
    time.sleep(2)

    # Insert 2 pages worth of new data.
    # This should reuse the one empty page, plus another page at the end of
    # the index relation; with split ordering
    #    old_blk -> blkno=1 -> old_blk + 1.
    # As this is run while vacuum in session 3 is happening, these splits
    # should receive cycle IDs where applicable.
    ses1.execute("INSERT INTO t (txt) SELECT i::text FROM generate_series(1, 812) i;")
    # unpin the btree page, allowing s3's vacuum to complete
    ses2.execute("FETCH ALL FROM foo;")
    ses2.execute("ROLLBACK;")
    # flush WAL to make sure PS is up-to-date
    ses1.execute("SELECT neon_xlogflush();")
    # check that our expectations are correct
    ses1.execute(BTREE_NUM_CYCLEID_PAGES)
    pages = ses1.fetchall()
    assert (
        len(pages) == 1 and pages[0][0] == 3
    ), f"3 page splits with cycle ID expected; actual {pages}"

    # final cleanup
    ses3t.join()
    ses1.close()
    ses2.close()
    ses3.close()
