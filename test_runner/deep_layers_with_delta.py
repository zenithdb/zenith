"""
Creates stack of L0 deltas each of which should have 1 Value::Delta per page in `data`,
in your running neon_local setup.

This showcases the need for concurrent IO.

I'm not completely sure about it because I don't yet understand the relation between
fillfactor and mvcc (does mvcc use the unfilled space in the page?).

I think there's a chance that some of the deltas are will_init .
Could be avoided with disabling fullpage writes.

TODO: concurrent IO will read beyond will_init and discard the data (I think).
Measure impact of that.

Some commands from shell history used in combination with this script:

```
 export NEON_REPO_DIR=$PWD/.neon
 export NEON_BIN_DIR=$PWD/target/release
 $NEON_BIN_DIR/neon_local stop
 rm -rf $NEON_REPO_DIR
 $NEON_BIN_DIR/neon_local init
 cat >>  $NEON_REPO_DIR/pageserver_1/pageserver.toml <<"EOF"
 # customizations
 virtual_file_io_mode = "direct"
 page_service_pipelining={mode="pipelined", max_batch_size=32, execution="concurrent-futures"}
 get_vectored_concurrent_io={mode="sidecar-task"}
EOF
 $NEON_BIN_DIR/neon_local start

 psql 'postgresql://localhost:1235/storage_controller' -c 'DELETE FROM tenant_shards'
 sed 's/.*get_vectored_concurrent_io.*/get_vectored_concurrent_io={mode="sidecar-task"}/' -i $NEON_REPO_DIR/pageserver_1/pageserver.toml
 $NEON_BIN_DIR/neon_local pageserver restart
 sleep 2
 $NEON_BIN_DIR/neon_local tenant create --set-default
 ./target/debug/neon_local endpoint stop foo
 rm -rf  $NEON_REPO_DIR/endpoints/foo
 ./target/debug/neon_local endpoint create foo
 echo 'full_page_writes=off' >>  $NEON_REPO_DIR/endpoints/foo/postgresql.conf
 ./target/debug/neon_local endpoint start foo
 pushd test_runner; poetry run python3 deep_layers_with_delta.py 20; popd
```

Layer files created:

christian@neon-hetzner-dev-christian:[~/src/neon]: ls -hlrt .neon/pageserver_1/tenants/*/timelines/*
total 67M
-rw-r--r-- 1 christian  23M Dec 13 12:38 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014F1378-00000000014F13F1-v1-00000001
-rw-r--r-- 1 christian 9.3M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014F13F1-0000000001C4D021-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001C4D021-0000000001DC51F1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001DC51F1-0000000001F3D3C1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001F3D3C1-00000000020B55A1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000020B55A1-000000000222D771-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:41 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000222D771-00000000023A5941-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000023A5941-000000000251DB11-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000251DB11-0000000002695D19-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002695D19-000000000280DEE9-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000280DEE9-00000000029860D1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000029860D1-0000000002AFE2A1-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002AFE2A1-0000000002C76471-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002C76471-0000000002DEE679-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002DEE679-0000000002F66849-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000002F66849-00000000030DEA29-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000030DEA29-0000000003256BF9-v1-00000001
-rw-r--r-- 1 christian 2.0M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000003256BF9-00000000033CEE39-v1-00000001
-rw-r--r-- 1 christian 1.7M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000033CEE39-0000000003512B91-v1-00000001
-rw-r--r-- 1 christian 1.4M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000003512B91-0000000003633D91-v1-00000001
-rw-r--r-- 1 christian 1.9M Dec 13 12:42 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000003633D91-00000000037AE2E9-v1-00000001
-rw-r--r-- 1 christian    0 Dec 13 12:42 ephemeral-22

Now use pagebench to measure perf, e.g.

./target/release/pagebench get-page-latest-lsn --num-clients=1  --queue-depth 100 --runtime=5s


"""

import sys

import psycopg2
from fixtures.common_types import TenantShardId, TimelineId
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.makelayers.l0stack import L0StackShape, make_l0_stack_standalone

ps_http = PageserverHttpClient(port=9898, is_testing_enabled_or_skip=lambda: None)
vps_http = PageserverHttpClient(port=1234, is_testing_enabled_or_skip=lambda: None)

tenants = ps_http.tenant_list()
assert len(tenants) == 1
tenant_shard_id = TenantShardId.parse(tenants[0]["id"])

timlines = ps_http.timeline_list(tenant_shard_id)
assert len(timlines) == 1
timeline_id = TimelineId(timlines[0]["timeline_id"])

connstr = "postgresql://cloud_admin@localhost:55432/postgres"
conn = psycopg2.connect(connstr)

shape = L0StackShape(logical_table_size_mib=50, delta_stack_height=int(sys.argv[1]))

make_l0_stack_standalone(vps_http, ps_http, tenant_shard_id, timeline_id, conn, shape)
