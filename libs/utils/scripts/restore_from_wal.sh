#!/usr/bin/env bash

set -euxo pipefail

PG_BIN=$1
WAL_PATH=$2
DATA_DIR=$3
PORT=$4
PG_VERSION=$5
SYSID=$(od -A n -j 24 -N 8 -t d8 "$WAL_PATH"/000000010000000000000002* | cut -c 3-)

# The way that initdb is invoked must match how the pageserver runs initdb.
function initdb_with_args {
    local cmd=(
        "$PG_BIN"/initdb
        -E utf8
        -U cloud_admin
        -D "$DATA_DIR"
        --locale 'C.UTF-8'
        --lc-collate 'C.UTF-8'
        --lc-ctype 'C.UTF-8'
        --lc-messages 'C.UTF-8'
        --lc-monetary 'C.UTF-8'
        --lc-numeric 'C.UTF-8'
        --lc-time 'C.UTF-8'
        --sysid="$SYSID"
    )

    case "$PG_VERSION" in
        14)
            # Postgres 14 and below didn't support --locale-provider
            ;;
        15 | 16)
            cmd+=(--locale-provider 'libc')
            ;;
        *)
            # Postgres 17 added the builtin provider
            cmd+=(--locale-provider 'builtin')
            ;;
    esac

    eval env -i LD_LIBRARY_PATH="$PG_BIN"/../lib ASAN_OPTIONS="${ASAN_OPTIONS-}" UBSAN_OPTIONS="${UBSAN_OPTIONS-}" "${cmd[*]}"
}

rm -fr "$DATA_DIR"
initdb_with_args
echo "port=$PORT" >> "$DATA_DIR"/postgresql.conf
echo "shared_preload_libraries='\$libdir/neon_rmgr.so'" >> "$DATA_DIR"/postgresql.conf
REDO_POS=0x$("$PG_BIN"/pg_controldata -D "$DATA_DIR" | grep -F "REDO location"| cut -c 42-)
declare -i WAL_SIZE=$REDO_POS+114
"$PG_BIN"/pg_ctl -D "$DATA_DIR" -l "$DATA_DIR/logfile.log" start
"$PG_BIN"/pg_ctl -D "$DATA_DIR" -l "$DATA_DIR/logfile.log" stop -m immediate
cp "$DATA_DIR"/pg_wal/000000010000000000000001 "$DATA_DIR"
cp "$WAL_PATH"/* "$DATA_DIR"/pg_wal/
for partial in "$DATA_DIR"/pg_wal/*.partial ; do mv "$partial" "${partial%.partial}" ; done
dd if="$DATA_DIR"/000000010000000000000001 of="$DATA_DIR"/pg_wal/000000010000000000000001 bs=$WAL_SIZE count=1 conv=notrunc
rm -f "$DATA_DIR"/000000010000000000000001
