#!/bin/bash
set -ex
cd "$(dirname "${0}")"
psql -c "alter system set autvacuum = 'off'"
psql -c "select pg_reload_conf()"
make installcheck
psql -c "alter system set autvacuum = 'on'"
psql -c "select pg_reload_conf()"
