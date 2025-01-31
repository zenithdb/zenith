#!/bin/bash
set -ex
cd "$(dirname "${0}")"
pg_prove test.sql