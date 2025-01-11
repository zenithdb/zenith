## WAL Decoding and Interpretation Benchmarks

Note that these benchmarks pull WAL from a public bucket in S3
as a preparation step. Hence, you need a way to auth with AWS.
You can achieve this by copying the `~/.aws/config` file from
the AWS SSO notion page and exporting `AWS_PROFILE=dev` when invoking
the benchmarks.

To run benchmarks:

```sh
aws sso login --profile dev

# All benchmarks.
AWS_PROFILE=dev cargo bench --package wal_decoder

# Specific file.
AWS_PROFILE=dev cargo bench --package wal_decoder --bench bench_interpret_wal

# Specific benchmark.
AWS_PROFILE=dev cargo bench --package wal_decoder --bench bench_interpret_wal unsharded

# List available benchmarks.
cargo bench --package wal_decoder --benches -- --list

# Generate flamegraph profiles using pprof-rs, profiling for 10 seconds.
# Output in target/criterion/*/profile/flamegraph.svg.
AWS_PROFILE=dev cargo bench --package wal_decoder --bench bench_interpret_wal unsharded -- --profile-time 10
```

Additional charts and statistics are available in `target/criterion/report/index.html`.

Benchmarks are automatically compared against the previous run. To compare against other runs, see
`--baseline` and `--save-baseline`.
