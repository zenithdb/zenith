### Overview
Pageserver and proxy periodically collect consumption metrics and push them to a HTTP endpoint.

This doc describes current implementation details.
For design details see [the RFC](./rfcs/021-metering.md) and [the discussion on Github](https://github.com/neondatabase/neon/pull/2884).

- The metrics are collected in a separate thread, and the collection interval and endpoint are configurable.

- Metrics are cached, so that we don't send unchanged metrics on every iteration.

- Metrics are sent in batches of 1000 (see CHUNK_SIZE const) metrics max with no particular grouping guarantees.

batch format is
```json

{ "events" : [metric1, metric2, ...] }

```
See metric format examples below.

- All metrics values are in bytes, unless otherwise specified.

- Currently no retries are implemented.

### Pageserver metrics

#### Configuration
The endpoint and the collection interval are specified in the pageserver config file (or can be passed as command line arguments):
`metric_collection_endpoint` defaults to None, which means that metric collection is disabled by default.
`metric_collection_interval` defaults to 10min

#### Metrics

Currently, the following metrics are collected:

- `written_size`

Amount of WAL produced , by a timeline, i.e. last_record_lsn
This is an absolute, per-timeline metric.

- `resident_size`

Size of all the layer files in the tenant's directory on disk on the pageserver.
This is an absolute, per-tenant metric.

- `remote_storage_size`

Size of the remote storage (S3) directory.
This is an absolute, per-tenant metric.

- `timeline_logical_size`

Logical size of the data in the timeline.
This is an absolute, per-timeline metric.

- `synthetic_storage_size`

Size of all tenant's branches including WAL.
This is the same metric that `tenant/{tenant_id}/size` endpoint returns.
This is an absolute, per-tenant metric.

Synthetic storage size is calculated in a separate thread, so it might be slightly outdated.

#### Format example

```json
{
"metric": "remote_storage_size",
"type": "absolute",
"time": "2022-12-28T11:07:19.317310284Z",
"idempotency_key": "2022-12-28 11:07:19.317310324 UTC-1-4019",
"value": 12345454,
"tenant_id": "5d07d9ce9237c4cd845ea7918c0afa7d",
"timeline_id": "a03ebb4f5922a1c56ff7485cc8854143",
}
```

`idempotency_key` is a unique key for each metric, so that we can deduplicate metrics.
It is a combination of the time, node_id and a random number.

### Proxy consumption metrics

#### Configuration
The endpoint and the collection interval can be passed as command line arguments for proxy:
`metric_collection_endpoint` no default, which means that metric collection is disabled by default.
`metric_collection_interval` no default

#### Metrics

Currently, only one proxy metric is collected:

- `proxy_io_bytes_per_client`
Outbound traffic per client.
This is an incremental, per-endpoint metric.

#### Format example

```json
{
"metric": "proxy_io_bytes_per_client",
"type": "incremental",
"start_time": "2022-12-28T11:07:19.317310284Z",
"stop_time": "2022-12-28T11:07:19.317310284Z",
"idempotency_key": "2022-12-28 11:07:19.317310324 UTC-1-4019",
"value": 12345454,
"endpoint_id": "5d07d9ce9237c4cd845ea7918c0afa7d",
}
```

The metric is incremental, so the value is the difference between the current and the previous value.
If there is no previous value, the value is the current value and the `start_time` equals `stop_time`.

### TODO

- [ ] Handle errors better: currently if one tenant fails to gather metrics, the whole iteration fails and metrics are not sent for any tenant.
- [ ] Add retries
- [ ] Tune the interval
