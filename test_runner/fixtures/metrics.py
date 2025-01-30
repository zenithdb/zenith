from __future__ import annotations

from collections import defaultdict

from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample

from fixtures.log_helper import log


class Metrics:
    metrics: dict[str, list[Sample]]
    name: str

    def __init__(self, name: str = ""):
        self.metrics = defaultdict(list)
        self.name = name

    def query_all(self, name: str, filter: dict[str, str] | None = None) -> list[Sample]:
        filter = filter or {}
        res: list[Sample] = []

        for sample in self.metrics[name]:
            try:
                if all(sample.labels[k] == v for k, v in filter.items()):
                    res.append(sample)
            except KeyError:
                pass
        return res

    def query_one(self, name: str, filter: dict[str, str] | None = None) -> Sample:
        res = self.query_all(name, filter or {})
        assert len(res) == 1, f"expected single sample for {name} {filter}, found {res}"
        return res[0]


class MetricsGetter:
    """
    Mixin for types that implement a `get_metrics` function and would like associated
    helpers for querying the metrics
    """

    def get_metrics(self) -> Metrics:
        raise NotImplementedError()

    def get_metric_value(self, name: str, filter: dict[str, str] | None = None) -> float | None:
        metrics = self.get_metrics()
        results = metrics.query_all(name, filter=filter)
        if not results:
            log.info(f'could not find metric "{name}"')
            return None
        assert len(results) == 1, f"metric {name} with given filters is not unique, got: {results}"
        return results[0].value

    def get_metrics_values(
        self, names: list[str], filter: dict[str, str] | None = None, absence_ok: bool = False
    ) -> dict[str, float]:
        """
        When fetching multiple named metrics, it is more efficient to use this
        than to call `get_metric_value` repeatedly.

        Throws RuntimeError if no metrics matching `names` are found, or if
        not all of `names` are found: this method is intended for loading sets
        of metrics whose existence is coupled.

        If it's expected that there may be no results for some of the metrics,
        specify `absence_ok=True`. The returned dict will then not contain values
        for these metrics.
        """
        metrics = self.get_metrics()
        samples = []
        for name in names:
            samples.extend(metrics.query_all(name, filter=filter))

        result = {}
        for sample in samples:
            if sample.name in result:
                raise RuntimeError(f"Multiple values found for {sample.name}")
            result[sample.name] = sample.value

        if not absence_ok:
            if len(result) != len(names):
                log.info(f"Metrics found: {metrics.metrics}")
                raise RuntimeError(f"could not find all metrics {' '.join(names)}")

        return result

    def get_metric_sum(
        self, names: list[str], filter: dict[str, str] | None = None, absence_ok: bool = False
    ) -> float:
        """
        Fetch all metrics matching `names` and `filter`, and sum their values
        """
        metrics = self.get_metrics()
        samples = []
        for name in names:
            samples.extend(metrics.query_all(name, filter=filter))

        found = False
        result = 0.0
        for sample in samples:
            result += sample.value
            found = True

        if not found and not absence_ok:
            log.info(f"Metrics found: {metrics.metrics}")
            raise RuntimeError(f"could not find any metrics matching {names}, {filter}")

        return result


def parse_metrics(text: str, name: str = "") -> Metrics:
    metrics = Metrics(name)
    gen = text_string_to_metric_families(text)
    for family in gen:
        for sample in family.samples:
            metrics.metrics[sample.name].append(sample)

    return metrics


def histogram(prefix_without_trailing_underscore: str) -> list[str]:
    assert not prefix_without_trailing_underscore.endswith("_")
    return [f"{prefix_without_trailing_underscore}_{x}" for x in ["bucket", "count", "sum"]]


def counter(name: str) -> str:
    # the prometheus_client package appends _total to all counters client-side
    return f"{name}_total"


PAGESERVER_PER_TENANT_REMOTE_TIMELINE_CLIENT_METRICS: tuple[str, ...] = (
    "pageserver_remote_timeline_client_calls_started_total",
    "pageserver_remote_timeline_client_calls_finished_total",
    "pageserver_remote_physical_size",
    "pageserver_remote_timeline_client_bytes_started_total",
    "pageserver_remote_timeline_client_bytes_finished_total",
)

PAGESERVER_GLOBAL_METRICS: tuple[str, ...] = (
    "pageserver_storage_operations_seconds_global_count",
    "pageserver_storage_operations_seconds_global_sum",
    "pageserver_storage_operations_seconds_global_bucket",
    "pageserver_unexpected_ondemand_downloads_count_total",
    "libmetrics_launch_timestamp",
    "libmetrics_build_info",
    "libmetrics_tracing_event_count_total",
    "pageserver_page_cache_read_hits_total",
    "pageserver_page_cache_read_accesses_total",
    "pageserver_page_cache_size_current_bytes",
    "pageserver_page_cache_size_max_bytes",
    *[f"pageserver_basebackup_query_seconds_{x}" for x in ["bucket", "count", "sum"]],
    *histogram("pageserver_smgr_query_seconds_global"),
    *histogram("pageserver_wait_lsn_seconds"),
    *histogram("pageserver_remote_operation_seconds"),
    *histogram("pageserver_io_operations_seconds"),
    "pageserver_smgr_query_started_global_count_total",
    "pageserver_tenant_states_count",
    "pageserver_circuit_breaker_broken_total",
    "pageserver_circuit_breaker_unbroken_total",
    counter("pageserver_tenant_throttling_count_accounted_start_global"),
    counter("pageserver_tenant_throttling_count_accounted_finish_global"),
    counter("pageserver_tenant_throttling_wait_usecs_sum_global"),
    counter("pageserver_tenant_throttling_count_global"),
    *histogram("pageserver_tokio_epoll_uring_slots_submission_queue_depth"),
)

PAGESERVER_PER_TENANT_METRICS: tuple[str, ...] = (
    "pageserver_current_logical_size",
    "pageserver_resident_physical_size",
    "pageserver_io_operations_bytes_total",
    "pageserver_last_record_lsn",
    "pageserver_disk_consistent_lsn",
    "pageserver_projected_remote_consistent_lsn",
    "pageserver_standby_horizon",
    "pageserver_smgr_query_seconds_bucket",
    "pageserver_smgr_query_seconds_count",
    "pageserver_smgr_query_seconds_sum",
    "pageserver_smgr_query_started_count_total",
    "pageserver_archive_size",
    "pageserver_pitr_history_size",
    "pageserver_layer_bytes",
    "pageserver_layer_count",
    "pageserver_layers_per_read_bucket",
    "pageserver_layers_per_read_count",
    "pageserver_layers_per_read_sum",
    "pageserver_visible_physical_size",
    "pageserver_storage_operations_seconds_count_total",
    "pageserver_storage_operations_seconds_sum_total",
    "pageserver_evictions_total",
    "pageserver_evictions_with_low_residence_duration_total",
    "pageserver_aux_file_estimated_size",
    "pageserver_valid_lsn_lease_count",
    "pageserver_flush_wait_upload_seconds",
    counter("pageserver_tenant_throttling_count_accounted_start"),
    counter("pageserver_tenant_throttling_count_accounted_finish"),
    counter("pageserver_tenant_throttling_wait_usecs_sum"),
    counter("pageserver_tenant_throttling_count"),
    counter("pageserver_timeline_wal_records_received"),
    counter("pageserver_page_service_pagestream_flush_in_progress_micros"),
    *histogram("pageserver_page_service_batch_size"),
    *histogram("pageserver_page_service_pagestream_batch_wait_time_seconds"),
    *PAGESERVER_PER_TENANT_REMOTE_TIMELINE_CLIENT_METRICS,
    # "pageserver_directory_entries_count", -- only used if above a certain threshold
    # "pageserver_broken_tenants_count" -- used only for broken
)
