import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from mypy_boto3_s3.type_defs import ListObjectsV2OutputTypeDef

from fixtures.log_helper import log
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.remote_storage import RemoteStorageKind, S3Storage
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import wait_until


def assert_tenant_state(
    pageserver_http: PageserverHttpClient,
    tenant: TenantId,
    expected_state: str,
    message: Optional[str] = None,
):
    tenant_status = pageserver_http.tenant_status(tenant)
    log.info(f"tenant_status: {tenant_status}")
    assert tenant_status["state"]["slug"] == expected_state, message or tenant_status


def remote_consistent_lsn(
    pageserver_http: PageserverHttpClient, tenant: TenantId, timeline: TimelineId
) -> Lsn:
    detail = pageserver_http.timeline_detail(tenant, timeline)

    if detail["remote_consistent_lsn"] is None:
        # No remote information at all. This happens right after creating
        # a timeline, before any part of it has been uploaded to remote
        # storage yet.
        return Lsn(0)
    else:
        lsn_str = detail["remote_consistent_lsn"]
        assert isinstance(lsn_str, str)
        return Lsn(lsn_str)


def wait_for_upload(
    pageserver_http: PageserverHttpClient,
    tenant: TenantId,
    timeline: TimelineId,
    lsn: Lsn,
):
    """waits for local timeline upload up to specified lsn"""
    for i in range(20):
        current_lsn = remote_consistent_lsn(pageserver_http, tenant, timeline)
        if current_lsn >= lsn:
            log.info("wait finished")
            return
        lr_lsn = last_record_lsn(pageserver_http, tenant, timeline)
        log.info(
            f"waiting for remote_consistent_lsn to reach {lsn}, now {current_lsn}, last_record_lsn={lr_lsn}, iteration {i + 1}"
        )
        time.sleep(1)
    raise Exception(
        "timed out while waiting for remote_consistent_lsn to reach {}, was {}".format(
            lsn, current_lsn
        )
    )


def wait_until_tenant_state(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    expected_state: str,
    iterations: int,
    period: float = 1.0,
) -> Dict[str, Any]:
    """
    Does not use `wait_until` for debugging purposes
    """
    for _ in range(iterations):
        try:
            tenant = pageserver_http.tenant_status(tenant_id=tenant_id)
            log.debug(f"Tenant {tenant_id} data: {tenant}")
            if tenant["state"]["slug"] == expected_state:
                return tenant
        except Exception as e:
            log.debug(f"Tenant {tenant_id} state retrieval failure: {e}")

        time.sleep(period)

    raise Exception(
        f"Tenant {tenant_id} did not become {expected_state} within {iterations * period} seconds"
    )


def wait_until_timeline_state(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    expected_state: str,
    iterations: int,
    period: float = 1.0,
) -> Dict[str, Any]:
    """
    Does not use `wait_until` for debugging purposes
    """
    for i in range(iterations):
        try:
            timeline = pageserver_http.timeline_detail(tenant_id=tenant_id, timeline_id=timeline_id)
            log.debug(f"Timeline {tenant_id}/{timeline_id} data: {timeline}")
            if isinstance(timeline["state"], str):
                if timeline["state"] == expected_state:
                    return timeline
            elif isinstance(timeline, Dict):
                if timeline["state"].get(expected_state):
                    return timeline

        except Exception as e:
            log.debug(f"Timeline {tenant_id}/{timeline_id} state retrieval failure: {e}")

        if i == iterations - 1:
            # do not sleep last time, we already know that we failed
            break
        time.sleep(period)

    raise Exception(
        f"Timeline {tenant_id}/{timeline_id} did not become {expected_state} within {iterations * period} seconds"
    )


def wait_until_tenant_active(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    iterations: int = 30,
    period: float = 1.0,
):
    wait_until_tenant_state(
        pageserver_http,
        tenant_id,
        expected_state="Active",
        iterations=iterations,
        period=period,
    )


def last_record_lsn(
    pageserver_http_client: PageserverHttpClient, tenant: TenantId, timeline: TimelineId
) -> Lsn:
    detail = pageserver_http_client.timeline_detail(tenant, timeline)

    lsn_str = detail["last_record_lsn"]
    assert isinstance(lsn_str, str)
    return Lsn(lsn_str)


def wait_for_last_record_lsn(
    pageserver_http: PageserverHttpClient,
    tenant: TenantId,
    timeline: TimelineId,
    lsn: Lsn,
) -> Lsn:
    """waits for pageserver to catch up to a certain lsn, returns the last observed lsn."""
    for i in range(100):
        current_lsn = last_record_lsn(pageserver_http, tenant, timeline)
        if current_lsn >= lsn:
            return current_lsn
        if i % 10 == 0:
            log.info(
                "waiting for last_record_lsn to reach {}, now {}, iteration {}".format(
                    lsn, current_lsn, i + 1
                )
            )
        time.sleep(0.1)
    raise Exception(
        "timed out while waiting for last_record_lsn to reach {}, was {}".format(lsn, current_lsn)
    )


def wait_for_upload_queue_empty(
    pageserver_http: PageserverHttpClient, tenant_id: TenantId, timeline_id: TimelineId
):
    while True:
        all_metrics = pageserver_http.get_metrics()
        tl = all_metrics.query_all(
            "pageserver_remote_timeline_client_calls_unfinished",
            {
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
            },
        )
        assert len(tl) > 0
        log.info(f"upload queue for {tenant_id}/{timeline_id}: {tl}")
        if all(m.value == 0 for m in tl):
            return
        time.sleep(0.2)


def wait_timeline_detail_404(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    iterations: int,
    interval: Optional[float] = None,
):
    if interval is None:
        interval = 0.25

    def timeline_is_missing():
        data = {}
        try:
            data = pageserver_http.timeline_detail(tenant_id, timeline_id)
            log.info(f"timeline detail {data}")
        except PageserverApiException as e:
            log.debug(e)
            if e.status_code == 404:
                return

        raise RuntimeError(f"Timeline exists state {data.get('state')}")

    wait_until(iterations, interval, func=timeline_is_missing)


def timeline_delete_wait_completed(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    iterations: int = 20,
    interval: Optional[float] = None,
    **delete_args,
):
    pageserver_http.timeline_delete(tenant_id=tenant_id, timeline_id=timeline_id, **delete_args)
    wait_timeline_detail_404(pageserver_http, tenant_id, timeline_id, iterations, interval)


if TYPE_CHECKING:
    # TODO avoid by combining remote storage related stuff in single type
    # and just passing in this type instead of whole builder
    from fixtures.neon_fixtures import NeonEnvBuilder


def assert_prefix_empty(neon_env_builder: "NeonEnvBuilder", prefix: Optional[str] = None):
    response = list_prefix(neon_env_builder, prefix)
    keys = response["KeyCount"]
    objects = response.get("Contents", [])
    common_prefixes = response.get("CommonPrefixes", [])

    remote_storage = neon_env_builder.pageserver_remote_storage
    is_mock_s3 = isinstance(remote_storage, S3Storage) and not remote_storage.cleanup

    if is_mock_s3:
        if keys == 1 and len(objects) == 0 and len(common_prefixes) == 1:
            # this has been seen in the wild by tests with the below contradicting logging
            # https://neon-github-public-dev.s3.amazonaws.com/reports/pr-5322/6207777020/index.html#suites/3556ed71f2d69272a7014df6dcb02317/53b5c368b5a68865
            # this seems like a mock_s3 issue
            log.warn(
                f"contrading ListObjectsV2 response with KeyCount={keys} and Contents={objects}, CommonPrefixes={common_prefixes}, assuming this means KeyCount=0"
            )
            keys = 0
        elif keys != 0 and len(objects) == 0:
            # this has been seen in one case with mock_s3:
            # https://neon-github-public-dev.s3.amazonaws.com/reports/pr-4938/6000769714/index.html#suites/3556ed71f2d69272a7014df6dcb02317/ca01e4f4d8d9a11f
            # looking at moto impl, it might be there's a race with common prefix (sub directory) not going away with deletes
            log.warn(
                f"contradicting ListObjectsV2 response with KeyCount={keys} and Contents={objects}, CommonPrefixes={common_prefixes}"
            )

    assert keys == 0, f"remote dir with prefix {prefix} is not empty after deletion: {objects}"


def assert_prefix_not_empty(neon_env_builder: "NeonEnvBuilder", prefix: Optional[str] = None):
    response = list_prefix(neon_env_builder, prefix)
    assert response["KeyCount"] != 0, f"remote dir with prefix {prefix} is empty: {response}"


def list_prefix(
    neon_env_builder: "NeonEnvBuilder", prefix: Optional[str] = None, delimiter: str = "/"
) -> ListObjectsV2OutputTypeDef:
    """
    Note that this function takes into account prefix_in_bucket.
    """
    # For local_fs we need to properly handle empty directories, which we currently don't, so for simplicity stick to s3 api.
    remote = neon_env_builder.pageserver_remote_storage
    assert isinstance(remote, S3Storage), "localfs is currently not supported"
    assert remote.client is not None

    prefix_in_bucket = remote.prefix_in_bucket or ""
    if not prefix:
        prefix = prefix_in_bucket
    else:
        # real s3 tests have uniqie per test prefix
        # mock_s3 tests use special pageserver prefix for pageserver stuff
        prefix = "/".join((prefix_in_bucket, prefix))

    # Note that this doesn't use pagination, so list is not guaranteed to be exhaustive.
    response = remote.client.list_objects_v2(
        Delimiter=delimiter,
        Bucket=remote.bucket_name,
        Prefix=prefix,
    )
    return response


def wait_tenant_status_404(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    iterations: int,
    interval: float = 0.250,
):
    def tenant_is_missing():
        data = {}
        try:
            data = pageserver_http.tenant_status(tenant_id)
            log.info(f"tenant status {data}")
        except PageserverApiException as e:
            log.debug(e)
            if e.status_code == 404:
                return

        raise RuntimeError(f"Timeline exists state {data.get('state')}")

    wait_until(iterations, interval=interval, func=tenant_is_missing)


def tenant_delete_wait_completed(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    iterations: int,
):
    pageserver_http.tenant_delete(tenant_id=tenant_id)
    wait_tenant_status_404(pageserver_http, tenant_id=tenant_id, iterations=iterations)


MANY_SMALL_LAYERS_TENANT_CONFIG = {
    "gc_period": "0s",
    "compaction_period": "0s",
    "checkpoint_distance": f"{1024**2}",
    "image_creation_threshold": "100",
}


def poll_for_remote_storage_iterations(remote_storage_kind: RemoteStorageKind) -> int:
    return 40 if remote_storage_kind is RemoteStorageKind.REAL_S3 else 15
