from .errors import ErrorCodes as ErrorCodes
from .settings import ChangedSetting as ChangedSetting, SettingCodes as SettingCodes, Settings as Settings
from _typeshed import Incomplete
from hpack import HeaderTuple as HeaderTuple
from hyperframe.frame import Frame as Frame

class Event: ...

class RequestReceived(Event):
    stream_id: Incomplete
    headers: Incomplete
    stream_ended: Incomplete
    priority_updated: Incomplete
    def __init__(self) -> None: ...

class ResponseReceived(Event):
    stream_id: Incomplete
    headers: Incomplete
    stream_ended: Incomplete
    priority_updated: Incomplete
    def __init__(self) -> None: ...

class TrailersReceived(Event):
    stream_id: Incomplete
    headers: Incomplete
    stream_ended: Incomplete
    priority_updated: Incomplete
    def __init__(self) -> None: ...

class _HeadersSent(Event): ...
class _ResponseSent(_HeadersSent): ...
class _RequestSent(_HeadersSent): ...
class _TrailersSent(_HeadersSent): ...
class _PushedRequestSent(_HeadersSent): ...

class InformationalResponseReceived(Event):
    stream_id: Incomplete
    headers: Incomplete
    priority_updated: Incomplete
    def __init__(self) -> None: ...

class DataReceived(Event):
    stream_id: Incomplete
    data: Incomplete
    flow_controlled_length: Incomplete
    stream_ended: Incomplete
    def __init__(self) -> None: ...

class WindowUpdated(Event):
    stream_id: Incomplete
    delta: Incomplete
    def __init__(self) -> None: ...

class RemoteSettingsChanged(Event):
    changed_settings: Incomplete
    def __init__(self) -> None: ...
    @classmethod
    def from_settings(cls, old_settings: Settings | dict[int, int], new_settings: dict[int, int]) -> RemoteSettingsChanged: ...

class PingReceived(Event):
    ping_data: Incomplete
    def __init__(self) -> None: ...

class PingAckReceived(Event):
    ping_data: Incomplete
    def __init__(self) -> None: ...

class StreamEnded(Event):
    stream_id: Incomplete
    def __init__(self) -> None: ...

class StreamReset(Event):
    stream_id: Incomplete
    error_code: Incomplete
    remote_reset: bool
    def __init__(self) -> None: ...

class PushedStreamReceived(Event):
    pushed_stream_id: Incomplete
    parent_stream_id: Incomplete
    headers: Incomplete
    def __init__(self) -> None: ...

class SettingsAcknowledged(Event):
    changed_settings: Incomplete
    def __init__(self) -> None: ...

class PriorityUpdated(Event):
    stream_id: Incomplete
    weight: Incomplete
    depends_on: Incomplete
    exclusive: Incomplete
    def __init__(self) -> None: ...

class ConnectionTerminated(Event):
    error_code: Incomplete
    last_stream_id: Incomplete
    additional_data: Incomplete
    def __init__(self) -> None: ...

class AlternativeServiceAvailable(Event):
    origin: Incomplete
    field_value: Incomplete
    def __init__(self) -> None: ...

class UnknownFrameReceived(Event):
    frame: Incomplete
    def __init__(self) -> None: ...
