from .config import H2Configuration as H2Configuration
from .errors import ErrorCodes as ErrorCodes
from .events import AlternativeServiceAvailable as AlternativeServiceAvailable, ConnectionTerminated as ConnectionTerminated, Event as Event, InformationalResponseReceived as InformationalResponseReceived, PingAckReceived as PingAckReceived, PingReceived as PingReceived, PriorityUpdated as PriorityUpdated, RemoteSettingsChanged as RemoteSettingsChanged, RequestReceived as RequestReceived, ResponseReceived as ResponseReceived, SettingsAcknowledged as SettingsAcknowledged, TrailersReceived as TrailersReceived, UnknownFrameReceived as UnknownFrameReceived, WindowUpdated as WindowUpdated
from .exceptions import DenialOfServiceError as DenialOfServiceError, FlowControlError as FlowControlError, FrameTooLargeError as FrameTooLargeError, NoAvailableStreamIDError as NoAvailableStreamIDError, NoSuchStreamError as NoSuchStreamError, ProtocolError as ProtocolError, RFC1122Error as RFC1122Error, StreamClosedError as StreamClosedError, StreamIDTooLowError as StreamIDTooLowError, TooManyStreamsError as TooManyStreamsError
from .frame_buffer import FrameBuffer as FrameBuffer
from .settings import ChangedSetting as ChangedSetting, SettingCodes as SettingCodes, Settings as Settings
from .stream import H2Stream as H2Stream, StreamClosedBy as StreamClosedBy
from .utilities import SizeLimitDict as SizeLimitDict, guard_increment_window as guard_increment_window
from .windows import WindowManager as WindowManager
from _typeshed import Incomplete
from collections.abc import Iterable
from enum import Enum, IntEnum
from hpack.struct import Header as Header, HeaderWeaklyTyped as HeaderWeaklyTyped
from hyperframe.frame import Frame as Frame
from typing import Any

class ConnectionState(Enum):
    IDLE = 0
    CLIENT_OPEN = 1
    SERVER_OPEN = 2
    CLOSED = 3

class ConnectionInputs(Enum):
    SEND_HEADERS = 0
    SEND_PUSH_PROMISE = 1
    SEND_DATA = 2
    SEND_GOAWAY = 3
    SEND_WINDOW_UPDATE = 4
    SEND_PING = 5
    SEND_SETTINGS = 6
    SEND_RST_STREAM = 7
    SEND_PRIORITY = 8
    RECV_HEADERS = 9
    RECV_PUSH_PROMISE = 10
    RECV_DATA = 11
    RECV_GOAWAY = 12
    RECV_WINDOW_UPDATE = 13
    RECV_PING = 14
    RECV_SETTINGS = 15
    RECV_RST_STREAM = 16
    RECV_PRIORITY = 17
    SEND_ALTERNATIVE_SERVICE = 18
    RECV_ALTERNATIVE_SERVICE = 19

class AllowedStreamIDs(IntEnum):
    EVEN = 0
    ODD = 1

class H2ConnectionStateMachine:
    state: Incomplete
    def __init__(self) -> None: ...
    def process_input(self, input_: ConnectionInputs) -> list[Event]: ...

class H2Connection:
    DEFAULT_MAX_OUTBOUND_FRAME_SIZE: int
    DEFAULT_MAX_INBOUND_FRAME_SIZE: Incomplete
    HIGHEST_ALLOWED_STREAM_ID: Incomplete
    MAX_WINDOW_INCREMENT: Incomplete
    DEFAULT_MAX_HEADER_LIST_SIZE: Incomplete
    MAX_CLOSED_STREAMS: Incomplete
    state_machine: Incomplete
    streams: Incomplete
    highest_inbound_stream_id: int
    highest_outbound_stream_id: int
    encoder: Incomplete
    decoder: Incomplete
    config: Incomplete
    local_settings: Incomplete
    remote_settings: Incomplete
    outbound_flow_control_window: Incomplete
    max_outbound_frame_size: Incomplete
    max_inbound_frame_size: Incomplete
    incoming_buffer: Incomplete
    def __init__(self, config: H2Configuration | None = None) -> None: ...
    @property
    def open_outbound_streams(self) -> int: ...
    @property
    def open_inbound_streams(self) -> int: ...
    @property
    def inbound_flow_control_window(self) -> int: ...
    def initiate_connection(self) -> None: ...
    def initiate_upgrade_connection(self, settings_header: bytes | None = None) -> bytes | None: ...
    def get_next_available_stream_id(self) -> int: ...
    def send_headers(self, stream_id: int, headers: Iterable[HeaderWeaklyTyped], end_stream: bool = False, priority_weight: int | None = None, priority_depends_on: int | None = None, priority_exclusive: bool | None = None) -> None: ...
    def send_data(self, stream_id: int, data: bytes | memoryview, end_stream: bool = False, pad_length: Any = None) -> None: ...
    def end_stream(self, stream_id: int) -> None: ...
    def increment_flow_control_window(self, increment: int, stream_id: int | None = None) -> None: ...
    def push_stream(self, stream_id: int, promised_stream_id: int, request_headers: Iterable[HeaderWeaklyTyped]) -> None: ...
    def ping(self, opaque_data: bytes | str) -> None: ...
    def reset_stream(self, stream_id: int, error_code: ErrorCodes | int = 0) -> None: ...
    def close_connection(self, error_code: ErrorCodes | int = 0, additional_data: bytes | None = None, last_stream_id: int | None = None) -> None: ...
    def update_settings(self, new_settings: dict[SettingCodes | int, int]) -> None: ...
    def advertise_alternative_service(self, field_value: bytes | str, origin: bytes | None = None, stream_id: int | None = None) -> None: ...
    def prioritize(self, stream_id: int, weight: int | None = None, depends_on: int | None = None, exclusive: bool | None = None) -> None: ...
    def local_flow_control_window(self, stream_id: int) -> int: ...
    def remote_flow_control_window(self, stream_id: int) -> int: ...
    def acknowledge_received_data(self, acknowledged_size: int, stream_id: int) -> None: ...
    def data_to_send(self, amount: int | None = None) -> bytes: ...
    def clear_outbound_data_buffer(self) -> None: ...
    def receive_data(self, data: bytes) -> list[Event]: ...
