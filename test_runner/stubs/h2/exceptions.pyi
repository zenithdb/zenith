from .errors import ErrorCodes as ErrorCodes
from _typeshed import Incomplete

class H2Error(Exception): ...

class ProtocolError(H2Error):
    error_code: Incomplete

class FrameTooLargeError(ProtocolError):
    error_code: Incomplete

class FrameDataMissingError(ProtocolError):
    error_code: Incomplete

class TooManyStreamsError(ProtocolError): ...

class FlowControlError(ProtocolError):
    error_code: Incomplete

class StreamIDTooLowError(ProtocolError):
    stream_id: Incomplete
    max_stream_id: Incomplete
    def __init__(self, stream_id: int, max_stream_id: int) -> None: ...

class NoAvailableStreamIDError(ProtocolError): ...

class NoSuchStreamError(ProtocolError):
    stream_id: Incomplete
    def __init__(self, stream_id: int) -> None: ...

class StreamClosedError(NoSuchStreamError):
    stream_id: Incomplete
    error_code: Incomplete
    def __init__(self, stream_id: int) -> None: ...

class InvalidSettingsValueError(ProtocolError, ValueError):
    error_code: Incomplete
    def __init__(self, msg: str, error_code: ErrorCodes) -> None: ...

class InvalidBodyLengthError(ProtocolError):
    expected_length: Incomplete
    actual_length: Incomplete
    def __init__(self, expected: int, actual: int) -> None: ...

class UnsupportedFrameError(ProtocolError): ...
class RFC1122Error(H2Error): ...

class DenialOfServiceError(ProtocolError):
    error_code: Incomplete
