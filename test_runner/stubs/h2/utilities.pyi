import collections
from .exceptions import FlowControlError as FlowControlError, ProtocolError as ProtocolError
from _typeshed import Incomplete
from collections.abc import Generator, Iterable
from hpack.struct import Header as Header, HeaderWeaklyTyped as HeaderWeaklyTyped
from typing import Any, NamedTuple

UPPER_RE: Incomplete
SIGIL: Incomplete
INFORMATIONAL_START: Incomplete
CONNECTION_HEADERS: Incomplete

def extract_method_header(headers: Iterable[Header]) -> bytes | None: ...
def is_informational_response(headers: Iterable[Header]) -> bool: ...
def guard_increment_window(current: int, increment: int) -> int: ...
def authority_from_headers(headers: Iterable[Header]) -> bytes | None: ...

class HeaderValidationFlags(NamedTuple):
    is_client: bool
    is_trailer: bool
    is_response_header: bool
    is_push_promise: bool

def validate_headers(headers: Iterable[Header], hdr_validation_flags: HeaderValidationFlags) -> Iterable[Header]: ...
def utf8_encode_headers(headers: Iterable[HeaderWeaklyTyped]) -> list[Header]: ...
def normalize_outbound_headers(headers: Iterable[Header], hdr_validation_flags: HeaderValidationFlags | None, should_split_outbound_cookies: bool = False) -> Generator[Header, None, None]: ...
def normalize_inbound_headers(headers: Iterable[Header], hdr_validation_flags: HeaderValidationFlags) -> Generator[Header, None, None]: ...
def validate_outbound_headers(headers: Iterable[Header], hdr_validation_flags: HeaderValidationFlags) -> Generator[Header, None, None]: ...

class SizeLimitDict(collections.OrderedDict[int, Any]):
    def __init__(self, *args: dict[int, int], **kwargs: Any) -> None: ...
    def __setitem__(self, key: int, value: Any | int) -> None: ...
