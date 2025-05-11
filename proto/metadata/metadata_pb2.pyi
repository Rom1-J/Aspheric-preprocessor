from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Metadata(_message.Message):
    __slots__ = ("id", "emails", "ips", "domains")
    ID_FIELD_NUMBER: _ClassVar[int]
    EMAILS_FIELD_NUMBER: _ClassVar[int]
    IPS_FIELD_NUMBER: _ClassVar[int]
    DOMAINS_FIELD_NUMBER: _ClassVar[int]
    id: str
    emails: _containers.RepeatedScalarFieldContainer[bytes]
    ips: _containers.RepeatedScalarFieldContainer[bytes]
    domains: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, id: _Optional[str] = ..., emails: _Optional[_Iterable[bytes]] = ..., ips: _Optional[_Iterable[bytes]] = ..., domains: _Optional[_Iterable[bytes]] = ...) -> None: ...

class MetadataList(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[Metadata]
    def __init__(self, items: _Optional[_Iterable[_Union[Metadata, _Mapping]]] = ...) -> None: ...
