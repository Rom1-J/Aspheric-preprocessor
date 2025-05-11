from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Bucket(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DUMPSTER: _ClassVar[Bucket]
    LEAKS_LOGS: _ClassVar[Bucket]
    LEAKS_DATABASES: _ClassVar[Bucket]
    COMBINATIONS: _ClassVar[Bucket]
    PASTES: _ClassVar[Bucket]
DUMPSTER: Bucket
LEAKS_LOGS: Bucket
LEAKS_DATABASES: Bucket
COMBINATIONS: Bucket
PASTES: Bucket

class MetadataInfo(_message.Message):
    __slots__ = ("id", "date", "bucket", "path", "size", "simhash", "children")
    ID_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    SIMHASH_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    id: str
    date: str
    bucket: Bucket
    path: bytes
    size: int
    simhash: int
    children: _containers.RepeatedCompositeFieldContainer[MetadataInfo]
    def __init__(self, id: _Optional[str] = ..., date: _Optional[str] = ..., bucket: _Optional[_Union[Bucket, str]] = ..., path: _Optional[bytes] = ..., size: _Optional[int] = ..., simhash: _Optional[int] = ..., children: _Optional[_Iterable[_Union[MetadataInfo, _Mapping]]] = ...) -> None: ...
