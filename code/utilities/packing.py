import msgpack


def _encode_ext_type(obj):
    if isinstance(obj, set):  # encode 'set' with typecode 10
        return msgpack.ExtType(10, packb(tuple(obj)))
    raise TypeError("unknown extended type for %r" % type(obj))


def _decode_ext_type(typecode, data):
    if typecode == 10:  # decode a 'set' type
        return set(unpackb(data, use_list=False))
    raise TypeError("unknown extended typecode %d" % typecode)


def pack(obj, stream, **kwargs):
    """Pack object using MessagePack (with extended types support)
       and write packed bytes to a stream."""
    msgpack.pack(obj, stream, default=_encode_ext_type, use_bin_type=True, **kwargs)


def packb(obj, **kwargs):
    """Pack object using MessagePack (with extended types support)
       and return packed bytes."""
    return msgpack.packb(obj, default=_encode_ext_type, use_bin_type=True, **kwargs)


def packer(*args, **kwargs):
    """Return a MessagePack (with extended types support) Packer object."""
    return msgpack.Packer(*args, default=_encode_ext_type, use_bin_type=True, **kwargs)


def unpack(stream, **kwargs):
    """Unpack a stream of packed bytes using MessagePack (with extended types support)
       and return unpacked object."""
    return msgpack.unpack(stream, ext_hook=_decode_ext_type, raw=False, **kwargs)


def unpackb(packed, **kwargs):
    """Unpack packed bytes using MessagePack (with extended types support)
       and return unpacked object."""
    return msgpack.unpackb(packed, ext_hook=_decode_ext_type, raw=False, **kwargs)


def unpacker(*args, **kwargs):
    """Return a MessagePack (with extended types support) Unpacker object."""
    return msgpack.Unpacker(*args, ext_hook=_decode_ext_type, raw=False, **kwargs)
