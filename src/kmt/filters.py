import logging
import hashlib
import base64

from . import types

logger = logging.getLogger(__name__)

def FilterHash(value, name="sha1"):

    # Get a reference to the object to use for hashing
    classref = getattr(hashlib, name)
    instance = classref()

    instance.update(str(value).encode("utf-8"))

    return instance.hexdigest()

def FilterBase64Encode(value, encoding="utf-8"):
    return base64.b64encode(str(value).encode(encoding))

def FilterBase64Decode(value, encoding="utf-8"):
    return base64.b64decode(value).decode(encoding)

types.default_filters["hash"] = FilterHash
types.default_filters["b64encode"] = FilterBase64Encode
types.default_filters["b64decode"] = FilterBase64Decode
