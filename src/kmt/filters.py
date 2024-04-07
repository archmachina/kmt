import logging
import hashlib
import base64
import json

from . import types

logger = logging.getLogger(__name__)

def FilterHash(value, name="sha1"):

    # Get a reference to the object to use for hashing
    classref = getattr(hashlib, name)
    instance = classref()

    instance.update(str(value).encode("utf-8"))

    return instance.hexdigest()

def FilterBase64Encode(value, encoding="utf-8"):
    bytes = base64.b64encode(str(value).encode(encoding))
    return bytes.decode("utf-8")

def FilterBase64Decode(value, encoding="utf-8"):
    bytes = value.encode("utf-8")
    return base64.b64decode(bytes).decode(encoding)

def FilterLoadAsString(filename, encoding="utf-8"):
    with open(filename, "r", encoding=encoding) as file:
        content = file.read()

        result = (json.dumps(content))[1:-1]

    return result

types.default_filters["hash"] = FilterHash
types.default_filters["b64encode"] = FilterBase64Encode
types.default_filters["b64decode"] = FilterBase64Decode
types.default_filters["load_as_string"] = FilterLoadAsString
