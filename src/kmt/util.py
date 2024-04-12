
import logging
import hashlib
import textwrap
import yaml
import re

from . import exception
from . import types

logger = logging.getLogger(__name__)

def validate(val, message, extype=exception.ValidationException):
    if not val:
        raise extype(message)

def hash_string(source, hash_type="sha1", encoding="utf-8"):
    validate(isinstance(source, str), "Invalid source string supplied to hash_string")
    validate(isinstance(hash_type, str), "Invalid hash type supplied to hash_object")
    validate(isinstance(encoding, str), "Invalid encoding supplied to hash_object")

    # Determine whether we should generate a short sum
    method = hash_type
    if hash_type == "short8" or hash_type == "short10":
        method = "sha256"

    # Get a reference to the object to use for hashing
    classref = getattr(hashlib, method)
    instance = classref()

    instance.update(str(source).encode(encoding))

    result = instance.hexdigest()

    if hash_type == "short8":
        short = 0
        short_wrap = textwrap.wrap(result, 8)
        for item in short_wrap:
            short = short ^ int(item, 16)
        result = short
    elif hash_type == "short10":
        result = result[:10]

    return result

def hash_object(source, hash_type="sha1"):
    validate(source is not None, "Invalid source supplied to hash_object")

    text = yaml.dump(source)

    return hash_string(text, hash_type=hash_type)

def extract_manifest_info(manifest, default_value=None):
    validate(isinstance(manifest, (dict, types.Manifest)), "Invalid manifest supplied to extract_manifest_info")

    if isinstance(manifest, types.Manifest):
        manifest = manifest.spec

    # api version
    # Don't use the 'default_value' yet as we want to know whether it exists first
    api_version = manifest.get("apiVersion")

    # group and version
    group = default_value
    version = default_value
    if isinstance(api_version, str) and api_version != "":
        split = api_version.split("/")

        if len(split) == 1:
            version = split[0]
        elif len(split) == 2:
            group = split[0]
            version = split[1]

    # Update the api_version to the default, if it didn't exist or was None
    if api_version is None:
        api_version = default_value

    # Kind
    kind = manifest.get("kind", default_value)

    # Name and Namespace
    namespace = default_value
    name = default_value
    metadata = manifest.get("metadata")
    if isinstance(metadata, dict):
        name = metadata.get("name", default_value)
        namespace = metadata.get("namespace", default_value)

    return {
        "group": group,
        "version": version,
        "kind": kind,
        "api_version": api_version,
        "namespace": namespace,
        "name": name
    }
