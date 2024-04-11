
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

def refresh_manifest_hash(manifest):
    validate(isinstance(manifest, types.Manifest), "Invalid manifest passed to hash_manifest")

    text = yaml.dump(manifest.spec)

    manifest.vars["kmt_md5sum"] = hash_string(text, hash_type="md5", encoding="utf-8")
    manifest.vars["kmt_sha1sum"] = hash_string(text, hash_type="sha1", encoding="utf-8")
    manifest.vars["kmt_sha256sum"] = hash_string(text, hash_type="sha256", encoding="utf-8")
    manifest.vars["kmt_shortsum"] = hash_string(text, hash_type="short8", encoding="utf-8")

def lookup_manifest(manifests, *, group=None, version=None, kind=None, namespace=None, pattern=None, multiple=False):
    matches = []

    for manifest in manifests:

        info = types.ManifestInfo(manifest.spec)

        if group is not None and group != info.group:
            continue

        if version is not None and version != info.version:
            continue

        if kind is not None and kind != info.kind:
            continue

        if namespace is not None and namespace != info.namespace:
            continue

        if pattern is not None and not re.search(pattern, info.name):
            continue

        matches.append(manifest.spec)

    if len(matches) == 0:
        raise exception.PipelineRunException("Could not find a matching object for lookup_manifest")

    if multiple:
        return matches

    if len(matches) > 1:
        raise exception.PipelineRunException("Could not find a single object for lookup_manifest. Multiple object matches")

    return matches[0]
