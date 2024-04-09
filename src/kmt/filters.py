import logging
import hashlib
import base64
import json
import re

from . import types
from .exception import PipelineRunException

from jinja2 import pass_context

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

def FilterIncludeFile(filename, encoding="utf-8"):
    with open(filename, "r", encoding=encoding) as file:
        content = file.read()

    return content

def FilterJsonEscape(value):
    return (json.dumps(str(value)))[1:-1]

@pass_context
def FilterLookupManifestName(context, name, group=None, version=None, kind=None, namespace=None):
    item = lookup_manifest(context, name, group, version, kind, namespace)

    return item["metadata"]["name"]

@pass_context
def FilterLookupManifest(context, name, group=None, version=None, kind=None, namespace=None, multiple=False):
    return lookup_manifest(context, name, group, version, kind, namespace, multiple)

def lookup_manifest(context, name, group=None, version=None, kind=None, namespace=None, multiple=False):
    if namespace is None:
        namespace = context.parent.get("kmt_namespace")

    matches = []

    # raise Exception(f"Keys: {context.parent.keys()}")

    manifests = context.parent["kmt_manifests"]
    for manifest in manifests:

        # api version
        item_api_version = manifest.spec.get("apiVersion", "")

        # group and version
        item_group = ""
        item_version = ""
        if item_api_version != "":
            split = item_api_version.split("/")

            if len(split) == 1:
                item_version = split[0]
            elif len(split) == 2:
                item_group = split[0]
                item_version = split[1]

        # Kind
        item_kind = manifest.spec.get("kind", "")

        # Name and Namespace
        item_namespace = ""
        item_name = ""
        metadata = manifest.spec.get("metadata")
        if isinstance(metadata, dict):
            item_name = metadata.get("name", "")
            item_namespace = metadata.get("namespace", "")

        if group is not None and group != item_group:
            continue

        if version is not None and version != item_version:
            continue

        if kind is not None and kind != item_kind:
            continue

        if namespace is not None and namespace != item_namespace:
            continue

        if name is not None and not re.search(name, item_name):
            continue

        matches.append(manifest.spec)

    if len(matches) == 0:
        raise PipelineRunException("Could not find a matching object for lookup_manifest")

    if multiple:
        return matches

    if len(matches) > 1:
        raise PipelineRunException("Could not find a single object for lookup_manifest. Multiple object matches")

    return matches[0]

types.default_filters["hash"] = FilterHash
types.default_filters["b64encode"] = FilterBase64Encode
types.default_filters["b64decode"] = FilterBase64Decode
types.default_filters["include_file"] = FilterIncludeFile
types.default_filters["json_escape"] = FilterJsonEscape
types.default_filters["lookup_manifest"] = FilterLookupManifest
types.default_filters["lookup_manifest_name"] = FilterLookupManifestName
