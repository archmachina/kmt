import logging
import hashlib
import base64
import json
import re
import yaml

from . import types
from . import util
from .exception import PipelineRunException

from jinja2 import pass_context

logger = logging.getLogger(__name__)

def filter_hash_string(value, hash_type="sha1"):
    return util.hash_string(value, hash_type)

def filter_base64_encode(value, encoding="utf-8"):
    bytes = base64.b64encode(str(value).encode(encoding))
    return bytes.decode("utf-8")

def filter_base64_decode(value, encoding="utf-8"):
    bytes = value.encode("utf-8")
    return base64.b64decode(bytes).decode(encoding)

def filter_include_file(filename, encoding="utf-8"):
    with open(filename, "r", encoding=encoding) as file:
        content = file.read()

    return content

def filter_json_escape(value):
    return (json.dumps(str(value)))[1:-1]

@pass_context
def filter_lookup_manifest_name(context, name, group=None, version=None, kind=None, namespace=None):
    item = lookup_manifest(context, name, group, version, kind, namespace, multiple=False)

    return item["metadata"]["name"]

@pass_context
def filter_lookup_manifest(context, name, group=None, version=None, kind=None, namespace=None, multiple=False):
    return lookup_manifest(context, name, group, version, kind, namespace, multiple)

@pass_context
def filter_hash_manifest(context, name, hash_type="sha1", group=None, version=None, kind=None, namespace=None):

    # Retrieve the manifest
    manifest = lookup_manifest(context, name, group, version, kind, namespace, multiple=False)

    # Convert the manifest spec to byte encoding
    text = yaml.dump(manifest)

    return util.hash_string(text)

def lookup_manifest(context, name, group=None, version=None, kind=None, namespace=None, multiple=False):
    if namespace is None:
        namespace = context.parent.get("kmt_namespace")

    matches = []

    # raise Exception(f"Keys: {context.parent.keys()}")

    manifests = context.parent["kmt_manifests"]
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

        if name is not None and not re.search(name, info.name):
            continue

        matches.append(manifest.spec)

    if len(matches) == 0:
        raise PipelineRunException("Could not find a matching object for lookup_manifest")

    if multiple:
        return matches

    if len(matches) > 1:
        raise PipelineRunException("Could not find a single object for lookup_manifest. Multiple object matches")

    return matches[0]

types.default_filters["hash_string"] = filter_hash_string
types.default_filters["b64encode"] = filter_base64_encode
types.default_filters["b64decode"] = filter_base64_decode
types.default_filters["include_file"] = filter_include_file
types.default_filters["json_escape"] = filter_json_escape
types.default_filters["lookup_manifest"] = filter_lookup_manifest
types.default_filters["lookup_manifest_name"] = filter_lookup_manifest_name
types.default_filters["hash_manifest"] = filter_hash_manifest
