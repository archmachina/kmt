import logging
import hashlib
import base64
import json
import re
import yaml
import os

import kmt.core as core
import kmt.util as util
import kmt.exception as exception
import kmt.yaml_types as yaml_types

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
def filter_lookup_manifest_name(context, pattern: str, spec:dict=None):

    if spec is None:
        spec = {}
    spec["pattern"] = pattern

    item = lookup_manifest(context, spec)

    return item.spec["metadata"]["name"]

@pass_context
def filter_lookup_manifest(context, pattern: str, spec:dict=None):

    if spec is None:
        spec = {}
    spec["pattern"] = pattern

    item = lookup_manifest(context, spec)

    return item.spec

@pass_context
def filter_hash_manifest(context, pattern: str, spec:dict=None, hash_type:str="sha1"):

    if spec is None:
        spec = {}
    spec["pattern"] = pattern

    # Retrieve the manifest
    item = lookup_manifest(context, spec)

    # Convert the manifest spec to byte encoding
    text = yaml.dump(item.spec)

    return util.hash_string(text, hash_type=hash_type)

def lookup_manifest(context, spec):
    lookup = yaml_types.Lookup(spec)

    item = lookup.find_match(context.parent["kmt_manifests"],
        current_namespace=context.parent.get("kmt_namespace"))

    return item

def filter_env(name, default=None):
    return os.environ.get(name, default)

core.default_filters["hash_string"] = filter_hash_string
core.default_filters["b64encode"] = filter_base64_encode
core.default_filters["b64decode"] = filter_base64_decode
core.default_filters["include_file"] = filter_include_file
core.default_filters["json_escape"] = filter_json_escape
core.default_filters["lookup_manifest"] = filter_lookup_manifest
core.default_filters["lookup_manifest_name"] = filter_lookup_manifest_name
core.default_filters["hash_manifest"] = filter_hash_manifest
core.default_filters["env"] = filter_env
