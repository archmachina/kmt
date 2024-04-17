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

@pass_context
def global_include_file_str(context, filename, template=False, encoding="utf-8"):
    with open(filename, "r", encoding=encoding) as file:
        content = file.read()

    if template:
        template_obj = context.environment.from_string(content)
        content = template_obj.render(context.parent)

    return (json.dumps(str(content)))[1:-1]

@pass_context
def global_lookup_manifest_name(context, **kwargs):

    lookup = yaml_types.LookupName(kwargs)

    scope = context.environment.kmt_manifest
    if scope is None:
        scope = context.environment.kmt_pipeline

    item = lookup.resolve(scope)

    return item

@pass_context
def global_lookup_manifest(context, **kwargs):

    lookup = yaml_types.Lookup(kwargs)

    scope = context.environment.kmt_manifest
    if scope is None:
        scope = context.environment.kmt_pipeline

    item = lookup.resolve(scope)

    return item

@pass_context
def global_hash_manifest(context, **kwargs):

    lookup = yaml_types.LookupHash(kwargs)

    scope = context.environment.kmt_manifest
    if scope is None:
        scope = context.environment.kmt_pipeline

    item = lookup.resolve(scope)

    return item

@pass_context
def global_hash_self(context, hash_type='sha1'):

    manifest = context.environment.kmt_manifest
    if manifest is None:
        raise exception.KMTTemplateException("Attempt to generate self hash with no current manifest")

    return util.hash_manifest(manifest.spec, hash_type=hash_type)

def global_env(name=None):
    env = os.environ.copy()

    if name is not None:
        return env.get(name)

    return env

def global_fail(message:str):
    raise exception.KMTTemplateException(message)

core.default_filters["hash_string"] = filter_hash_string
core.default_filters["b64encode"] = filter_base64_encode
core.default_filters["b64decode"] = filter_base64_decode

core.default_globals["env"] = global_env
core.default_globals["include_file_str"] = global_include_file_str
core.default_globals["lookup_manifest"] = global_lookup_manifest
core.default_globals["lookup_manifest_name"] = global_lookup_manifest_name
core.default_globals["hash_manifest"] = global_hash_manifest
core.default_globals["hash_self"] = global_hash_self
core.default_globals["fail"] = global_fail
