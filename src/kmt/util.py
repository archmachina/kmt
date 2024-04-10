
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

def manifest_hash(manifest):
    validate(isinstance(manifest, types.Manifest), "Invalid manifest passed to manifest_hash")

    md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    sha256 = hashlib.sha256()

    text = yaml.dump(manifest.spec)
    encode = text.encode("utf-8")
    md5.update(encode)
    sha1.update(encode)
    sha256.update(encode)

    manifest.vars["kmt_md5sum"] = md5.hexdigest()
    manifest.vars["kmt_sha1sum"] = sha1.hexdigest()
    manifest.vars["kmt_sha256sum"] = sha256.hexdigest()

    short = 0
    short_wrap = textwrap.wrap(sha256.hexdigest(), 8)
    for item in short_wrap:
        short = short ^ int(item, 16)

    manifest.vars["kmt_shortsum"] = format(short, 'x')

def lookup_manifest(manifests, pattern, group=None, version=None, kind=None, namespace=None, multiple=False):
    matches = []

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

        if pattern is not None and not re.search(pattern, item_name):
            continue

        matches.append(manifest.spec)

    if len(matches) == 0:
        raise exception.PipelineRunException("Could not find a matching object for lookup_manifest")

    if multiple:
        return matches

    if len(matches) > 1:
        raise exception.PipelineRunException("Could not find a single object for lookup_manifest. Multiple object matches")

    return matches[0]
