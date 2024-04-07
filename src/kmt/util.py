
import logging
import hashlib
import textwrap
import yaml

from . import exception
from . import types

logger = logging.getLogger(__name__)

def validate(val, message, extype=exception.ValidationException):
    if not val:
        raise extype(message)

def refresh_metadata(block):
    validate(isinstance(block, types.TextBlock), "Invalid TextBlock passed to refresh_metadata")

    # Best effort extract of Group, Version, Kind, Name from the object, if
    # it is yaml

    manifest = None
    try:
        manifest = yaml.safe_load(block.text)
        if not isinstance(manifest, dict):
            logger.debug(f"refresh_metadata: Parsed yaml is not a dictionary")
            manifest = None
    except yaml.YAMLError as exc:
        logger.debug(f"refresh_metadata: Could not parse input object: {exc}")

    api_version = ""
    group = ""
    version = ""
    kind = ""
    namespace = ""
    name = ""

    if manifest is not None:
        # api version
        api_version = manifest.get("apiVersion", "")

        # group and version
        if api_version != "":
            split = api_version.split("/")

            if len(split) == 1:
                version = split[0]
            elif len(split) == 2:
                group = split[0]
                version = split[1]

        # Kind
        kind = manifest.get("kind", "")

        # Name and Namespace
        metadata = manifest.get("metadata")
        if isinstance(metadata, dict):
            name = metadata.get("name", "")
            namespace = metadata.get("namespace", "")

    block.vars["metadata_group"] = group
    block.vars["metadata_version"] = version
    block.vars["metadata_kind"] = kind
    block.vars["metadata_namespace"] = namespace
    block.vars["metadata_name"] = name
    block.vars["metadata_api_version"] = api_version
    block.vars["metadata_manifest"] = manifest

def block_sum(block):
    validate(isinstance(block, types.TextBlock), "Invalid text block passed to block_sum")

    md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    sha256 = hashlib.sha256()

    encode = block.text.encode("utf-8")
    md5.update(encode)
    sha1.update(encode)
    sha256.update(encode)

    block.vars["md5sum"] = md5.hexdigest()
    block.vars["sha1sum"] = sha1.hexdigest()
    block.vars["sha256sum"] = sha256.hexdigest()

    short = 0
    short_wrap = textwrap.wrap(sha256.hexdigest(), 8)
    for item in short_wrap:
        short = short ^ int(item, 16)

    block.vars["shortsum"] = format(short, 'x')
