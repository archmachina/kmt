
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
