
import jinja2
import logging
import hashlib
import textwrap

from .exception import *
from . import types

logger = logging.getLogger(__name__)

def validate(val, message, extype=ValidationException):
    if not val:
        raise extype(message)

def block_sum(block):
    validate(isinstance(block, types.TextBlock), "Invalid text block passed to _block_sum")

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
