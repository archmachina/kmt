
import logging
import hashlib
import textwrap
import yaml
import re

from . import exception
from . import types
from . import yamlwrap

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

    validate(method in ["md5", "sha256", "sha1"], f"Invalid hashing method supplied: {method}")

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
        result = format(short, "x")
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

def walk_object(object, callback):
    validate(object is not None, "Invalid object supplied to walk_object")
    validate(callable(callback), "Invalid callback supplied to walk_object")

    if not isinstance(object, (dict, list)):
        return object

    visited = set()
    item_list = [object]

    while len(item_list) > 0:
        if len(item_list) > 10000:
            raise exception.RecursionLimitException("Exceeded the maximum recursion depth limit")

        current = item_list.pop()

        # Check if we've seen this object before
        if id(current) in visited:
            continue

        # Save this to the visited list, so we don't revisit again, if there is a loop
        # in the origin object
        visited.add(id(current))

        if isinstance(current, dict):
            for key in current:
                # Call the callback to replace the current object
                current[key] = callback(current[key])

                if isinstance(current[key], (dict, list)):
                    item_list.append(current[key])
        elif isinstance(current, list):
            index = 0
            while index < len(current):
                current[index] = callback(current[index])

                if isinstance(current[index], (dict, list)):
                    item_list.append(current[index])

                index = index + 1
        else:
            # Anything non dictionary or list should never have ended up in this list, so this
            # is really an internal error
            raise exception.KMTInternalException(f"Invalid type for resolve in walk_object: {type(current)}")

def coerce_value(types, val):
    if types is None:
        # Nothing to do here
        return val

    if isinstance(types, type):
        types = (types,)

    validate(isinstance(types, tuple) and all(isinstance(x, type) for x in types),
        "Invalid types passed to coerce_value")

    parsed = None

    for type_item in types:
        # Return val if it is already the correct type
        if isinstance(val, type_item):
            return val

        if type_item == bool:
            try:
                result = parse_bool(val)
                return result
            except:
                pass
        elif type_item == str:
            if val is None:
                # Don't convert None to string. This is likely not wanted.
                continue

            return str(val)

        # None of the above have worked, try parsing as yaml to see if it
        # becomes the correct type
        if isinstance(val, str):
            try:
                if parsed is None:
                    parsed = yamlwrap.load(val)

                if isinstance(parsed, type_item):
                    return parsed
            except yaml.YAMLError as e:
                pass

    raise exception.KMTConversionException(f"Could not convert value to target types: {types}")

def parse_bool(obj) -> bool:
    validate(obj is not None, "None value passed to parse_bool")

    if isinstance(obj, bool):
        return obj

    obj = str(obj)

    if obj.lower() in ["true", "1"]:
        return True

    if obj.lower() in ["false", "0"]:
        return False

    raise exception.KMTConversionException(f"Unparseable value ({obj}) passed to parse_bool")

def extract_property(spec, key, /, default=None, required=False):
    validate(isinstance(spec, dict), "Invalid spec passed to extract_property. Must be dict")

    if key not in spec:
        # Raise exception is the key isn't present, but required
        if required:
            raise KeyError(f'Missing key "{key}" in spec or value is null')

        # If the key is not present, return the default
        return default

    # Retrieve value
    val = spec.pop(key)

    return val
