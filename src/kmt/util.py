
import logging
import hashlib
import textwrap
import yaml
import copy
import jinja2
import re

from jinja2.meta import find_undeclared_variables

import kmt.yaml_types as yaml_types
import kmt.exception as exception
import kmt.core as core

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
    instance = hashlib.new(method)

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

    text = yaml_dump(source)

    return hash_string(text, hash_type=hash_type)

def hash_manifest(source, hash_type="sha1"):
    validate(source is not None, "Invalid source supplied to hash_manifest")

    new_obj = source.copy()

    # Metadata changes shouldn't affect the hash
    if "metadata" in new_obj:
        new_obj.pop("metadata")

    text = yaml_dump(new_obj)

    return hash_string(text, hash_type=hash_type)

def walk_object(object, callback, update=False):
    validate(object is not None, "Invalid object supplied to walk_object")
    validate(callable(callback), "Invalid callback supplied to walk_object")
    validate(isinstance(update, bool), "Invalid update flag passed to walk_object")

    # Always visit the top level object
    ret = callback(object)
    if update:
        # If we're updating, this becomes the new root level object to return
        object = ret

    if not isinstance(object, (dict, list)):
        # Nothing else to do for this object
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
                ret = callback(current[key])
                if update:
                    current[key] = ret

                if isinstance(current[key], (dict, list)):
                    item_list.append(current[key])
        elif isinstance(current, list):
            index = 0
            while index < len(current):
                ret = callback(current[index])
                if update:
                    current[index] = ret

                if isinstance(current[index], (dict, list)):
                    item_list.append(current[index])

                index = index + 1
        else:
            # Anything non dictionary or list should never have ended up in this list, so this
            # is really an internal error
            raise exception.KMTInternalException(f"Invalid type for resolve in walk_object: {type(current)}")

    return object

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
                    parsed = yaml_load(val)

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
    if val is None:
        return default

    return val

def _get_template_str_vars(template_str, environment:jinja2.Environment):
    if not isinstance(template_str, str):
        return set()

    ast = environment.parse(template_str)
    deps = set(find_undeclared_variables(ast))

    return deps

def resolve_var_refs(source_vars:dict, environment:jinja2.Environment, inplace:bool=False, ignore_list:list=None):
    """
    Performs templating on the source dictionary and attempts to resolve variable references
    taking in to account nested references
    """
    validate(isinstance(source_vars, dict), "Invalid source vars provided to resolve_var_refs")
    validate(isinstance(inplace, bool), "Invalid inplace var provided to resolve_var_refs")
    validate(ignore_list is None or (all(isinstance(x, str) for x in ignore_list)),
        "Invalid ignore_list provided to resolve_var_refs")

    if ignore_list is None:
        ignore_list = []

    var_map = {}

    working_vars = source_vars
    if not inplace:
        working_vars = copy.deepcopy(source_vars)

    # Create a map of keys to the vars the value references
    for key in working_vars:
        deps = set()

        # Recursively walk through all properties for the object and calculate a set
        # of dependencies
        # It's possible that some dependencies could be resolvable, but will show as unresolvable here:
        # If a.b depends on x.y, and x.x depends on a.a, it could, in theory, be resolved, but this will
        # show it as unresolvable.
        # Since we don't have access to that info from jinja2 easily, it would be difficult to calculate
        # and offers little value being a small edge case.
        # Skip calculating dependencies, if the key is in the ignore list, meaning it has no dependencies
        if key not in ignore_list:
            walk_object(working_vars[key], lambda x: deps.update(_get_template_str_vars(x, environment)))

        var_map[key] = deps

    # Loop while there are values left in var_map, which represents the key/value
    # and the vars it depends on
    while len(var_map.keys()) > 0:
        process_list = []

        # Add any keys to the process list that don't have any dependencies left
        for key in var_map:
            if len(var_map[key]) == 0:
                process_list.append(key)

        # Remove the items we're processing from the var map
        for key in process_list:
            var_map.pop(key)

        # Fail if there is nothing to process
        if len(process_list) < 1:
            raise exception.KMTResolveException(
                f"Circular or unresolvable variable references: vars {var_map}"
            )

        for prockey in process_list:
            if prockey not in ignore_list:
                # Template the variable and update 'new_vars', if it's not in the ignore_list
                working_vars[prockey] = walk_object(
                    working_vars[prockey],
                    lambda x: _template_if_string(x, environment, working_vars),
                    update=True
                )

            # Remove the variable as a dependency for all other variables
            for key in var_map:
                if prockey in var_map[key]:
                    var_map[key].remove(prockey)

    return working_vars

def _template_if_string(source, environment:jinja2.Environment, template_vars:dict):
    validate(isinstance(environment, jinja2.Environment), "Invalid environment passed to _template_string")
    validate(isinstance(template_vars, dict), "Invalid template_vars passed to _template_string")

    if not isinstance(source, str):
        return source

    template = environment.from_string(source)
    return template.render(template_vars)

def yaml_dump(source):
    dumper = yaml.SafeDumper

    return yaml.dump(source, Dumper=dumper, explicit_start=True, sort_keys=False, indent=2)

def yaml_dump_all(source):
    dumper = yaml.SafeDumper

    return yaml.dump_all(source, Dumper=dumper, explicit_start=True, sort_keys=False, indent=2)

def yaml_load(source):
    loader = yaml.SafeLoader

    return yaml.load(source, Loader=loader)

def yaml_load_all(source):
    loader = yaml.SafeLoader

    return yaml.load_all(source, Loader=loader)

def check_find_manifests_keys(search:dict):
    validate(isinstance(search, dict), "Invalid search supplied to check_find_manifests_keys")

    allowed_keys = [
        "group",
        "version",
        "kind",
        "api_version",
        "namespace",
        "pattern",
        "alias"
    ]

    errored = []
    for key in search.keys():
        if key not in allowed_keys or not isinstance(search[key], str):
            errored.append(key)

    if len(errored) > 0:
        raise exception.PipelineRunException(f"Invalid keys or invalid key value found on lookup: {errored}")

def find_manifests(search, manifests, *, multiple, current_namespace=None):
    validate(isinstance(search, dict) and all(isinstance(x, (str, type(None))) for x in search.values()),
        "Invalid search criteria provided to find_manifests")
    validate(isinstance(manifests, list) and all(isinstance(x, core.Manifest) for x in manifests),
        "Invalid manifests provided to find_manifests")
    validate(isinstance(multiple, bool), "Invalid multiple parameter to find_manifests")
    validate(current_namespace is None or isinstance(current_namespace, str),
        "Invalid current_namespace provided to find_manifests")

    matches = []

    for manifest in manifests:

        info = manifest.get_info()

        if "group" in search and search["group"] != info["group"]:
            continue

        if "version" in search and search["version"] != info["version"]:
            continue

        if "kind" in search and search["kind"] != info["kind"]:
            continue

        if "api_version" in search and search["api_version"] != info["api_version"]:
            continue

        if "namespace" in search:
            if search["namespace"] != info["namespace"]:
                continue
        elif info["namespace"] is not None and info["namespace"] != current_namespace:
            # If no namespace has been defined in the lookup, we will match on
            # the current namespace and any resource without a namespace.
            continue

        if "pattern" in search and search["pattern"] is not None and not re.search(search["pattern"], info["name"]):
            continue

        if "alias" in search and (search["alias"] != info["alias"] or search["alias"] == info["name"]):
            continue

        matches.append(manifest)

    if multiple:
        return matches

    if len(matches) == 0:
        raise exception.PipelineRunException(f"Could not find a matching object in find_manifests. search: {search}")

    if len(matches) > 1:
        raise exception.PipelineRunException(f"Could not find a single object in find_manifests. Multiple object matches. search: {search}")

    return matches[0]
