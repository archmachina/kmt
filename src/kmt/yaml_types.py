import re
import yaml

import kmt.core as core
import kmt.exception as exception
import kmt.util as util

#
# Manifest name lookups
#
# Performs lookups of manifests based on search keys

class YamlTag:
    def resolve(self, scope):
        raise exception.KMTUnimplementedException("Unimplemented")

    def get_current_namespace(self, scope):
        if not isinstance(scope, core.Manifest):
            return None

        metadata = scope.spec.get("metadata")
        if metadata is None:
            return None

        return metadata.get("namespace")

    def get_manifests(self, scope):
        pipeline = None
        if isinstance(scope, core.Manifest):
            pipeline = scope.pipeline
        elif isinstance(scope, core.Pipeline):
            pipeline = scope
        else:
            raise exception.KMTInternalException("Invalid scope passed to get_manifests. Must be Manifest or Pipeline")

        return pipeline.manifests

class Lookup(YamlTag):
    def __init__(self, spec):
        util.validate(isinstance(spec, dict), "Invalid specification passed to Lookup")

        self.spec = spec.copy()
        util.check_find_manifests_keys(self.spec)

    def resolve(self, scope):
        current_namespace = self.get_current_namespace(scope)
        manifests = self.get_manifests(scope)

        manifest = util.find_manifests(self.spec, manifests, multiple=False, current_namespace=current_namespace)

        return manifest.spec

class LookupName(YamlTag):
    def __init__(self, spec):
        util.validate(isinstance(spec, dict), "Invalid specification passed to LookupName")

        self.spec = spec.copy()
        util.check_find_manifests_keys(self.spec)

    def resolve(self, scope):
        current_namespace = self.get_current_namespace(scope)
        manifests = self.get_manifests(scope)

        item = util.find_manifests(self.spec, manifests, multiple=False, current_namespace=current_namespace)

        metadata = item.spec.get("metadata")
        if not isinstance(metadata, dict):
            raise exception.KMTManifestException("Invalid metadata on manifest")

        name = metadata.get("name")
        if not isinstance(name, str):
            raise exception.KMTManifestException("Invalid name in manifest metadata")

        return name

class LookupHash(YamlTag):
    def __init__(self, spec):
        util.validate(isinstance(spec, dict), "Invalid specification passed to LookupHash")

        self.spec = spec.copy()

        hash_type = "sha1"
        if "hash_type" in self.spec:
            hash_type = self.spec.pop("hash_type")

        util.check_find_manifests_keys(self.spec)

        self.spec["hash_type"] = hash_type

    def resolve(self, scope):
        current_namespace = self.get_current_namespace(scope)
        manifests = self.get_manifests(scope)

        item = util.find_manifests(self.spec, manifests, multiple=False, current_namespace=current_namespace)

        return util.hash_manifest(item.spec, hash_type=self.spec["hash_type"])

class LookupConfigMap(YamlTag):
    def __init__(self, name):
        util.validate(isinstance(name, str), "Invalid name passed to LookupConfigMap")

        self.name = name

    def resolve(self, scope):
        current_namespace = self.get_current_namespace(scope)
        manifests = self.get_manifests(scope)

        spec = {
            "kind": "ConfigMap",
            "alias": self.name
        }

        matches = util.find_manifests(spec, manifests, multiple=True, current_namespace=current_namespace)

        if len(matches) < 1:
            raise exception.KMTTemplateException(f"Could not find matching configmap: {self.name}")

        if len(matches) > 1:
            raise exception.KMTTemplateException(f"Found multiple matches for configmap: {self.name}")

        info = matches[0].get_info()

        return info["name"]

class LookupSecret(YamlTag):
    def __init__(self, name):
        util.validate(isinstance(name, str), "Invalid name passed to LookupSecret")

        self.name = name

    def resolve(self, scope):
        current_namespace = self.get_current_namespace(scope)
        manifests = self.get_manifests(scope)

        spec = {
            "kind": "Secret",
            "alias": self.name
        }

        matches = util.find_manifests(spec, manifests, multiple=True, current_namespace=current_namespace)

        if len(matches) < 1:
            raise exception.KMTTemplateException(f"Could not find matching secret: {self.name}")

        if len(matches) > 1:
            raise exception.KMTTemplateException(f"Found multiple matches for secret: {self.name}")

        info = matches[0].get_info()

        return info["name"]

def lookup_representer(dumper: yaml.SafeDumper, lookup: Lookup):
    return dumper.represent_mapping("!lookup", lookup.spec)

def lookup_name_representer(dumper: yaml.SafeDumper, lookup_name: LookupName):
    return dumper.represent_mapping("!name", lookup_name.spec)

def lookup_hash_representer(dumper: yaml.SafeDumper, lookup_hash: LookupHash):
    return dumper.represent_mapping("!hash", lookup_hash.spec)

def lookup_configmap_representer(dumper: yaml.SafeDumper, lookup_configmap: LookupConfigMap):
    return dumper.represent_str("!configmap", lookup_configmap.name)

def lookup_secret_representer(dumper: yaml.SafeDumper, lookup_secret: LookupSecret):
    return dumper.represent_str("!secret", lookup_secret.name)


def lookup_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode):
    return Lookup(spec=loader.construct_mapping(node))

def lookup_name_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode):
    return LookupName(spec=loader.construct_mapping(node))

def lookup_hash_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode):
    return LookupHash(spec=loader.construct_mapping(node))

def lookup_configmap_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode):
    return LookupConfigMap(name=loader.construct_yaml_str(node))

def lookup_secret_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode):
    return LookupSecret(name=loader.construct_yaml_str(node))


#
# String representer
#
# Define a representer for pyyaml to format multiline strings as block quotes
def str_representer(dumper, data):
    if isinstance(data, str) and '\n' in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

#
# yaml representers
representers = [
    (str, str_representer),
    (Lookup, lookup_representer),
    (LookupName, lookup_name_representer),
    (LookupHash, lookup_hash_representer),
    (LookupConfigMap, lookup_configmap_representer),
    (LookupSecret, lookup_secret_representer)
]

for type_ref, representer in representers:
    yaml.SafeDumper.add_representer(type_ref, representer)
    yaml.Dumper.add_representer(type_ref, representer)

#
# yaml constructors
constructors = [
    ("!lookup", lookup_constructor),
    ("!name", lookup_name_constructor),
    ("!hash", lookup_hash_constructor),
    ("!configmap", lookup_configmap_constructor),
    ("!secret", lookup_secret_constructor)
]

for type_ref, constructor in constructors:
    yaml.SafeLoader.add_constructor(type_ref, constructor)
    yaml.Loader.add_constructor(type_ref, constructor)
