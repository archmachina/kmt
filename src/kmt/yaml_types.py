import re
import yaml

import kmt.core as core
import kmt.exception as exception
import kmt.util as util

#
# Manifest name lookups
class Lookup:
    def __init__(self, spec):
        util.validate(isinstance(spec, dict), "Invalid specification passed to Lookup")

        self.spec = spec.copy()

        allowed_keys = [
            "group",
            "version",
            "kind",
            "api_version",
            "namespace",
            "pattern"
        ]

        errored = []
        for key in self.spec.keys():
            if key not in allowed_keys or not isinstance(self.spec[key], str):
                errored.append(key)

        if len(errored) > 0:
            raise exception.PipelineRunException(f"Invalid keys or invalid key value found on lookup: {errored}")

    def find_matches(self, manifests, *, current_namespace=None):
        return self._find(manifests, multiple=True, current_namespace=current_namespace)

    def find_match(self, manifests, *, current_namespace=None):
        return self._find(manifests, multiple=False, current_namespace=current_namespace)

    def _find(self, manifests, *, multiple, current_namespace=None):
        util.validate(isinstance(manifests, list) and all(isinstance(x, core.Manifest) for x in manifests),
            "Invalid manifests provided to Lookup find")

        matches = []

        for manifest in manifests:

            info = util.extract_manifest_info(manifest)

            if "group" in self.spec and self.spec["group"] != info["group"]:
                continue

            if "version" in self.spec and self.spec["version"] != info["version"]:
                continue

            if "kind" in self.spec and self.spec["kind"] != info["kind"]:
                continue

            if "api_version" in self.spec and self.spec["api_version"] != info["api_version"]:
                continue

            if "namespace" in self.spec:
                if self.spec["namespace"] != info["namespace"]:
                    continue
            elif info["namespace"] is not None and info["namespace"] != current_namespace:
                # If no namespace has been defined in the lookup, we will match on
                # the current namespace and any resource without a namespace.
                continue

            if "pattern" in self.spec and not re.search(self.spec["pattern"], info["name"]):
                continue

            matches.append(manifest)

        if multiple:
            return matches

        if len(matches) == 0:
            raise exception.PipelineRunException("Could not find a matching object for Lookup find")

        if len(matches) > 1:
            raise exception.PipelineRunException("Could not find a single object for Lookup find. Multiple object matches")

        return matches[0]


def lookup_representer(dumper: yaml.SafeDumper, lookup: Lookup):
    return dumper.represent_mapping("!lookup", lookup.spec)

def lookup_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode):
    return Lookup(spec=loader.construct_mapping(node))

yaml.SafeDumper.add_representer(Lookup, lookup_representer)
yaml.Dumper.add_representer(Lookup, lookup_representer)

yaml.SafeLoader.add_constructor("!lookup", lookup_constructor)
yaml.Loader.add_constructor("!lookup", lookup_constructor)

#
# String representer
#
# Define a representer for pyyaml to format multiline strings as block quotes
def str_representer(dumper, data):
    if isinstance(data, str) and '\n' in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

yaml.SafeDumper.add_representer(str, str_representer)
yaml.Dumper.add_representer(str, str_representer)
