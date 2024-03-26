import ttast
import logging
import copy
import yaml
import re

from ttast.util import validate

logger = logging.getLogger(__name__)

def str_representer(dumper, data):
    if isinstance(data, str) and '\n' in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

yaml.add_representer(str, str_representer)

class HandlerMetadata(ttast.Handler):
    def parse(self):
        self.name = self.state.templater.extract_property(self.state.step_def, "name")
        validate(isinstance(self.name, str) or self.name is None, "Name is not a string")

        self.namespace = self.state.templater.extract_property(self.state.step_def, "namespace")
        validate(isinstance(self.namespace, str) or self.namespace is None, "Namespace is not a string")

        self.annotations = self.state.templater.extract_property(self.state.step_def, "annotations")
        validate(isinstance(self.annotations, dict) or self.annotations is None, "Annotations is not a dictionary")

        self.labels = self.state.templater.extract_property(self.state.step_def, "labels")
        validate(isinstance(self.labels, dict) or self.labels is None, "Labels is not a dictionary")

    def is_per_block():
        return True

    def run(self, block):
        if block is None:
            return

        # The text blocks must be valid yaml or this handler will (and should) fail
        manifest = yaml.safe_load(block.text)
        if manifest is None:
            # Empty yaml document. Just return
            return

        # Make sure we're working with a dictionary
        validate(isinstance(manifest, dict), f"Parsed yaml must be a dictionary: {type(manifest)}")

        if manifest.get("metadata") is None:
            manifest["metadata"] = {}

        if self.name is not None:
            manifest["metadata"]["name"] = self.name

        if self.namespace is not None:
            manifest["metadata"]["namespace"] = self.namespace

        if self.annotations is not None:
            if manifest["metadata"].get("annotations") is None:
                manifest["metadata"]["annotations"] = {}

            for key in self.annotations:
                manifest["metadata"]["annotations"][key] = self.annotations[key]

        if self.labels is not None:
            if manifest["metadata"].get("labels") is None:
                manifest["metadata"]["labels"] = {}

            for key in self.labels:
                manifest["metadata"]["labels"][key] = self.labels[key]

        block.text = yaml.dump(manifest, explicit_start=True)

class SupportHandlerK8sMetadata(ttast.SupportHandler):
    def parse(self):
        self.match_group = self.state.templater.extract_property(self.state.step_def, "match_group")
        validate(isinstance(self.match_group, str) or self.match_group is None, "Invalid match_group value")

        self.match_version = self.state.templater.extract_property(self.state.step_def, "match_version")
        validate(isinstance(self.match_version, str) or self.match_version is None, "Invalid match_version value")

        self.match_kind = self.state.templater.extract_property(self.state.step_def, "match_kind")
        validate(isinstance(self.match_kind, str) or self.match_kind is None, "Invalid match_kind value")

        self.match_namespace = self.state.templater.extract_property(self.state.step_def, "match_namespace")
        validate(isinstance(self.match_namespace, str) or self.match_namespace is None, "Invalid match_namespace value")

        self.match_name = self.state.templater.extract_property(self.state.step_def, "match_name")
        validate(isinstance(self.match_name, str) or self.match_name is None, "Invalid match_name value")

    def pre(self, block):
        if block is None:
            return

        # Best effort extract of Group, Version, Kind, Name from the object, if
        # it is yaml

        manifest = None
        try:
            manifest = yaml.safe_load(block.text)
            if not isinstance(manifest, dict):
                logger.debug(f"ExtractMetadata: Parsed yaml is not a dictionary")
                manifest = None
        except yaml.YAMLError as exc:
            logger.debug(f"ExtractMetadata: Could not parse input object: {exc}")

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

        block.meta["k8s_group"] = group
        block.meta["k8s_version"] = version
        block.meta["k8s_kind"] = kind
        block.meta["k8s_namespace"] = namespace
        block.meta["k8s_name"] = name
        block.meta["k8s_api_version"] = api_version
        block.meta["k8s_manifest"] = manifest

        # k8s group match
        if self.match_group is not None and not re.search(self.match_group, block.meta["k8s_group"]):
            return []

        # k8s version match
        if self.match_version is not None and not re.search(self.match_version, block.meta["k8s_version"]):
            return []

        # k8s kind match
        if self.match_kind is not None and not re.search(self.match_kind, block.meta["k8s_kind"]):
            return []

        # k8s namespace match
        if self.match_namespace is not None and not re.search(self.match_namespace, block.meta["k8s_namespace"]):
            return []

        # k8s name match
        if self.match_name is not None and not re.search(self.match_name, block.meta["k8s_name"]):
            return []

    def post(self, block):
        pass

class SupportHandlerSplitYaml(ttast.SupportHandler):
    def parse(self):
        pass

    def pre(self, block):
        pass

    def post(self, block):
        if block is None:
            return

        lines = block.text.splitlines()
        documents = []
        current = []

        for line in lines:

            # Determine if we have the beginning of a yaml document
            if line == "---" and len(current) > 0:
                documents.append("\n".join(current))
                current = []

            current.append(line)

        documents.append("\n".join(current))

        # Strip each document
        documents = [x.strip() for x in documents]

        # If we have a single document and it's the same as the
        # original block, just exit
        if len(documents) == 1 and documents[0] == block.text:
            return

        # Add all documents to the pipeline text block list
        new_blocks = [ttast.TextBlock(item) for item in documents]
        for new_block in new_blocks:
            new_block.meta = copy.deepcopy(block.meta)
            new_block.tags = copy.deepcopy(block.tags)

            self.state.pipeline.add_block(new_block)

        # Remove the original source block from the list
        self.state.pipeline.remove_block(block)

        logger.debug(f"split_yaml: output 1 document -> {len(documents)} documents")

        return new_blocks
