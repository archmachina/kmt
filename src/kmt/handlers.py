import ttast
import logging
import copy
import yaml

from .util import *

logger = logging.getLogger(__name__)

class HandlerMetadata(ttast.Handler):
    def parse(self):
        self.name = pop_property(self.state.step_def, "name", template_map=self.state.vars)
        validate(isinstance(self.name, str) or self.name is None, "Name is not a string")

        self.namespace = pop_property(self.state.step_def, "namespace", template_map=self.state.vars)
        validate(isinstance(self.namespace, str) or self.namespace is None, "Namespace is not a string")

        self.annotations = pop_property(self.state.step_def, "annotations", template_map=self.state.vars)
        validate(isinstance(self.annotations, dict) or self.annotations is None, "Annotations is not a dictionary")

        self.labels = pop_property(self.state.step_def, "labels", template_map=self.state.vars)
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

        if "metadata" not in manifest:
            manifest["metadata"] = {}

        if self.name is not None:
            manifest["metadata"]["name"] = self.name

        if self.namespace is not None:
            manifest["metadata"]["namespace"] = self.namespace

        if self.annotations is not None:
            if "annotations" not in manifest["metadata"]:
                manifest["metadata"]["annotations"] = {}

            for key in self.annotations:
                manifest["metadata"]["annotations"][key] = self.annotations[key]

        if self.labels is not None:
            if "labels" not in manifest["metadata"]:
                manifest["metadata"]["labels"] = {}

            for key in self.labels:
                manifest["metadata"]["labels"][key] = self.labels[key]

        block.text = yaml.dump(manifest)

class SupportHandlerExtractGVKN(ttast.SupportHandler):
    def parse(self):
        pass

    def pre(self, block):
        if block is None:
            return
        
        # Best effort extract of Group, Version, Kind, Name from the object, if
        # it is yaml

        try:
            manifest = yaml.safe_load(block.text)
        except yaml.YAMLError as exc:
            logger.debug(f"ExtractGVKN: Could not parse input object: {exc}")
            return

        # Make sure we're working with a dictionary
        if not isinstance(manifest, dict):
            logger.debug(f"ExtractGVKN: Parsed yaml is not a dictionary")
            return

        api_version = ""
        group = ""
        version = ""
        name = ""
        kind = ""

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
        kind = manifest.get("kind")

        # Name
        metadata = manifest.get("metadata")
        if isinstance(metadata, dict):
            name = metadata.get("name", "")

        block.meta["k8s_name"] = name
        block.meta["k8s_kind"] = kind
        block.meta["k8s_group"] = group
        block.meta["k8s_version"] = version
        block.meta["k8s_api_version"] = api_version


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
