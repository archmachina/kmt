import logging
import copy
import yaml
import re
import jsonpatch
import os
import glob
import sys
import hashlib
import base64
import textwrap

from .util import validate
from . import types
from .exception import PipelineRunException

logger = logging.getLogger(__name__)

class SupportHandlerSum(types.SupportHandler):
    """
    """
    def parse(self, step_def):
        pass

    def pre(self):
        pass

    def post(self):
        if block is None:
            return

        _block_sum(block)

        logger.debug(f"sum: document short sum: {block.meta['shortsum']}")

class SupportHandlerTags(types.SupportHandler):
    def parse(self, step_def):
        # Apply tags
        self.apply_tags = self.state.templater.extract_property(step_def, "apply_tags", types=list, default=[])
        validate(isinstance(self.apply_tags, list), "Step 'apply_tags' must be a list of strings")
        validate(all(isinstance(x, str) for x in self.apply_tags), "Step 'apply_tags' must be a list of strings")

    def pre(self):
        pass

    def post(self):
        if block is not None:
            for tag in self.apply_tags:
                block.tags.add(tag)

class SupportHandlerWhen(types.SupportHandler):
    def parse(self, step_def):
        # When condition
        self.when = self.state.templater.extract_property(step_def, "when", default=[])
        validate(isinstance(self.when, (list, str)), "Step 'when' must be a string or list of strings")
        if isinstance(self.when, str):
            self.when = [self.when]
        validate(all(isinstance(x, str) for x in self.when), "Step 'when' must be a string or list of strings")

    def pre(self):
        if len(self.when) > 0:
            for condition in self.when:
                result = self.state.templater.template_if_string("{{" + condition + "}}")
                if not parse_bool(result):
                    return []

    def post(self):
        pass

class SupportHandlerMatchTags(types.SupportHandler):
    def parse(self, step_def):
        # Extract match any tags
        self.match_any_tags = self.state.templater.extract_property(step_def, "match_any_tags", types=list, default=[])
        validate(isinstance(self.match_any_tags, list), "Step 'match_any_tags' must be a list of strings")
        validate(all(isinstance(x, str) for x in self.match_any_tags), "Step 'match_any_tags' must be a list of strings")
        self.match_any_tags = set(self.match_any_tags)

        # Extract match all tags
        match_all_tags = self.state.templater.extract_property(step_def, "match_all_tags", types=list, default=[])
        validate(isinstance(match_all_tags, list), "Step 'match_all_tags' must be a list of strings")
        validate(all(isinstance(x, str) for x in match_all_tags), "Step 'match_all_tags' must be a list of strings")
        self.match_all_tags = set(match_all_tags)

        # Extract exclude tags
        self.exclude_tags = self.state.templater.extract_property(step_def, "exclude_tags", types=list, default=[])
        validate(isinstance(self.exclude_tags, list), "Step 'exclude_tags' must be a list of strings")
        validate(all(isinstance(x, str) for x in self.exclude_tags), "Step 'exclude_tags' must be a list of strings")
        self.exclude_tags = set(self.exclude_tags)

    def pre(self):
        if len(self.match_any_tags) > 0:
            # If there are any 'match_any_tags', then at least one of them has to match with the document
            if len(self.match_any_tags.intersection(block.tags)) == 0:
                return []

        if len(self.match_all_tags) > 0:
            # If there are any 'match_all_tags', then all of those tags must match the document
            for tag in self.match_all_tags:
                if tag not in block.tags:
                    return []

        if len(self.exclude_tags) > 0:
            # If there are any exclude tags and any are present in the block, it isn't a match
            for tag in self.exclude_tags:
                if tag in block.tags:
                    return []

    def post(self):
        pass

class SupportHandlerK8sMetadata(types.SupportHandler):
    def parse(self, step_def):
        self.match_group = self.state.templater.extract_property(step_def, "match_group")
        validate(isinstance(self.match_group, str) or self.match_group is None, "Invalid match_group value")

        self.match_version = self.state.templater.extract_property(step_def, "match_version")
        validate(isinstance(self.match_version, str) or self.match_version is None, "Invalid match_version value")

        self.match_kind = self.state.templater.extract_property(step_def, "match_kind")
        validate(isinstance(self.match_kind, str) or self.match_kind is None, "Invalid match_kind value")

        self.match_namespace = self.state.templater.extract_property(step_def, "match_namespace")
        validate(isinstance(self.match_namespace, str) or self.match_namespace is None, "Invalid match_namespace value")

        self.match_name = self.state.templater.extract_property(step_def, "match_name")
        validate(isinstance(self.match_name, str) or self.match_name is None, "Invalid match_name value")

    def pre(self):
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

            # Save the parsed manifest back to the block to normalise the yaml format
            block.text = yaml.dump(manifest)

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

    def post(self):
        pass

class SupportHandlerSplitYaml(types.SupportHandler):
    def parse(self, step_def):
        pass

    def pre(self):
        pass

    def post(self):
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
        new_blocks = [types.TextBlock(item) for item in documents]
        for new_block in new_blocks:
            new_block.meta = copy.deepcopy(block.meta)
            new_block.tags = copy.deepcopy(block.tags)

            self.state.pipeline.add_block(new_block)

        # Remove the original source block from the list
        self.state.pipeline.remove_block(block)

        logger.debug(f"split_yaml: output 1 document -> {len(documents)} documents")

        return new_blocks
