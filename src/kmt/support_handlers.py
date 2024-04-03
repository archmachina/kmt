import logging
import copy
import yaml
import re


from .util import validate
from . import util
from . import types

logger = logging.getLogger(__name__)

class SupportHandlerSum(types.SupportHandler):
    """
    """
    def extract(self, step_def):
        pass

    def pre(self):
        pass

    def post(self):
        for block in self.state.working_blocks:
            util.block_sum(block)

        logger.debug(f"sum: document short sum: {block.vars['shortsum']}")

class SupportHandlerWhen(types.SupportHandler):
    def extract(self, step_def):
        # When condition
        self.when = self.state.spec_util.extract_property(step_def, "when", default=[])

    def pre(self):
        working_blocks = self.state.working_blocks

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

            when = spec_util.resolve(self.when, (list, str))
            if isinstance(when, str):
                when = [when]

            if len(when) > 0:
                for condition in when:
                    result = spec_util.resolve("{{" + condition + "}}", bool)
                    if not result:
                        self.state.working_blocks.remove(block)

    def post(self):
        pass

class SupportHandlerTags(types.SupportHandler):
    def extract(self, step_def):
        # Extract match any tags
        self.match_any_tags = self.state.spec_util.extract_property(step_def, "match_any_tags", default=[])

        # Extract match all tags
        self.match_all_tags = self.state.spec_util.extract_property(step_def, "match_all_tags", default=[])

        # Extract exclude tags
        self.exclude_tags = self.state.spec_util.extract_property(step_def, "exclude_tags", default=[])

        # Apply tags
        self.apply_tags = self.state.spec_util.extract_property(step_def, "apply_tags", default=[])

    def pre(self):
        working_blocks = self.state.working_blocks

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

            match_any_tags = spec_util.resolve(self.match_any_tags, list)
            match_any_tags = set([spec_util.resolve(x, str) for x in match_any_tags])
            if len(match_any_tags) > 0:
                # If there are any 'match_any_tags', then at least one of them has to match with the document
                if len(match_any_tags.intersection(block.tags)) == 0:
                    self.state.working_blocks.remove(block)
                    continue

            match_all_tags = spec_util.resolve(self.match_all_tags, list)
            match_all_tags = set([spec_util.resolve(x, str) for x in match_all_tags])
            if len(match_all_tags) > 0:
                # If there are any 'match_all_tags', then all of those tags must match the document
                for tag in match_all_tags:
                    if tag not in block.tags:
                        self.state.working_blocks.remove(block)
                        continue

            exclude_tags = spec_util.resolve(self.exclude_tags, list)
            exclude_tags = set([spec_util.resolve(x, str) for x in exclude_tags])
            if len(exclude_tags) > 0:
                # If there are any exclude tags and any are present in the block, it isn't a match
                for tag in exclude_tags:
                    if tag in block.tags:
                        self.state.working_blocks.remove(block)
                        continue

    def post(self):

        for block in self.state.working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

            apply_tags = spec_util.resolve(self.apply_tags, list)
            for tag in apply_tags:
                block.tags.add(spec_util.resolve(tag, str))

class SupportHandlerK8sMetadata(types.SupportHandler):
    def extract(self, step_def):
        self.match_group = self.state.spec_util.extract_property(step_def, "match_group")

        self.match_version = self.state.spec_util.extract_property(step_def, "match_version")

        self.match_kind = self.state.spec_util.extract_property(step_def, "match_kind")

        self.match_namespace = self.state.spec_util.extract_property(step_def, "match_namespace")

        self.match_name = self.state.spec_util.extract_property(step_def, "match_name")

    def pre(self):
        working_blocks = self.state.working_blocks

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

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

            block.vars["metadata_group"] = group
            block.vars["metadata_version"] = version
            block.vars["metadata_kind"] = kind
            block.vars["metadata_namespace"] = namespace
            block.vars["metadata_name"] = name
            block.vars["metadata_api_version"] = api_version
            block.vars["metadata_manifest"] = manifest

            # k8s group match
            match_group = spec_util.resolve(self.match_group, (str, type(None)))
            if match_group is not None and not re.search(match_group, group):
                self.state.working_blocks.remove(block)
                continue

            # k8s version match
            match_version = spec_util.resolve(self.match_version, (str, type(None)))
            if match_version is not None and not re.search(match_version, version):
                self.state.working_blocks.remove(block)
                continue

            # k8s kind match
            match_kind = spec_util.resolve(self.match_kind, (str, type(None)))
            if match_kind is not None and not re.search(match_kind, kind):
                self.state.working_blocks.remove(block)
                continue

            # k8s namespace match
            match_namespace = spec_util.resolve(self.match_namespace, (str, type(None)))
            if match_namespace is not None and not re.search(match_namespace, namespace):
                self.state.working_blocks.remove(block)
                continue

            # k8s name match
            match_name = spec_util.resolve(self.match_name, (str, type(None)))
            if match_name is not None and not re.search(match_name, name):
                self.state.working_blocks.remove(block)
                continue

    def post(self):
        pass

class SupportHandlerSplitYaml(types.SupportHandler):
    def extract(self, step_def):
        pass

    def pre(self):
        pass

    def post(self):
        working_blocks = self.state.working_blocks

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

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
                new_block.vars = copy.deepcopy(block.vars)
                new_block.tags = copy.deepcopy(block.tags)

                # Add to the working blocks for this step, so it can get picked up by following
                # handlers. Add to the pipeline blocks to be picked up in later steps in the
                # pipeline
                self.state.working_blocks.append(new_block)
                self.state.pipeline.blocks.append(new_block)

            # Remove the original source block from the working list and pipeline list
            self.state.working_blocks.remove(block)
            self.state.pipeline.blocks.remove(block)

            logger.debug(f"split_yaml: output 1 document -> {len(documents)} documents")

# types.default_support_handlers.append(SupportHandlerSum)
types.default_support_handlers.append(SupportHandlerSplitYaml)
types.default_support_handlers.append(SupportHandlerTags)
types.default_support_handlers.append(SupportHandlerWhen)
types.default_support_handlers.append(SupportHandlerK8sMetadata)
