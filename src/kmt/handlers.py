import ttast
import logging
import copy
import yaml

logger = logging.getLogger(__name__)

class HandlerMetadata(ttast.Handler):
    def parse(self):
        pass

    def is_per_block(self):
        return True

    def run(self, block):
        pass

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
