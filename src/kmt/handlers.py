import logging
import copy
import yaml
import re
import jsonpatch
import glob
import sys

from . import types
from . import util
from . import util

from .exception import PipelineRunException

logger = logging.getLogger(__name__)

class HandlerPipeline(types.Handler):
    """
    """
    def extract(self, step_def):
        # Path to other pipeline
        self.path = self.state.spec_util.extract_property(step_def, "path")

        self.vars = self.state.spec_util.extract_property(step_def, "vars", default={})

        self.pass_blocks = self.state.spec_util.extract_property(step_def, "pass_blocks", default=False)

    def run(self):
        spec_util = self.state.spec_util

        # Determine whether we pass blocks to the new pipeline
        # Filtering is done via normal support handlers e.g. when, tags, etc.
        pass_blocks = spec_util.resolve(self.pass_blocks, bool)

        # Path to the other pipeline
        path = spec_util.resolve(self.path, str)

        # Vars to pass to the new pipeline
        # Do a recursive template/resolve all references before passing it to
        # the new pipeline
        pipeline_vars = spec_util.resolve(self.vars, dict, recursive=True)

        pipeline_blocks = []
        if pass_blocks:
            # If we're passing blocks to the new pipeline, then the working_blocks
            # list needs to be cleared and the passed blocks removed from the current pipeline
            # block list
            for block in self.state.working_blocks:
                self.state.pipeline.blocks.remove(block)

            pipeline_blocks = self.state.working_blocks
            self.state.working_blocks = []

            # The working blocks are no longer in the pipeline blocks and working_blocks is empty.
            # pipeline_blocks holds the only reference to these blocks now

        # Create the new pipeline and run
        pipeline = types.Pipeline(path, common=self.state.pipeline.common,
                        pipeline_vars=pipeline_vars, blocks=pipeline_blocks)

        pipeline_blocks = pipeline.run()

        # The blocks returned from the pipeline will be added to the working blocks
        # If pass_blocks is true, then working_blocks would be empty, but if not, then
        # there are still working blocks to be preserved, so append the blocks
        # They also need to be entered in to the pipeline block list
        for block in pipeline_blocks:
            self.state.working_blocks.append(block)
            self.state.pipeline.blocks.append(block)

class HandlerImport(types.Handler):
    """
    """
    def extract(self, step_def):
        self.import_files = self.state.spec_util.extract_property(step_def, "files")

        self.recursive = self.state.spec_util.extract_property(step_def, "recursive", default=False)

    def run(self):
        spec_util = self.state.spec_util

        filenames = set()

        import_files = spec_util.resolve(self.import_files, list)
        import_files = [spec_util.resolve(x, str) for x in import_files]

        recursive = spec_util.resolve(self.recursive, bool)

        for import_file in import_files:
            logger.debug(f"import: processing file glob: {import_file}")
            matches = glob.glob(import_file, recursive=recursive)
            for match in matches:
                filenames.add(match)

        # Ensure consistency for load order
        filenames = list(filenames)
        filenames.sort()

        for filename in filenames:
            logger.debug(f"import: reading file {filename}")
            with open(filename, "r", encoding="utf-8") as file:
                content = file.read()
                new_block = types.TextBlock(content)
                new_block.vars["import_filename"] = filename

                self.state.pipeline.blocks.append(new_block)
                self.state.working_blocks.append(new_block)

class HandlerVars(types.Handler):
    """
    """
    def extract(self, step_def):
        self.pipeline_vars = self.state.spec_util.extract_property(step_def, "pipeline", default={})

        self.block_vars = self.state.spec_util.extract_property(step_def, "block", default={})

    def run(self):
        working_blocks = self.state.working_blocks.copy()
        spec_util = self.state.spec_util

        pipeline_vars = spec_util.resolve(self.pipeline_vars, (dict, type(None)))
        if pipeline_vars is not None:
            for key in pipeline_vars:
                self.state.pipeline.vars[key] = spec_util.template_if_string(pipeline_vars[key])

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

            block_vars = spec_util.resolve(self.block_vars, (dict, type(None)))
            if block_vars is not None:
                for key in block_vars:
                    block.vars[key] = spec_util.template_if_string(block_vars[key])

class HandlerReplace(types.Handler):
    """
    """
    def extract(self, step_def):
        self.items = self.state.spec_util.extract_property(step_def, "items")

        self.regex = self.state.spec_util.extract_property(step_def, "regex", default=False)

    def run(self):
        working_blocks = self.state.working_blocks.copy()
        spec_util = self.state.spec_util

        regex = spec_util.resolve(self.regex, bool)

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

            for replace_item in spec_util.resolve(self.items, list):
                # Copy the dictionary as we'll change it when removing values
                replace_item = replace_item.copy()

                replace_key = spec_util.extract_property(replace_item, "key")
                replace_key = spec_util.resolve(replace_key, str)

                replace_value = spec_util.extract_property(replace_item, "value")
                replace_value = spec_util.resolve(replace_value, str)

                replace_regex = spec_util.extract_property(replace_item, "regex", default=False)
                replace_regex = spec_util.resolve(replace_regex, bool)

                logger.debug(f"replace: replacing regex({regex or replace_regex}): {replace_key} -> {replace_value}")

                if regex or replace_regex:
                    block.text = re.sub(replace_key, replace_value, block.text)
                else:
                    block.text = block.text.replace(replace_key, replace_value)

class HandlerSplitYaml(types.Handler):
    """
    """
    def extract(self, step_def):
        self.strip = self.state.spec_util.extract_property(step_def, "strip", default=False)

    def run(self):
        working_blocks = self.state.working_blocks.copy()

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

            # Strip each document, if required
            if spec_util.resolve(self.strip, bool):
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

                self.state.pipeline.blocks.append(new_block)
                self.state.working_blocks.append(new_block)

            # Remove the original source block from the list
            self.state.pipeline.blocks.remove(block)
            self.state.working_blocks.remove(block)

            logger.debug(f"split_yaml: output 1 document -> {len(documents)} documents")

class HandlerStdin(types.Handler):
    """
    """
    def extract(self, step_def):
        self.split = self.state.spec_util.extract_property(step_def, "split")

        self.strip = self.state.spec_util.extract_property(step_def, "strip", default=False)

    def run(self):
        spec_util = self.state.spec_util

        # Read content from stdin
        logger.debug("stdin: reading document from stdin")
        stdin_content = sys.stdin.read()

        # Split if required and convert to a list of documents
        split = spec_util.resolve(self.split, (str, type(None)))
        if split is not None and split != "":
            stdin_items = stdin_content.split(split)
        else:
            stdin_items = [stdin_content]

        # strip leading and trailing whitespace, if required
        if spec_util.resolve(self.strip, bool):
            stdin_items = [x.strip() for x in stdin_items]

        # Add the stdin items to the list of text blocks
        new_blocks = [types.TextBlock(item) for item in stdin_items]
        for item in new_blocks:
            self.state.pipeline.blocks.append(item)
            self.state.working_blocks.append(item)

class HandlerStdout(types.Handler):
    """
    """
    def extract(self, step_def):
        self.prefix = self.state.spec_util.extract_property(step_def, "prefix")

        self.suffix = self.state.spec_util.extract_property(step_def, "suffix")

    def run(self):
        working_blocks = self.state.working_blocks.copy()

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

            prefix = spec_util.resolve(self.prefix, (str, type(None)))
            if prefix is not None:
                print(prefix)

            print(block.text)

            suffix = spec_util.resolve(self.suffix, (str, type(None)))
            if suffix is not None:
                print(suffix)

class HandlerTemplate(types.Handler):
    """
    """
    def extract(self, step_def):
        self.vars = self.state.spec_util.extract_property(step_def, "vars", default={})

    def run(self):
        working_blocks = self.state.working_blocks.copy()

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)

            # Merge in any vars specified on the step after the block vars
            for key in self.vars:
                block_vars[key] = self.vars[key]

            spec_util = self.state.spec_util.new_scope(block_vars)

            block.text = spec_util.template_if_string(block.text)
            if not isinstance(block.text, str):
                raise PipelineRunException("Could not template source text")

class HandlerSum(types.Handler):
    """
    """
    def extract(self, step_def):
        pass

    def run(self):
        for block in self.state.working_blocks:
            util.block_sum(block)

            logger.debug(f"sum: document short sum: {block.vars['shortsum']}")

class HandlerJsonPatch(types.Handler):
    def extract(self, step_def):
        self.patches = self.state.spec_util.extract_property(step_def, "patches")

    def run(self):
        working_blocks = self.state.working_blocks.copy()

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

            # The text blocks must be valid yaml or this handler will (and should) fail
            manifest = yaml.safe_load(block.text)
            if manifest is None:
                # Document is empty. We'll just create an empty dictionary and continue
                manifest = {}

            # Make sure we're working with a dictionary
            util.validate(isinstance(manifest, dict), f"Parsed yaml must be a dictionary: {type(manifest)}")

            # Apply the patches to the manifest object
            patches = spec_util.resolve(self.patches, list)
            patches = [spec_util.resolve(x, dict) for x in patches]
            patch_list = jsonpatch.JsonPatch(patches)
            manifest = patch_list.apply(manifest)

            # Save the yaml format back to the block
            block.text = yaml.dump(manifest, explicit_start=True)

class HandlerDelete(types.Handler):
    def extract(self, step_def):
        pass

    def run(self):
        working_blocks = self.state.working_blocks.copy()

        # Remove all of the remaining working blocks from the working list
        # and pipeline
        for block in working_blocks:
            self.state.working_blocks.remove(block)
            self.state.pipeline.blocks.remove(block)

class HandlerMetadata(types.Handler):
    def extract(self, step_def):
        self.name = self.state.spec_util.extract_property(step_def, "name")

        self.namespace = self.state.spec_util.extract_property(step_def, "namespace")

        self.annotations = self.state.spec_util.extract_property(step_def, "annotations")

        self.labels = self.state.spec_util.extract_property(step_def, "labels")

    def run(self):
        working_blocks = self.state.working_blocks.copy()

        for block in working_blocks:
            block_vars = block.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(block_vars)

            # The text blocks must be valid yaml or this handler will (and should) fail
            manifest = yaml.safe_load(block.text)
            if manifest is None:
                # Document is empty. We'll just create an empty dictionary and continue
                manifest = {}

            # Make sure we're working with a dictionary
            util.validate(isinstance(manifest, dict), f"Parsed yaml must be a dictionary: {type(manifest)}")

            if manifest.get("metadata") is None:
                manifest["metadata"] = {}

            name = spec_util.resolve(self.name, (str, type(None)))
            if name is not None:
                manifest["metadata"]["name"] = name

            namespace = spec_util.resolve(self.namespace, (str, type(None)))
            if namespace is not None:
                manifest["metadata"]["namespace"] = namespace

            annotations = spec_util.resolve(self.annotations, (dict, type(None)))
            if annotations is not None:
                if manifest["metadata"].get("annotations") is None:
                    manifest["metadata"]["annotations"] = {}

                for key in annotations:
                    manifest["metadata"]["annotations"][key] = spec_util.resolve(annotations[key], str)

            labels = spec_util.resolve(self.labels, (dict, type(None)))
            if labels is not None:
                if manifest["metadata"].get("labels") is None:
                    manifest["metadata"]["labels"] = {}

                for key in labels:
                    manifest["metadata"]["labels"][key] = spec_util.resolve(labels[key], str)

            block.text = yaml.dump(manifest, explicit_start=True)

types.default_handlers["pipeline"] = HandlerPipeline
types.default_handlers["import"] = HandlerImport
types.default_handlers["vars"] = HandlerVars
types.default_handlers["replace"] = HandlerReplace
types.default_handlers["splityaml"] = HandlerSplitYaml
types.default_handlers["stdin"] = HandlerStdin
types.default_handlers["stdout"] = HandlerStdout
types.default_handlers["template"] = HandlerTemplate
types.default_handlers["sum"] = HandlerSum
types.default_handlers["jsonpatch"] = HandlerJsonPatch
types.default_handlers["metadata"] = HandlerMetadata
types.default_handlers["delete"] = HandlerDelete
