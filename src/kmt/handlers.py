import logging
import copy
import yaml
import re
import jsonpatch
import os
import glob
import sys

from .util import validate
from . import types
from .exception import PipelineRunException

logger = logging.getLogger(__name__)

class HandlerConfig(types.Handler):
    """
    """
    def extract(self, step_def):
        # Read the content from the file and use _process_config_content to do the work
        self.config_file = self.state.spec_util.extract_property(step_def, "file")
        validate(isinstance(self.config_file, str) or self.config_file is None, "Step 'config_file' must be a string or absent")
        validate(not isinstance(self.config_file, str) or self.config_file != "", "Step 'config_file' cannot be empty")

        # Extract the content var, which can be either a dict or yaml string
        self.config_content = self.state.spec_util.extract_property(step_def, "content")
        validate(isinstance(self.config_content, (str, dict)) or self.config_content is None, "Step 'config_content' must be a string, dict or absent")

        # Extract stdin bool, indicating whether to read config from stdin
        self.stdin = self.state.spec_util.extract_property(step_def, "stdin", types=bool, default=False)
        validate(isinstance(self.stdin, bool), "Step 'stdin' must be a bool, bool like string or absent")

    def run(self):

        # Working dir - If the configuration source is a file, the working directory should be set
        # to the dirname for the config file. Otherwise, the working dir should be inherited from the calling
        # step, which is referenced by self.state.workingdir
        # However, if a step has defined the workingdir, it shouldn't be overridden

        if self.config_file is not None:
            logger.debug(f"config: including config from file {self.config_file}")
            with open(self.config_file, "r", encoding='utf-8') as file:
                content = file.read()

            # The working dir for the steps is set to the dirname for the config file
            dirname = os.path.dirname(self.config_file)

            self._process_config_content(content, dirname)

        # Call _process_config_content, which can determine whether to process as string or dict
        if self.config_content is not None:
            logger.debug(f"config: including inline config")
            self._process_config_content(self.config_content, self.state.workingdir)

        if self.stdin:
            # Read configuration from stdin
            logger.debug(f"config: including stdin config")
            stdin_content = sys.stdin.read()
            self._process_config_content(stdin_content, self.state.workingdir)

    def _process_config_content(self, content, workingdir):
        validate(isinstance(content, (str, dict)), "Included configuration must be a string or dictionary")
        validate(isinstance(workingdir, str), "Invalid workingdir passed to _process_config_content")

        # Don't error on an empty configuration. Just return
        if content == "":
            logger.debug("config: empty configuration. Ignoring.")
            return

        # Parse yaml if it is a string
        if isinstance(content, str):
            content = yaml.safe_load(content)

        validate(isinstance(content, dict), "Parsed configuration is not a dictionary")

        # Extract vars from the config
        # Don't template the vars - These will be templated when processed in a step
        config_vars = self.state.spec_util.extract_property(content, "vars", default={}, template=False)
        validate(isinstance(config_vars, dict), "Config 'vars' is not a dictionary")

        for config_var_name in config_vars:
            self.state.pipeline.set_var(config_var_name, config_vars[config_var_name])

        # Extract pipeline steps from the config
        # Don't template the pipeline steps - These will be templated when they are executed
        config_pipeline = self.state.spec_util.extract_property(content, "pipeline", default=[], template=False)
        validate(isinstance(config_pipeline, list), "Config 'pipeline' is not a list")

        for step in config_pipeline:
            validate(isinstance(step, dict), "Pipeline entry is not a dictionary")

            # Only define the working directory if the step hasn't already defined it
            if 'workingdir' not in step:
                step["workingdir"] = workingdir

            self.state.pipeline.add_step(step)

        # Validate config has no other properties
        validate(len(content.keys()) == 0, f"Found unknown properties in configuration: {content.keys()}")

class HandlerImport(types.Handler):
    """
    """
    def extract(self, step_def):
        self.import_files = self.state.spec_util.extract_property(step_def, "files", types=list)
        validate(isinstance(self.import_files, list), "Step 'files' must be a list of strings")
        validate(all(isinstance(x, str) for x in self.import_files), "Step 'files' must be a list of strings")

        self.recursive = self.state.spec_util.extract_property(step_def, "recursive", types=bool)
        validate(isinstance(self.recursive, bool), "Step 'recursive' must be a bool or bool like string")

    def run(self):
        filenames = set()
        for import_file in self.import_files:
            logger.debug(f"import: processing file glob: {import_file}")
            matches = glob.glob(import_file, recursive=self.recursive)
            for match in matches:
                filenames.add(match)

        # Ensure consistency for load order
        filenames = list(filenames)
        filenames.sort()

        new_blocks = []
        for filename in filenames:
            logger.debug(f"import: reading file {filename}")
            with open(filename, "r", encoding="utf-8") as file:
                content = file.read()
                new_block = types.TextBlock(content)
                new_block.vars["import_filename"] = filename
                self.state.pipeline.add_block(new_block)
                new_blocks.append(new_block)

        return new_blocks

class HandlerMeta(types.Handler):
    """
    """
    def extract(self, step_def):
        self.vars = self.state.spec_util.extract_property(step_def, "vars", types=dict)
        validate(isinstance(self.vars, dict), "Step 'vars' must be a dictionary of strings")
        validate(all(isinstance(x, str) for x in self.vars), "Step 'vars' must be a dictionary of strings")

    def run(self):
        for key in self.vars:
            block.vars[key] = self.vars[key]

class HandlerReplace(types.Handler):
    """
    """
    def extract(self, step_def):
        self.replace = self.state.spec_util.extract_property(step_def, "replace", types=list, default={})
        validate(isinstance(self.replace, list), "Step 'replace' must be a list")
        validate(all(isinstance(x, dict) for x in self.replace), "Step 'replace' items must be dictionaries")
        for item in self.replace:
            validate('key' in item and isinstance(item['key'], str), "Step 'replace' items must contain a string 'key' property")
            validate('value' in item and isinstance(item['value'], str), "Step 'replace' items must contain a string 'value' property")

        self.regex = self.state.spec_util.extract_property(step_def, "regex", types=bool, default=False)
        validate(isinstance(self.regex, bool), "Step 'regex' must be a bool, bool like string or absent")

    def run(self):
        for replace_item in self.replace:
            # Copy the dictionary as we'll change it when removing values
            replace_item = replace_item.copy()

            replace_key = replace_item['key']
            replace_value = replace_item['value']

            replace_regex = self.state.spec_util.extract_property(replace_item, "regex", types=bool, default=False)
            validate(isinstance(replace_regex, bool), "Replace item 'regex' must be a bool, bool like string or absent")

            logger.debug(f"replace: replacing regex({self.regex or replace_regex}): {replace_key} -> {replace_value}")

            if self.regex or replace_regex:
                block.text = re.sub(replace_key, replace_value, block.text)
            else:
                block.text = block.text.replace(replace_key, replace_value)

class HandlerSplitYaml(types.Handler):
    """
    """
    def extract(self, step_def):
        self.strip = self.state.spec_util.extract_property(step_def, "strip", types=bool, default=False)
        validate(isinstance(self.strip, bool), "Step 'strip' must be a bool or str value")

    def run(self):
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
        if self.strip:
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

            self.state.pipeline.add_block(new_block)

        # Remove the original source block from the list
        self.state.pipeline.remove_block(block)

        logger.debug(f"split_yaml: output 1 document -> {len(documents)} documents")

        return new_blocks

class HandlerStdin(types.Handler):
    """
    """
    def extract(self, step_def):
        self.split = self.state.spec_util.extract_property(step_def, "split")
        validate(isinstance(self.split, str) or self.split is None, "Step 'split' must be a string")

        self.strip = self.state.spec_util.extract_property(step_def, "strip", types=bool, default=False)
        validate(isinstance(self.strip, bool), "Step 'strip' must be a bool or str value")

    def run(self):
        # Read content from stdin
        logger.debug("stdin: reading document from stdin")
        stdin_content = sys.stdin.read()

        # Split if required and convert to a list of documents
        if self.split is not None and self.split != "":
            stdin_items = stdin_content.split(self.split)
        else:
            stdin_items = [stdin_content]

        # strip leading and trailing whitespace, if required
        if self.strip:
            stdin_items = [x.strip() for x in stdin_items]

        # Add the stdin items to the list of text blocks
        new_blocks = [types.TextBlock(item) for item in stdin_items]
        for item in new_blocks:
            self.state.pipeline.add_block(item)

        return new_blocks

class HandlerStdout(types.Handler):
    """
    """
    def extract(self, step_def):
        self.prefix = self.state.spec_util.extract_property(step_def, "prefix")
        validate(isinstance(self.prefix, str) or self.prefix is None, "Step 'prefix' must be a string")

        self.suffix = self.state.spec_util.extract_property(step_def, "suffix")
        validate(isinstance(self.suffix, str) or self.suffix is None, "Step 'suffix' must be a string")

    def run(self):
        if self.prefix is not None:
            print(self.prefix)

        print(block.text)

        if self.suffix is not None:
            print(self.suffix)

class HandlerTemplate(types.Handler):
    """
    """
    def extract(self, step_def):
        self.vars = self.state.spec_util.extract_property(step_def, "vars", types=dict)
        validate(isinstance(self.vars, dict) or self.vars is None, "Step 'vars' must be a dictionary or absent")

    def run(self):
        template_vars = self.state.vars.copy()

        if self.vars is not None:
            for key in self.vars:
                template_vars[key] = self.vars[key]

        block.text = self.state.spec_util.template_if_string(block.text)
        if not isinstance(block.text, str):
            raise PipelineRunException("Could not template source text")

class HandlerSum(types.Handler):
    """
    """
    def extract(self, step_def):
        pass

    def run(self):
        _block_sum(block)

        logger.debug(f"sum: document short sum: {block.vars['shortsum']}")

class HandlerJsonPatch(types.Handler):
    def extract(self, step_def):
        self.patches = self.state.spec_util.extract_property(step_def, "patches", types=list)
        validate(isinstance(self.patches, list), "Invalid patch list supplied")
        validate(all(isinstance(x, dict) for x in self.patches), "Invalid patch list supplied")

    def run(self):
        if block is None:
            return

        # The text blocks must be valid yaml or this handler will (and should) fail
        manifest = yaml.safe_load(block.text)
        if manifest is None:
            # Empty yaml document. Just return
            return

        # Make sure we're working with a dictionary
        validate(isinstance(manifest, dict), f"Parsed yaml must be a dictionary: {type(manifest)}")

        # Apply the patches to the manifest object
        patch_list = jsonpatch.JsonPatch(self.patches)
        manifest = patch_list.apply(manifest)

        # Save the yaml format back to the block
        block.text = yaml.dump(manifest, explicit_start=True)

class HandlerMetadata(types.Handler):
    def extract(self, step_def):
        self.name = self.state.spec_util.extract_property(step_def, "name")
        validate(isinstance(self.name, str) or self.name is None, "Name is not a string")

        self.namespace = self.state.spec_util.extract_property(step_def, "namespace")
        validate(isinstance(self.namespace, str) or self.namespace is None, "Namespace is not a string")

        self.annotations = self.state.spec_util.extract_property(step_def, "annotations", types=dict)
        validate(isinstance(self.annotations, dict) or self.annotations is None, "Annotations is not a dictionary")

        self.labels = self.state.spec_util.extract_property(step_def, "labels", types=dict)
        validate(isinstance(self.labels, dict) or self.labels is None, "Labels is not a dictionary")

    def run(self):
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
