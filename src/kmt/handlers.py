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

class StepHandlerPipeline(types.StepHandler):
    """
    """
    def extract(self, step_def):
        # Path to other pipeline
        self.path = self.state.spec_util.extract_property(step_def, "path")

        self.vars = self.state.spec_util.extract_property(step_def, "vars", default={})

        self.pass_manifests = self.state.spec_util.extract_property(step_def, "pass_manifests", default=False)

    def run(self):
        spec_util = self.state.spec_util

        # Determine whether we pass manifests to the new pipeline
        # Filtering is done via normal support handlers e.g. when, tags, etc.
        pass_manifests = spec_util.resolve(self.pass_manifests, bool)

        # Path to the other pipeline
        path = spec_util.resolve(self.path, str)

        # Vars to pass to the new pipeline
        # Do a recursive template/resolve all references before passing it to
        # the new pipeline
        pipeline_vars = spec_util.resolve(self.vars, dict, recursive=True)

        pipeline_manifests = []
        if pass_manifests:
            # If we're passing manifests to the new pipeline, then the working_manifests
            # list needs to be cleared and the passed manifests removed from the current pipeline
            # manifest list
            for manifest in self.state.working_manifests:
                self.state.pipeline.manifests.remove(manifest)

            pipeline_manifests = self.state.working_manifests
            self.state.working_manifests = []

            # The working manifests are no longer in the pipeline manifests and working_manifests is empty.
            # pipeline_manifests holds the only reference to these manifests now

        # Create the new pipeline and run
        pipeline = types.Pipeline(path, common=self.state.pipeline.common,
                        pipeline_vars=pipeline_vars, manifests=pipeline_manifests)

        pipeline_manifests = pipeline.run()

        # The manifests returned from the pipeline will be added to the working manifests
        # If pass_manifests is true, then working_manifests would be empty, but if not, then
        # there are still working manifests to be preserved, so append the manifests
        # They also need to be entered in to the pipeline manifest list
        for manifest in pipeline_manifests:
            self.state.working_manifests.append(manifest)
            self.state.pipeline.manifests.append(manifest)

class StepHandlerImport(types.StepHandler):
    """
    """
    def extract(self, step_def):
        self.import_files = self.state.spec_util.extract_property(step_def, "files")

        self.recursive = self.state.spec_util.extract_property(step_def, "recursive", default=False)

        self.template = self.state.spec_util.extract_property(step_def, "template", default=True)

    def run(self):
        spec_util = self.state.spec_util

        filenames = set()

        import_files = spec_util.resolve(self.import_files, list)
        import_files = [spec_util.resolve(x, str) for x in import_files]

        recursive = spec_util.resolve(self.recursive, bool)

        template = spec_util.resolve(self.template, bool)

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

            if template:
                content = spec_util.template_if_string(content)
                if not isinstance(content, str):
                    raise PipelineRunException("Could not template import text")

            # Load all documents from the file, after any templating
            docs = [x for x in yaml.safe_load_all(content)]

            for doc in docs:
                manifest = types.Manifest(doc)
                manifest.vars["import_filename"] = filename

                self.state.pipeline.manifests.append(manifest)
                self.state.working_manifests.append(manifest)

class StepHandlerVars(types.StepHandler):
    """
    """
    def extract(self, step_def):
        self.pipeline_var_list = self.state.spec_util.extract_property(step_def, "pipeline", default=[])

        self.manifest_var_list = self.state.spec_util.extract_property(step_def, "manifest", default=[])

    def run(self):
        working_manifests = self.state.working_manifests.copy()
        spec_util = self.state.spec_util

        pipeline_var_list = spec_util.resolve(self.pipeline_var_list, (list, type(None)))
        if pipeline_var_list is not None:
            pipeline_var_list = [spec_util.resolve(x, dict) for x in pipeline_var_list]
            # pipeline_vars should be a list of dictionaries now

            for var_spec in pipeline_var_list:
                # Extract vars
                var_spec = var_spec.copy()
                key = self.state.spec_util.extract_property(var_spec, "key")
                value = self.state.spec_util.extract_property(var_spec, "value")
                util.validate(len(var_spec.keys()) == 0, f"Unknown properties on vars spec: {var_spec.keys()}")

                # Resolve any templating and type
                key = spec_util.resolve(key, str)
                value = spec_util.resolve(value, str)

                self.state.pipeline.vars[key] = value
                logger.debug(f"Set pipeline var {key} -> {value}")

        for manifest in working_manifests:
            manifest_vars = manifest.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(manifest_vars)

            manifest_var_list = spec_util.resolve(self.manifest_var_list, (list, type(None)))

            if manifest_var_list is not None:
                manifest_var_list = [spec_util.resolve(x, dict) for x in manifest_var_list]
                # manifest_var_list should be a list of dictionaries now

                for var_spec in manifest_var_list:
                    # Extract vars
                    var_spec = var_spec.copy()
                    key = self.state.spec_util.extract_property(var_spec, "key")
                    value = self.state.spec_util.extract_property(var_spec, "value")
                    set_pipeline = self.state.spec_util.extract_property(var_spec, "pipeline", default=False)
                    util.validate(len(var_spec.keys()) == 0, f"Unknown properties on vars spec: {var_spec.keys()}")

                    # Resolve any templating and type
                    key = spec_util.resolve(key, str)
                    value = spec_util.resolve(value, str)
                    set_pipeline = spec_util.resolve(set_pipeline, bool)

                    if set_pipeline:
                        self.state.pipeline.vars[key] = value
                        logger.debug(f"Set pipeline var {key} -> {value}")
                    else:
                        manifest.vars[key] = value
                        logger.debug(f"Set manifest var {key} -> {value}")

# class StepHandlerReplace(types.StepHandler):
#     """
#     """
#     def extract(self, step_def):
#         self.items = self.state.spec_util.extract_property(step_def, "items")

#         self.regex = self.state.spec_util.extract_property(step_def, "regex", default=False)

#     def run(self):
#         working_manifests = self.state.working_manifests.copy()
#         spec_util = self.state.spec_util

#         regex = spec_util.resolve(self.regex, bool)

#         for manifest in working_manifests:
#             manifest_vars = manifest.create_scoped_vars(self.state.vars)
#             spec_util = self.state.spec_util.new_scope(manifest_vars)

#             for replace_item in spec_util.resolve(self.items, list):
#                 # Copy the dictionary as we'll change it when removing values
#                 replace_item = replace_item.copy()

#                 replace_key = spec_util.extract_property(replace_item, "key")
#                 replace_key = spec_util.resolve(replace_key, str)

#                 replace_value = spec_util.extract_property(replace_item, "value")
#                 replace_value = spec_util.resolve(replace_value, str)

#                 replace_regex = spec_util.extract_property(replace_item, "regex", default=False)
#                 replace_regex = spec_util.resolve(replace_regex, bool)

#                 logger.debug(f"replace: replacing regex({regex or replace_regex}): {replace_key} -> {replace_value}")

#                 if regex or replace_regex:
#                     manifest.text = re.sub(replace_key, replace_value, manifest.text)
#                 else:
#                     manifest.text = manifest.text.replace(replace_key, replace_value)

class StepHandlerStdin(types.StepHandler):
    """
    """
    def extract(self, step_def):

        self.template = self.state.spec_util.extract_property(step_def, "template", default=True)

    def run(self):
        spec_util = self.state.spec_util

        template = spec_util.resolve(self.template, bool)

        # Read content from stdin
        logger.debug("stdin: reading document from stdin")
        content = sys.stdin.read()

        if template:
            content = spec_util.template_if_string(content)
            if not isinstance(content, str):
                raise PipelineRunException("Could not template import text")

        # Load all documents from the file, after any templating
        docs = [x for x in yaml.safe_load_all(content)]

        for doc in docs:
            manifest = types.Manifest(doc)

            self.state.pipeline.manifests.append(manifest)
            self.state.working_manifests.append(manifest)

class StepHandlerRefreshHash(types.StepHandler):
    """
    """
    def extract(self, step_def):
        pass

    def run(self):
        for manifest in self.state.working_manifests:
            util.manifest_hash(manifest)

            logger.debug(f"RefreshHash: manifest short sum: {manifest.vars['kmt_shortsum']}")

class StepHandlerJsonPatch(types.StepHandler):
    def extract(self, step_def):
        self.patches = self.state.spec_util.extract_property(step_def, "patches")

    def run(self):
        working_manifests = self.state.working_manifests.copy()

        for manifest in working_manifests:
            manifest_vars = manifest.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(manifest_vars)

            # Apply the patches to the manifest object
            patches = spec_util.resolve(self.patches, list)
            patches = [spec_util.resolve(x, dict) for x in patches]
            patch_list = jsonpatch.JsonPatch(patches)
            manifest.spec = patch_list.apply(manifest.spec)

class StepHandlerDelete(types.StepHandler):
    def extract(self, step_def):
        pass

    def run(self):
        working_manifests = self.state.working_manifests.copy()

        # Remove all of the remaining working manifests from the working list
        # and pipeline
        for manifest in working_manifests:
            self.state.working_manifests.remove(manifest)
            self.state.pipeline.manifests.remove(manifest)

class StepHandlerMetadata(types.StepHandler):
    def extract(self, step_def):
        self.name = self.state.spec_util.extract_property(step_def, "name")

        self.namespace = self.state.spec_util.extract_property(step_def, "namespace")

        self.annotations = self.state.spec_util.extract_property(step_def, "annotations")

        self.labels = self.state.spec_util.extract_property(step_def, "labels")

    def run(self):
        working_manifests = self.state.working_manifests.copy()

        for manifest in working_manifests:
            manifest_vars = manifest.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(manifest_vars)

            spec = manifest.spec

            if spec.get("metadata") is None:
                spec["metadata"] = {}

            name = spec_util.resolve(self.name, (str, type(None)))
            if name is not None:
                spec["metadata"]["name"] = name

            namespace = spec_util.resolve(self.namespace, (str, type(None)))
            if namespace is not None:
                spec["metadata"]["namespace"] = namespace

            annotations = spec_util.resolve(self.annotations, (dict, type(None)))
            if annotations is not None:
                if spec["metadata"].get("annotations") is None:
                    spec["metadata"]["annotations"] = {}

                for key in annotations:
                    spec["metadata"]["annotations"][key] = spec_util.resolve(annotations[key], str)

            labels = spec_util.resolve(self.labels, (dict, type(None)))
            if labels is not None:
                if spec["metadata"].get("labels") is None:
                    spec["metadata"]["labels"] = {}

                for key in labels:
                    spec["metadata"]["labels"][key] = spec_util.resolve(labels[key], str)

types.default_handlers["pipeline"] = StepHandlerPipeline
types.default_handlers["import"] = StepHandlerImport
types.default_handlers["vars"] = StepHandlerVars
# types.default_handlers["replace"] = StepHandlerReplace
types.default_handlers["stdin"] = StepHandlerStdin
types.default_handlers["refresh_hash"] = StepHandlerRefreshHash
types.default_handlers["jsonpatch"] = StepHandlerJsonPatch
types.default_handlers["metadata"] = StepHandlerMetadata
types.default_handlers["delete"] = StepHandlerDelete
