import logging
import copy
import yaml
import re
import jsonpatch
import glob
import sys

import kmt.core as core
import kmt.util as util
import kmt.yaml_types as yaml_types
import kmt.exception as exception

from .exception import PipelineRunException

logger = logging.getLogger(__name__)

class StepHandlerPipeline(core.StepHandler):
    """
    """
    def extract(self, step_def):
        # Path to other pipeline
        self.path = util.extract_property(step_def, "path")

        self.pass_vars = util.extract_property(step_def, "pass_vars", default=[])
        self.vars = util.extract_property(step_def, "vars", default={})

        self.pass_manifests = util.extract_property(step_def, "pass_manifests", default=False)

    def run(self):
        templater = self.state.pipeline.get_templater()

        # Determine whether we pass manifests to the new pipeline
        # Filtering is done via normal support handlers e.g. when, tags, etc.
        pass_manifests = templater.resolve(self.pass_manifests, bool)

        # Path to the other pipeline
        path = templater.resolve(self.path, str)

        # Resolve pass_vars_filter
        pass_vars = templater.resolve(self.pass_vars, list)

        # Resolve vars
        step_vars = templater.resolve(self.vars, dict)

        #
        # Determine all of the vars to pass to the new pipeline
        pipeline_vars = {}

        # Are we passing existing vars to the new pipline
        for var_name in pass_vars:
            split = var_name.split("=")
            if len(split) == 1:
                target_var = split[0]
                source_var = split[0]
            elif len(split) == 2:
                target_var = split[0]
                source_var = split[1]
            else:
                raise exception.KMTManifestException("Invalid syntax for variable rename in pass_vars")

            if source_var not in self.state.pipeline.vars:
                continue

            pipeline_vars[target_var] = self.state.pipeline.vars[source_var]

        # Resolve any vars specified in the 'vars' parameter and update pass_vars
        new_vars = templater.resolve(step_vars, dict, recursive=True)
        pipeline_vars.update(new_vars)

        pipeline_manifests = []
        if pass_manifests:
            # If we're passing manifests to the new pipeline, then the working_manifests
            # list needs to be cleared and the passed manifests removed from the current pipeline
            # manifest list
            for spec in self.state.working_manifests:
                self.state.pipeline.manifests.remove(spec)

            # Only pass the spec, not the Manifest object itself
            pipeline_manifests = [x.spec for x in self.state.working_manifests]
            self.state.working_manifests = []

            # The working manifests are no longer in the pipeline manifests and working_manifests is empty.
            # pipeline_manifests holds the only reference to these manifests now

        # Create the new pipeline and run
        pipeline = core.Pipeline(path, common=self.state.pipeline.common,
                        pipeline_vars=pipeline_vars, manifests=pipeline_manifests,
                        root_pipeline=False)

        pipeline_manifests = [x.spec for x in pipeline.run()]

        # The manifests returned from the pipeline will be added to the working manifests
        # If pass_manifests is true, then working_manifests would be empty, but if not, then
        # there are still working manifests to be preserved, so append the manifests
        # They also need to be entered in to the pipeline manifest list
        for spec in pipeline_manifests:
            new_manifest = core.Manifest(spec, pipeline=self.state.pipeline)
            self.state.working_manifests.append(new_manifest)
            self.state.pipeline.manifests.append(new_manifest)

class StepHandlerImport(core.StepHandler):
    """
    """
    def extract(self, step_def):
        self.import_files = util.extract_property(step_def, "files")

        self.recursive = util.extract_property(step_def, "recursive", default=False)

        self.template = util.extract_property(step_def, "template", default=True)

    def run(self):
        templater = self.state.pipeline.get_templater()

        filenames = set()

        import_files = templater.resolve(self.import_files, list)
        import_files = [templater.resolve(x, str) for x in import_files]

        recursive = templater.resolve(self.recursive, bool)

        template = templater.resolve(self.template, bool)

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
                content = templater.template_if_string(content)
                if not isinstance(content, str):
                    raise PipelineRunException("Could not template import text")

            # Load all documents from the file, after any templating
            docs = [x for x in util.yaml_load_all(content)]

            for doc in docs:
                manifest = core.Manifest(doc, pipeline=self.state.pipeline)
                manifest.local_vars["import_filename"] = filename

                self.state.pipeline.manifests.append(manifest)
                self.state.working_manifests.append(manifest)

class StepHandlerVars(core.StepHandler):
    """
    """
    def extract(self, step_def):
        self.pipeline_var_list = util.extract_property(step_def, "pipeline", default=[])

        self.manifest_var_list = util.extract_property(step_def, "manifest", default=[])

    def run(self):
        working_manifests = self.state.working_manifests.copy()
        templater = self.state.pipeline.get_templater()

        pipeline_var_list = templater.resolve(self.pipeline_var_list, (list, type(None)))
        if pipeline_var_list is not None:
            pipeline_var_list = [templater.resolve(x, dict) for x in pipeline_var_list]
            # pipeline_vars should be a list of dictionaries now

            for var_spec in pipeline_var_list:
                # Extract vars
                var_spec = var_spec.copy()
                key = util.extract_property(var_spec, "key")
                value = util.extract_property(var_spec, "value")
                util.validate(len(var_spec.keys()) == 0, f"Unknown properties on vars spec: {var_spec.keys()}")

                # Resolve any templating and type
                key = templater.resolve(key, str)
                value = templater.resolve(value, str)

                self.state.pipeline.vars[key] = value
                logger.debug(f"Set pipeline var {key} -> {value}")

        for manifest in working_manifests:
            templater = manifest.get_templater()

            manifest_var_list = templater.resolve(self.manifest_var_list, (list, type(None)))

            if manifest_var_list is not None:
                manifest_var_list = [templater.resolve(x, dict) for x in manifest_var_list]
                # manifest_var_list should be a list of dictionaries now

                for var_spec in manifest_var_list:
                    # Extract vars
                    var_spec = var_spec.copy()
                    key = util.extract_property(var_spec, "key")
                    value = util.extract_property(var_spec, "value")
                    set_pipeline = util.extract_property(var_spec, "pipeline", default=False)
                    util.validate(len(var_spec.keys()) == 0, f"Unknown properties on vars spec: {var_spec.keys()}")

                    # Resolve any templating and type
                    key = templater.resolve(key, str)
                    value = templater.resolve(value, str)
                    set_pipeline = templater.resolve(set_pipeline, bool)

                    if set_pipeline:
                        self.state.pipeline.vars[key] = value
                        logger.debug(f"Set pipeline var {key} -> {value}")
                    else:
                        manifest.local_vars[key] = value
                        logger.debug(f"Set manifest var {key} -> {value}")

class StepHandlerStdin(core.StepHandler):
    """
    """
    def extract(self, step_def):

        self.template = util.extract_property(step_def, "template", default=True)

    def run(self):
        templater = self.state.pipeline.get_templater()

        template = templater.resolve(self.template, bool)

        # Read content from stdin
        logger.debug("stdin: reading document from stdin")
        content = sys.stdin.read()

        if template:
            content = templater.template_if_string(content)
            if not isinstance(content, str):
                raise PipelineRunException("Could not template import text")

        # Load all documents from the file, after any templating
        docs = [x for x in util.yaml_load_all(content)]

        for doc in docs:
            manifest = core.Manifest(doc, self.state.pipeline)

            self.state.pipeline.manifests.append(manifest)
            self.state.working_manifests.append(manifest)

class StepHandlerJsonPatch(core.StepHandler):
    def extract(self, step_def):
        self.patches = util.extract_property(step_def, "patches")

    def run(self):
        working_manifests = self.state.working_manifests.copy()

        for manifest in working_manifests:
            templater = manifest.get_templater()

            # Apply the patches to the manifest object
            patches = templater.resolve(self.patches, list)
            patches = [templater.resolve(x, dict) for x in patches]
            patch_list = jsonpatch.JsonPatch(patches)
            manifest.spec = patch_list.apply(manifest.spec)

class StepHandlerDelete(core.StepHandler):
    def extract(self, step_def):
        pass

    def run(self):
        working_manifests = self.state.working_manifests.copy()

        # Remove all of the remaining working manifests from the working list
        # and pipeline
        for manifest in working_manifests:
            self.state.working_manifests.remove(manifest)
            self.state.pipeline.manifests.remove(manifest)

class StepHandlerMetadata(core.StepHandler):
    def extract(self, step_def):
        self.name = util.extract_property(step_def, "name")

        self.namespace = util.extract_property(step_def, "namespace")

        self.annotations = util.extract_property(step_def, "annotations")

        self.labels = util.extract_property(step_def, "labels")

    def run(self):
        working_manifests = self.state.working_manifests.copy()

        for manifest in working_manifests:
            templater = manifest.get_templater()

            spec = manifest.spec

            if spec.get("metadata") is None:
                spec["metadata"] = {}

            name = templater.resolve(self.name, (str, type(None)))
            if name is not None:
                spec["metadata"]["name"] = name

            namespace = templater.resolve(self.namespace, (str, type(None)))
            if namespace is not None:
                spec["metadata"]["namespace"] = namespace

            annotations = templater.resolve(self.annotations, (dict, type(None)))
            if annotations is not None:
                if spec["metadata"].get("annotations") is None:
                    spec["metadata"]["annotations"] = {}

                for key in annotations:
                    spec["metadata"]["annotations"][key] = templater.resolve(annotations[key], str)

            labels = templater.resolve(self.labels, (dict, type(None)))
            if labels is not None:
                if spec["metadata"].get("labels") is None:
                    spec["metadata"]["labels"] = {}

                for key in labels:
                    spec["metadata"]["labels"][key] = templater.resolve(labels[key], str)

core.default_handlers["pipeline"] = StepHandlerPipeline
core.default_handlers["import"] = StepHandlerImport
core.default_handlers["vars"] = StepHandlerVars
core.default_handlers["stdin"] = StepHandlerStdin
core.default_handlers["jsonpatch"] = StepHandlerJsonPatch
core.default_handlers["metadata"] = StepHandlerMetadata
core.default_handlers["delete"] = StepHandlerDelete
