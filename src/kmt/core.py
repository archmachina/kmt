import os
import yaml
import jinja2
import inspect
import copy
import logging

import kmt.yaml_types as yaml_types
import kmt.util as util
import kmt.exception as exception

logger = logging.getLogger(__name__)

# Default handlers, support handlers and filters. These can be amended elsewhere
# and apply to newly created Common objects
default_handlers = {}
default_step_support_handlers = []
default_pipeline_support_handlers = []
default_filters = {}
default_globals = {}

class Manifest:
    def __init__(self, source, *, pipeline):
        util.validate(isinstance(source, dict), "Invalid source passed to Manifest init")
        util.validate(isinstance(pipeline, Pipeline), "Invalid pipeline passed to Manifest init")

        self.spec = source
        self.pipeline = pipeline

        self.tags = set()
        self.local_vars = {}

        # Require a minimum of a metadata dictionary and name to create this
        # manifest.
        if "metadata" not in self.spec:
            raise exception.KMTManifestException("Missing metadata on manifest")

        metadata = self.spec.get("metadata")
        if not isinstance(metadata, dict):
            raise exception.KMTManifestException(f"Invalid metadata type on manifest: {type(metadata)}")

        if "name" not in metadata:
            raise exception.KMTManifestException("Missing name on manifest")

        name = metadata.get("name")
        if not isinstance(name, str) or name == "":
            raise exception.KMTManifestException("Missing or invalid name on manifest")

        # Add annotations, if not present
        annotations = metadata.get("annotations")
        if annotations is None:
            annotations = {}
            metadata["annotations"] = annotations

        if not isinstance(annotations, dict):
            raise exception.KMTManifestException("Invalid type for annotations on manifest")

        # alias
        name = metadata.get("name")
        if "kmt/alias" not in annotations:
            annotations["kmt/alias"] = name

    def __str__(self):
        output = util.yaml_dump(self.spec)

        return output

    def get_templater(self):

        # Update metadata vars first
        self.refresh_metadata()

        # Add builtin values
        builtin = {
            "env": os.environ.copy(),
            "kmt_manifests": [x.spec for x in self.pipeline.manifests],
            "kmt_tags": list(self.tags),
            "kmt_manifest": self.spec
        }

        effective_vars = self.pipeline.vars.copy()
        effective_vars.update(self.local_vars)
        effective_vars.update(builtin)

        overlay = self.pipeline.common.environment.overlay()
        overlay.kmt_pipeline = self.pipeline
        overlay.kmt_manifest = self

        return Templater(overlay, effective_vars)

    def get_info(self, default_value=None):
        # api version
        # Don't use the 'default_value' yet as we want to know whether it exists first
        api_version = self.spec.get("apiVersion")

        # group and version
        group = default_value
        version = default_value
        if isinstance(api_version, str) and api_version != "":
            split = api_version.split("/")

            if len(split) == 1:
                version = split[0]
            elif len(split) == 2:
                group = split[0]
                version = split[1]

        # Update the api_version to the default, if it didn't exist or was None
        if api_version is None:
            api_version = default_value

        # Kind
        kind = self.spec.get("kind", default_value)

        # metadata
        metadata = self.get_metadata()

        # Name and Namespace
        name = metadata.get("name", default_value)
        namespace = metadata.get("namespace", default_value)

        # Annotations
        annotations = self.get_annotations()

        # Labels
        labels = self.get_labels()

        # Manifest alias
        alias = annotations.get("kmt/alias", default_value)

        return {
            "group": group,
            "version": version,
            "kind": kind,
            "api_version": api_version,
            "namespace": namespace,
            "name": name,
            "alias": alias,
            "manifest": self,
            "metadata": metadata,
            "annotations": annotations,
            "labels": labels
        }

    def refresh_metadata(self):

        info = self.get_info(default_value="")

        self.local_vars["kmt_metadata_group"] = info["group"]
        self.local_vars["kmt_metadata_version"] = info["version"]
        self.local_vars["kmt_metadata_kind"] = info["kind"]
        self.local_vars["kmt_metadata_api_version"] = info["api_version"]
        self.local_vars["kmt_metadata_namespace"] = info["namespace"]
        self.local_vars["kmt_metadata_name"] = info["name"]

    def validate(self):
        """
        Validation of some minimum requirements for the manifest
        """
        if "metadata" not in self.spec:
            raise exception.KMTManifestException("Missing metadata on manifest")

        metadata = self.spec.get("metadata")
        if not isinstance(metadata, dict):
            raise exception.KMTManifestException("Invalid metadata on manifest")

        if "name" not in metadata:
            raise exception.KMTManifestException("Missing name on manifest metadata")

        name = metadata.get("name")
        if not isinstance(name, str):
            raise exception.KMTManifestException("Invalid type or missing manifest name")

        if name == "":
            raise exception.KMTManifestException("Empty manifest name on manifest")

        if "kind" not in self.spec:
            raise exception.KMTManifestException("Missing kind on manifest")
        kind = self.spec.get("kind")

        if not isinstance(kind, str) or kind == "":
            raise exception.KMTManifestException("Invalid type or missing kind on manifest")

        if "annotations" in metadata:
            annotations = metadata.get("annotations")
            if not isinstance(annotations, dict):
                raise exception.KMTManifestException("Invalid type for annotations on manifest")

        if "labels" in metadata:
            labels = metadata.get("labels")
            if not isinstance(labels, dict):
                raise exception.KMTManifestException("Invalid type for labels on manifest")

    def get_metadata(self):
        metadata = self.spec.get("metadata")

        if not isinstance(metadata, dict):
            raise exception.KMTManifestException("Missing metadata on manifest")

        return metadata

    def get_annotations(self):
        metadata = self.get_metadata()

        annotations = metadata.get("annotations")
        if annotations is None:
            annotations = {}
            metadata["annotations"] = annotations

        if not isinstance(annotations, dict):
            raise exception.KMTManifestException("Invalid annotations type on manifest")

        return annotations

    def get_labels(self):
        metadata = self.get_metadata()

        labels = metadata.get("labels")
        if labels is None:
            labels = {}
            metadata["labels"] = labels

        if not isinstance(labels, dict):
            raise exception.KMTManifestException("Invalid labels type on manifest")

        return labels

class Common:
    def __init__(self):
        self.environment = jinja2.Environment(undefined=jinja2.StrictUndefined)

        # Make sure the jinja2 environment has these properties
        self.environment.extend(kmt_pipeline=None, kmt_manifest=None)

        self.handlers = copy.copy(default_handlers)
        self.step_support_handlers = copy.copy(default_step_support_handlers)
        self.pipeline_support_handlers = copy.copy(default_pipeline_support_handlers)

        for filter_name in default_filters:
            self.environment.filters[filter_name] = default_filters[filter_name]

        for global_name in default_globals:
            self.environment.globals[global_name] = default_globals[global_name]

    def add_handlers(self, handlers):
        util.validate(isinstance(handlers, dict), "Invalid handlers passed to add_handlers")
        util.validate((all(x is None or (inspect.isclass(x) and issubclass(x, StepHandler))) for x in handlers.values()), "Invalid handlers passed to add_handlers")

        for key in handlers:
            self.handlers[key] = handlers[key]

    def add_step_support_handlers(self, handlers):
        util.validate(isinstance(handlers, list), "Invalid handlers passed to add_step_support_handlers")
        util.validate((all(inspect.isclass(x) and issubclass(x, StepSupportHandler)) for x in handlers), "Invalid handlers passed to add_step_support_handlers")

        for handler in handlers:
            if handler not in self.step_support_handlers:
                self.step_support_handlers.append(handler)

    def add_pipeline_support_handlers(self, handlers):
        util.validate(isinstance(handlers, list), "Invalid handlers passed to add_pipeline_support_handlers")
        util.validate((all(inspect.isclass(x) and issubclass(x, PipelineSupportHandler)) for x in handlers), "Invalid handlers passed to add_pipeline_support_handlers")

        for handler in handlers:
            if handler not in self.pipeline_support_handlers:
                self.pipeline_support_handlers.append(handler)

    def add_filters(self, filters):
        util.validate(isinstance(filters, dict), "Invalid filters passed to add_filters")
        util.validate(all((callable(x) or x is None) for x in filters.values()), "Invalid filters passed to add_filters")

        for key in filters:
            self.environment.filters[key] = filters[key]

class PipelineStepState:
    def __init__(self, pipeline, working_manifests):
        util.validate(isinstance(pipeline, Pipeline) or pipeline is None, "Invalid pipeline passed to PipelineStepState")
        util.validate(isinstance(working_manifests, list) and all(isinstance(x, Manifest) for x in working_manifests),
            "Invalid working manifests passed to PipelineStepState")

        self.pipeline = pipeline
        self.working_manifests = working_manifests

        self.skip_handler = False

class PipelineSupportHandler:
    def init(self, pipeline):
        util.validate(isinstance(pipeline, Pipeline), "Invalid pipeline passed to PipelineSupportHandler")

        self.pipeline = pipeline

    def pre(self):
        raise exception.PipelineRunException("pre undefined in PipelineSupportHandler")

    def post(self):
        raise exception.PipelineRunException("post undefined in PipelineSupportHandler")

class StepSupportHandler:
    def init(self, state):
        util.validate(isinstance(state, PipelineStepState), "Invalid step state passed to StepSupportHandler")

        self.state = state

    def extract(self, step):
        raise exception.PipelineRunException("parse undefined in StepSupportHandler")

    def pre(self):
        raise exception.PipelineRunException("pre undefined in StepSupportHandler")

    def post(self):
        raise exception.PipelineRunException("post undefined in StepSupportHandler")

class StepHandler:
    def init(self, state):
        util.validate(isinstance(state, PipelineStepState), "Invalid step state passed to StepHandler")

        self.state = state

    def extract(self, step):
        raise exception.PipelineRunException("parse undefined in StepHandler")

    def run(self):
        raise exception.PipelineRunException("run undefined in StepHandler")

class Pipeline:
    def __init__(self, configdir, common=None, pipeline_vars=None, manifests=None, root_pipeline=True):

        if pipeline_vars is None:
            pipeline_vars = {}

        if manifests is None:
            manifests = []

        if common is None:
            common = Common()

        util.validate(isinstance(configdir, str) and configdir != "", "Invalid configdir passed to Pipeline init")
        util.validate(isinstance(pipeline_vars, dict), "Invalid pipeline_vars passed to Pipeline init")
        util.validate(isinstance(manifests, list) and all(isinstance(x, dict) for x in manifests),
            "Invalid manifests passed to Pipeline init")
        util.validate(isinstance(common, Common), "Invalid common object passed to Pipeline init")

        self.common = common

        self._input_manifests = manifests
        self.manifests = []

        self.vars = {}

        #
        # Read and parse configuration file as yaml
        #

        # Open config file from configdir
        configdir = os.path.realpath(configdir)
        logger.debug(f"Processing pipeline for directory: {configdir}")
        if not os.path.isdir(configdir):
            raise exception.PipelineRunException(f"Config dir {configdir} is not a directory")

        configfile = os.path.join(configdir, "config.yaml")
        if not os.path.isfile(configfile):
            raise exception.PipelineRunException(f"Could not find config.yaml in config directory {configdir}")

        # Parse content of the config file and process parameters
        with open(configfile, "r", encoding="utf-8") as file:
            pipeline_spec = util.yaml_load(file)

        self.configdir = configdir
        self.configfile = configfile

        #
        # Extract relevant properties from the spec
        #

        # Temporary templater, just for retrieving the pipeline configuration
        templater = Templater(environment=self.common.environment, template_vars={})
        logger.debug("Processing pipeline specification")

        # Config defaults - vars that can be overridden by the supplied vars
        # Don't template the vars - These will be templated when processed in a step
        config_defaults = util.extract_property(pipeline_spec, "defaults", default={})
        config_defaults = templater.resolve(config_defaults, (dict, type(None)), template=False)
        if config_defaults is None:
            config_defaults = {}
        util.validate(isinstance(config_defaults, dict), "Config 'defaults' is not a dictionary")

        # Config vars - vars that can't be overridden
        # Don't template the vars - These will be templated when processed in a step
        config_vars = util.extract_property(pipeline_spec, "vars", default={})
        config_vars = templater.resolve(config_vars, (dict, type(None)), template=False)
        if config_vars is None:
            config_vars = {}
        util.validate(isinstance(config_vars, dict), "Config 'vars' is not a dictionary")

        # Pipeline - list of the steps to run for this pipeline
        # Don't template the pipeline steps - These will be templated when they are executed
        config_pipeline = util.extract_property(pipeline_spec, "pipeline", default=[])
        config_pipeline = templater.resolve(config_pipeline, list, template=False)
        util.validate(isinstance(config_pipeline, list), "Config 'pipeline' is not a list")
        self.pipeline_steps = config_pipeline

        # Accept manifests - whether to include incoming manifests in pipeline processing
        accept_manifests = util.extract_property(pipeline_spec, "accept_manifests", default=False)
        accept_manifests = templater.resolve(accept_manifests, bool)
        util.validate(isinstance(accept_manifests, bool), "Invalid type for accept_manifests")

        # Accept vars - A list of the vars that will be accepted for input from the parent/caller
        accept_vars = util.extract_property(pipeline_spec, "accept_vars", default=[])
        accept_vars = templater.resolve(accept_vars, list)
        util.validate(isinstance(accept_vars, list) and all(isinstance(x, str) for x in accept_vars),
            "Invalid type for accept_vars or members")

        # Create manifests out of the dictionary specs passed in
        self._input_manifests = [Manifest(x, pipeline=self) for x in self._input_manifests]

        # If accept_manifests is true, we'll apply the pipeline steps to the incoming manifests as well
        if accept_manifests:
            self.manifests = self._input_manifests
            self._input_manifests = []

        # Make sure there are no other properties left on the pipeline spec
        util.validate(len(pipeline_spec.keys()) == 0, f"Unknown properties on pipeline specification: {pipeline_spec.keys()}")

        #
        # Merge variables in to the pipeline variables in order
        #

        # Make sure only allowed vars are being passed to this pipeline
        allowed_var_list = set()
        allowed_var_list.update(accept_vars)
        allowed_var_list.update(config_defaults.keys())

        disallowed = []
        for var_name in pipeline_vars:
            if var_name not in allowed_var_list:
                disallowed.append(var_name)

        if len(disallowed) > 0:
            raise exception.KMTConfigException(f"Invalid vars passed to pipeline: {disallowed}")

        # Merge defaults, then supplied vars, then 'vars' in to the pipeline
        # Allows defaults to be overridden by the caller, but then 'vars'
        # can enforce a value, if required
        unresolved_vars = {}
        unresolved_vars.update(config_defaults)
        unresolved_vars.update(pipeline_vars)
        unresolved_vars.update(config_vars)

        # Add builtin pipeline vars
        builtin = {
            "env": os.environ.copy(),
            "kmt_manifests": [x.spec for x in self.manifests]
        }
        unresolved_vars.update(builtin)

        # Create a list of vars that shouldn't be templated as it either doesn't apply or they have
        # already been templated and shouldn't be templated again.
        # Need to add the pipeline_vars keys, but remove any that have been defined in
        # the pipeline 'vars', as they haven't been templated yet
        ignore_list = set(builtin.keys())

        for key in pipeline_vars:
            ignore_list.add(key)

        for key in config_vars:
            if key in ignore_list:
                ignore_list.remove(key)

        # Resolve all vars from unresolved vars and store the result in the actual
        # vars property.
        # pipeline.vars can be used to access variables that have already been resolved
        self.vars = util.resolve_var_refs(unresolved_vars, self.common.environment, ignore_list=ignore_list)

        # root_pipeline defines whether to act as the final/top level pipeline
        self.root_pipeline = root_pipeline

    def get_templater(self):
        overlay = self.common.environment.overlay()
        overlay.kmt_pipeline = self
        overlay.kmt_manifest = None

        return Templater(overlay, self.vars)

    def run(self):

        # Create and initialise pipeline support handlers
        ps_handlers = [x() for x in self.common.pipeline_support_handlers]
        for ps_handler in ps_handlers:
            ps_handler.init(self)

        # Run pre for all pipeline support handlers
        for ps_handler in ps_handlers:
            logger.debug(f"Running pipeline support handler pre: {ps_handler}")
            ps_handler.pre()

        # Process each of the steps in this pipeline
        for step_outer in self.pipeline_steps:
            logger.debug(f"Processing step with specification: {step_outer}")

            state = PipelineStepState(pipeline=self, working_manifests=self.manifests.copy())

            # Initialise each support handler based on the step definition
            # This is the outer step definition, not the arguments to the handler
            ss_handlers = [x() for x in self.common.step_support_handlers]
            for support in ss_handlers:
                support.init(state)
                support.extract(step_outer)

            # Once the support handlers have initialised, there should be a single
            # key representing the handler type
            if len(step_outer.keys()) < 1:
                raise exception.PipelineRunException("Missing step type on the step definition")
            
            if len(step_outer.keys()) > 1:
                raise exception.PipelineRunException(f"Multiple keys remaining on the step definition - cannot determine type: {step_outer.keys()}")

            # Extract the step type
            step_type = [x for x in step_outer][0]
            if not isinstance(step_type, str) or step_type == "":
                raise exception.PipelineRunException("Invalid step type on step definition")

            logger.debug(f"Step type: {step_type}")

            # Extract the step config and allow it to be templated
            templater = Templater(self.common.environment, self.vars)
            step_inner = util.extract_property(step_outer, step_type, default={})
            step_inner = templater.resolve(step_inner, (dict, type(None)))
            if step_inner is None:
                step_inner = {}
            util.validate(isinstance(step_inner, dict), "Invalid value for step inner configuration")

            # Create the handler object to process the handler config
            if step_type not in self.common.handlers:
                raise exception.PipelineRunException(f"Missing handler for step type: {step_type}")
            
            handler = self.common.handlers[step_type]()
            handler.init(state)
            handler.extract(step_inner)

            # Make sure there are no remaining properties that the handler wasn't looking for
            if len(step_inner.keys()) > 0:
                raise exception.PipelineRunException(f"Unexpected properties for handler config: {step_inner.keys()}")

            # Run pre for any support handlers
            logger.debug("Running pre support handlers")
            logger.debug(f"Pipeline manifests: {len(self.manifests)}. Working manifests: {len(state.working_manifests)}")
            for ss_handler in ss_handlers:
                logger.debug(f"Calling support handler pre: {ss_handler}")
                os.chdir(self.configdir)
                ss_handler.pre()

            # Run the main handler
            if not state.skip_handler:
                logger.debug(f"Pipeline manifests: {len(self.manifests)}. Working manifests: {len(state.working_manifests)}")
                logger.debug(f"Calling handler: {handler}")
                os.chdir(self.configdir)
                handler.run()

            # Run post for any support handlers
            logger.debug("Running post support handlers")
            logger.debug(f"Pipeline manifests: {len(self.manifests)}. Working manifests: {len(state.working_manifests)}")
            for ss_handler in ss_handlers:
                logger.debug(f"Calling support handler post: {ss_handler}")
                os.chdir(self.configdir)
                ss_handler.post()

        # Run post for all pipeline support handlers
        for ps_handler in ps_handlers:
            logger.debug(f"Running pipeline support handler post: {ps_handler}")
            ps_handler.post()

        return self.manifests + self._input_manifests

class Templater:
    def __init__(self, environment:jinja2.Environment, template_vars:dict):
        util.validate(isinstance(environment, jinja2.Environment), "Invalid environment passed to Templater ctor")
        util.validate(isinstance(template_vars, dict), "Invalid template vars passed to Templater")

        self._environment = environment
        self.vars = template_vars

    def template_if_string(self, val):

        # Determine which vars will be used for templating
        template_vars = self.vars

        if not isinstance(val, str):
            return val

        template = self._environment.from_string(val)
        output = template.render(template_vars)

        return output

    def resolve(self, value, types=None, *, template=True, recursive=False):
        util.validate(isinstance(template, bool), "Invalid value for template passed to resolve")

        # Template the value, if it is a string
        if template:
            if recursive:
                # Walk the object and template anything that is a string
                value = util.walk_object(value, lambda x: self.template_if_string(x), update=True)
            else:
                value = self.template_if_string(value)

        if types is not None:
            value = util.coerce_value(types, value)

        return value
