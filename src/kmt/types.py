import os
import yaml
import jinja2
import inspect
import copy
import logging
import re

from . import util
from kmt import yamlwrap

from . import exception
from .exception import PipelineRunException
from jinja2.meta import find_undeclared_variables

logger = logging.getLogger(__name__)

# Default handlers, support handlers and filters. These can be amended elsewhere
# and apply to newly created Common objects
default_handlers = {}
default_step_support_handlers = []
default_pipeline_support_handlers = []
default_filters = {}

#
# Manifest name lookups
class Lookup:
    def __init__(self, spec):
        util.validate(isinstance(spec, dict), "Invalid specification passed to Lookup")

        self.spec = spec.copy()

        allowed_keys = [
            "group",
            "version",
            "kind",
            "api_version",
            "namespace",
            "pattern"
        ]

        errored = []
        for key in self.spec.keys():
            if key not in allowed_keys or not isinstance(self.spec[key], str):
                errored.append(key)

        if len(errored) > 0:
            raise PipelineRunException(f"Invalid keys or invalid key value found on lookup: {errored}")

    def find_matches(self, manifests, *, current_namespace=None):
        return self._find(manifests, multiple=True, current_namespace=current_namespace)

    def find_match(self, manifests, *, current_namespace=None):
        return self._find(manifests, multiple=False, current_namespace=current_namespace)

    def _find(self, manifests, *, multiple, current_namespace=None):
        util.validate(isinstance(manifests, list) and all(isinstance(x, Manifest) for x in manifests),
            "Invalid manifests provided to Lookup find")

        matches = []

        for manifest in manifests:

            info = util.extract_manifest_info(manifest)

            if "group" in self.spec and self.spec["group"] != info["group"]:
                continue

            if "version" in self.spec and self.spec["version"] != info["version"]:
                continue

            if "kind" in self.spec and self.spec["kind"] != info["kind"]:
                continue

            if "api_version" in self.spec and self.spec["api_version"] != info["api_version"]:
                continue

            if "namespace" in self.spec:
                if self.spec["namespace"] != info["namespace"]:
                    continue
            elif info["namespace"] is not None and info["namespace"] != current_namespace:
                # If no namespace has been defined in the lookup, we will match on
                # the current namespace and any resource without a namespace.
                continue

            if "pattern" in self.spec and not re.search(self.spec["pattern"], info["name"]):
                continue

            matches.append(manifest)

        if multiple:
            return matches

        if len(matches) == 0:
            raise PipelineRunException("Could not find a matching object for Lookup find")

        if len(matches) > 1:
            raise PipelineRunException("Could not find a single object for Lookup find. Multiple object matches")

        return matches[0]


def lookup_representer(dumper: yaml.SafeDumper, lookup: Lookup):
    return dumper.represent_mapping("!lookup", lookup.spec)

def lookup_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode):
    return Lookup(spec=loader.construct_mapping(node))

yaml.SafeDumper.add_representer(Lookup, lookup_representer)
yaml.Dumper.add_representer(Lookup, lookup_representer)

yaml.SafeLoader.add_constructor("!lookup", lookup_constructor)
yaml.Loader.add_constructor("!lookup", lookup_constructor)

class Manifest:
    def __init__(self, source):
        util.validate(isinstance(source, dict), "Invalid source passed to Manifest init")

        self.spec = source

        self.tags = set()
        self.vars = {}

    def __str__(self):
        output = yamlwrap.dump(self.spec)

        return output

    def create_scoped_vars(self, base_vars):
        util.validate(isinstance(base_vars, dict), "Invalid base_vars passed to Manifest create_vars")

        new_vars = base_vars.copy()

        # Copy any vars that have been defined for the manifest
        for key in self.vars:
            new_vars[key] = self.vars[key]

        # Set some builtin vars
        new_vars["kmt_tags"] = list(self.tags)
        new_vars["kmt_manifest"] = self.spec

        info = util.extract_manifest_info(self.spec, default_value="")

        new_vars["kmt_metadata_group"] = info["group"]
        new_vars["kmt_metadata_version"] = info["version"]
        new_vars["kmt_metadata_kind"] = info["kind"]
        new_vars["kmt_metadata_api_version"] = info["api_version"]
        new_vars["kmt_metadata_namespace"] = info["namespace"]
        new_vars["kmt_metadata_name"] = info["name"]

        return new_vars

    def refresh_hash(self):

        text = yaml.dump(self.spec)

        self.vars["kmt_md5sum"] = util.hash_string(text, hash_type="md5", encoding="utf-8")
        self.vars["kmt_sha1sum"] = util.hash_string(text, hash_type="sha1", encoding="utf-8")
        self.vars["kmt_sha256sum"] = util.hash_string(text, hash_type="sha256", encoding="utf-8")
        self.vars["kmt_shortsum"] = util.hash_string(text, hash_type="short8", encoding="utf-8")

class Common:
    def __init__(self):
        self.environment = jinja2.Environment()

        self.handlers = copy.copy(default_handlers)
        self.step_support_handlers = copy.copy(default_step_support_handlers)
        self.pipeline_support_handlers = copy.copy(default_pipeline_support_handlers)

        for filter_name in default_filters:
            self.environment.filters[filter_name] = default_filters[filter_name]

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
    def __init__(self, pipeline, step_vars, working_manifests):
        util.validate(isinstance(pipeline, Pipeline) or pipeline is None, "Invalid pipeline passed to PipelineStepState")
        util.validate(isinstance(step_vars, dict), "Invalid step vars passed to PipelineStepState")
        util.validate(isinstance(working_manifests, list) and all(isinstance(x, Manifest) for x in working_manifests),
            "Invalid working manifests passed to PipelineStepState")

        self.pipeline = pipeline
        self.vars = step_vars
        self.working_manifests = working_manifests
        self.templater = Templater(self.pipeline.common.environment, self.vars)

        self.skip_handler = False

class PipelineSupportHandler:
    def init(self, pipeline, pipeline_vars):
        util.validate(isinstance(pipeline, Pipeline), "Invalid pipeline passed to PipelineSupportHandler")

        self.pipeline = pipeline
        self.pipeline_vars = pipeline_vars
        self.templater = Templater(self.pipeline.common.environment, self.pipeline_vars)

    def pre(self):
        raise PipelineRunException("pre undefined in PipelineSupportHandler")

    def post(self):
        raise PipelineRunException("post undefined in PipelineSupportHandler")

class StepSupportHandler:
    def init(self, state):
        util.validate(isinstance(state, PipelineStepState), "Invalid step state passed to StepSupportHandler")

        self.state = state

    def extract(self, step):
        raise PipelineRunException("parse undefined in StepSupportHandler")

    def pre(self):
        raise PipelineRunException("pre undefined in StepSupportHandler")

    def post(self):
        raise PipelineRunException("post undefined in StepSupportHandler")

class StepHandler:
    def init(self, state):
        util.validate(isinstance(state, PipelineStepState), "Invalid step state passed to StepHandler")

        self.state = state

    def extract(self, step):
        raise PipelineRunException("parse undefined in StepHandler")

    def run(self):
        raise PipelineRunException("run undefined in StepHandler")

class Pipeline:
    def __init__(self, configdir, common=None, pipeline_vars=None, manifests=None):

        if pipeline_vars is None:
            pipeline_vars = {}

        if manifests is None:
            manifests = []
        
        if common is None:
            common = Common()

        util.validate(isinstance(configdir, str) and configdir != "", "Invalid configdir passed to Pipeline init")
        util.validate(isinstance(pipeline_vars, dict), "Invalid pipeline_vars passed to Pipeline init")
        util.validate(isinstance(manifests, list) and all(isinstance(x, Manifest) for x in manifests),
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
            raise PipelineRunException(f"Config dir {configdir} is not a directory")

        configfile = os.path.join(configdir, "config.yaml")
        if not os.path.isfile(configfile):
            raise PipelineRunException(f"Could not find config.yaml in config directory {configdir}")

        # Parse content of the config file and process parameters
        with open(configfile, "r", encoding="utf-8") as file:
            pipeline_spec = yaml.safe_load(file)

        self.configdir = configdir
        self.configfile = configfile

        #
        # Extract relevant properties from the spec
        #

        templater = Templater(environment=self.common.environment, template_vars={})
        logger.debug("Processing pipeline specification")

        # Config defaults - vars that can be overridden by the supplied vars
        # Don't template the vars - These will be templated when processed in a step
        config_defaults = util.extract_property(pipeline_spec, "defaults", default={})
        config_defaults = templater.resolve(config_defaults, dict, template=False)
        util.validate(isinstance(config_defaults, dict), "Config 'defaults' is not a dictionary")

        # Config vars - vars that can't be overridden
        # Don't template the vars - These will be templated when processed in a step
        config_vars = util.extract_property(pipeline_spec, "vars", default={})
        config_vars = templater.resolve(config_vars, dict, template=False)
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

        # If accept_manifests is true, we'll apply the pipeline steps to the incoming manifests as well
        if accept_manifests:
            self.manifests = self._input_manifests
            self._input_manifests = []

        # Make sure there are no other properties left on the pipeline spec
        util.validate(len(pipeline_spec.keys()) == 0, f"Unknown properties on pipeline specification: {pipeline_spec.keys()}")

        #
        # Merge variables in to the pipeline variables in order
        #

        # Merge defaults in to the pipeline vars
        for key in config_defaults:
            self.vars[key] = config_defaults[key]

        # Merge supplied vars in to the pipeline vars
        for key in pipeline_vars:
            self.vars[key] = pipeline_vars[key]

        # Merge 'vars' in to the pipeline vars last as these take preference
        for key in config_vars:
            self.vars[key] = config_vars[key]

        # Built in vars - override anything else defined
        self.vars["env"] = os.environ.copy()

    def run(self):

        # Run the pipeline and capture any manifests, without resolving lookups
        manifests = self.run_no_resolve()

        # Call _resolve_reference for all nodes in the manifest to see if replacement
        # is required
        for manifest in manifests:
            util.walk_object(manifest.spec, lambda x: self._resolve_reference(manifest, x), update=True)

        return manifests

    def _resolve_reference(self, current_manifest, item):
        if isinstance(item, Lookup):
            current_namespace = None
            metadata = current_manifest.spec.get('metadata')
            if metadata is not None:
                current_namespace = metadata.get("namespace")

            manifest = item.find_match(self.manifests, current_namespace=current_namespace)

            metadata = manifest.spec.get("metadata")
            util.validate(isinstance(metadata, dict), f"Invalid metadata on object in _resolve_reference: {type(metadata)}")

            name = metadata.get("name")
            util.validate(isinstance(name, str), f"Invalid name on object in _resolve_reference: {type(name)}")

            return name

        return item

    def run_no_resolve(self):

        # Create and initialise pipeline support handlers
        ps_handlers = [x() for x in self.common.pipeline_support_handlers]
        for ps_handler in ps_handlers:
            ps_handler.init(self, self.vars)

        # Run pre for all pipeline support handlers
        for ps_handler in ps_handlers:
            logger.debug(f"Running pipeline support handler pre: {ps_handler}")
            ps_handler.pre()

        # Process each of the steps in this pipeline
        for step_outer in self.pipeline_steps:
            logger.debug(f"Processing step with specification: {step_outer}")

            # Create a common state used by each support handler and handler
            step_vars = copy.deepcopy(self.vars)
            step_vars["kmt_manifests"] = self.manifests

            state = PipelineStepState(pipeline=self, step_vars=step_vars,
                working_manifests=self.manifests.copy())

            # Initialise each support handler based on the step definition
            # This is the outer step definition, not the arguments to the handler
            ss_handlers = [x() for x in self.common.step_support_handlers]
            for support in ss_handlers:
                support.init(state)
                support.extract(step_outer)

            # Once the support handlers have initialised, there should be a single
            # key representing the handler type
            if len(step_outer.keys()) < 1:
                raise PipelineRunException("Missing step type on the step definition")
            
            if len(step_outer.keys()) > 1:
                raise PipelineRunException(f"Multiple keys remaining on the step definition - cannot determine type: {step_outer.keys()}")

            # Extract the step type
            step_type = [x for x in step_outer][0]
            if not isinstance(step_type, str) or step_type == "":
                raise PipelineRunException("Invalid step type on step definition")

            logger.debug(f"Step type: {step_type}")

            # Extract the step config and allow it to be templated with vars defined up to this
            # point
            templater = Templater(self.common.environment, state.vars)
            step_inner = util.extract_property(step_outer, step_type, default={})
            step_inner = templater.resolve(step_inner, (dict, type(None)))
            if step_inner is None:
                step_inner = {}
            util.validate(isinstance(step_inner, dict), "Invalid value for step inner configuration")

            # Create the handler object to process the handler config
            if step_type not in self.common.handlers:
                raise PipelineRunException(f"Missing handler for step type: {step_type}")
            
            handler = self.common.handlers[step_type]()
            handler.init(state)
            handler.extract(step_inner)

            # Make sure there are no remaining properties that the handler wasn't looking for
            if len(step_inner.keys()) > 0:
                raise PipelineRunException(f"Unexpected properties for handler config: {step_inner.keys()}")

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
    def __init__(self, environment, template_vars):
        util.validate(isinstance(environment, jinja2.Environment), "Invalid environment passed to Templater ctor")
        util.validate(isinstance(template_vars, dict), "Invalid template vars passed to Templater")

        self._environment = environment

        # Define the template vars
        # Don't copy the template vars, just reference it. These vars may be changed elsewhere
        # and shouldn't need to be reimported or altered within the Templater
        self._unresolved_vars = template_vars
        self.vars = copy.deepcopy(template_vars)
        self._resolve_var_refs()

    def new_scope(self, new_vars):
        util.validate(isinstance(new_vars, dict), "Invalid new_vars passed to new_scope")

        # Create a new set of vars
        working_vars = self._unresolved_vars.copy()
        for key in new_vars:
            working_vars[key] = new_vars[key]

        new_templater = Templater(self._environment, working_vars)

        return new_templater

    def template_if_string(self, val, var_override=None, limit=30):
        util.validate(isinstance(var_override, dict) or var_override is None,
            "Invalid var_override passed to template_if_string")
        util.validate(isinstance(limit, int) and limit > 0, "Invalid limit passed to template_if_string")

        # Determine which vars will be used for templating
        template_vars = self.vars
        if var_override is not None:
            template_vars = var_override

        if not isinstance(val, str):
            return val
        
        # Perform at most 'count' passes of the string
        count = 0
        current = val

        while count < limit:
            count = count + 1

            template = self._environment.from_string(current)
            output = template.render(template_vars)
            if output == current:
                return output

            current = output

        raise PipelineRunException(f"Reached recursion limit for string template '{val}'")

    def resolve(self, value, types, /, template=True, recursive=False, var_override=None):
        util.validate(isinstance(template, bool), "Invalid value for template passed to resolve")

        # Template the value, if it is a string
        if template:
            if recursive:
                value = self.recursive_template(value, var_override=var_override)
            else:
                value = self.template_if_string(value, var_override=var_override)

        value = util.coerce_value(types, value)

        return value

    def recursive_template(self, item, var_override=None):

        # Potentially convert a string to a dict or list type
        item = self.template_if_string(item, var_override=var_override)

        # Walk the object and template anything that is a string
        item = util.walk_object(item, lambda x: self.template_if_string(x, var_override=var_override), update=True)

        return item

    def _get_template_str_vars(self, template_str):
        if not isinstance(template_str, str):
            return set()

        ast = self._environment.parse(template_str)
        deps = set(find_undeclared_variables(ast))

        return deps

    def _resolve_var_refs(self):
        """
        Performs templating on the source dictionary and attempts to resolve variable references
        taking in to account nested references
        """

        var_map = {}

        # Create a map of keys to the vars the value references
        for key in self.vars:
            deps = set()

            # Recursively walk through all properties for the object and calculate a set
            # of dependencies
            # It's possible that some dependencies could be resolvable, but will show as unresolvable here:
            # If a.b depends on x.y, and x.x depends on a.a, it could, in theory, be resolved, but this will
            # show it as unresolvable.
            # Since we don't have access to that info from jinja2 easily, it would be difficult to calculate
            # and offers little value being a small edge case.
            util.walk_object(self.vars[key], lambda x: deps.update(self._get_template_str_vars(x)))

            var_map[key] = deps

        # Loop while there are values left in var_map, which represents the key/value
        # and the vars it depends on
        while len(var_map.keys()) > 0:
            process_list = []

            # Add any keys to the process list that don't have any dependencies left
            for key in var_map:
                if len(var_map[key]) == 0:
                    process_list.append(key)

            # Remove the items we're processing from the var map
            for key in process_list:
                var_map.pop(key)

            # Fail if there is nothing to process
            if len(process_list) < 1:
                raise exception.KMTResolveException("Circular or unresolvable variable references")

            for prockey in process_list:
                # Template the variable and update 'new_vars'
                self.vars[prockey] = self.template_if_string(self.vars[prockey])

                # Remove the variable as a dependency for all other variables
                for key in var_map:
                    if prockey in var_map[key]:
                        var_map[key].remove(prockey)
