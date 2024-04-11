import os
import yaml
import jinja2
import inspect
import copy
import logging

from . import util

from .exception import PipelineRunException

logger = logging.getLogger(__name__)

# Default handlers, support handlers and filters. These can be amended elsewhere
# and apply to newly created Common objects
default_handlers = {}
default_step_support_handlers = []
default_pipeline_support_handlers = []
default_filters = {}

# Define a representer for pyyaml to format multiline strings as block quotes
def str_representer(dumper, data):
    if isinstance(data, str) and '\n' in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

yaml.add_representer(str, str_representer)

#
# Manifest name lookups
class Lookup:
    def __init__(self, *, group=None, version=None, kind=None, namespace=None, pattern=None):
        self.group = group
        self.version = version
        self.kind = kind
        self.namespace = namespace
        self.pattern = pattern

def lookup_representer(dumper: yaml.SafeDumper, lookup: Lookup):
    return dumper.represent_mapping("!lookup", {
        "group": lookup.group,
        "version": lookup.version,
        "kind": lookup.kind,
        "namespace": lookup.namespace,
        "pattern": lookup.pattern
    })

def lookup_constructor(loader: yaml.SafeLoader, node: yaml.nodes.MappingNode):
    return Lookup(**loader.construct_mapping(node))

yaml.SafeDumper.add_representer(Lookup, lookup_representer)
yaml.Dumper.add_representer(Lookup, lookup_representer)

yaml.SafeLoader.add_constructor("!lookup", lookup_constructor)
yaml.Loader.add_constructor("!lookup", lookup_constructor)

class ManifestInfo:
    def __init__(self, source):
        util.validate(isinstance(source, (dict, Manifest)), "Invalid manifest passed to extract_metadata")

        if isinstance(source, Manifest):
            source = source.spec
            util.validate(isinstance(source, dict), "Missing spec on Manifest object")

        # api version
        self.api_version = source.get("apiVersion", "")

        # group and version
        self.group = ""
        self.version = ""
        if self.api_version != "":
            split = self.api_version.split("/")

            if len(split) == 1:
                self.version = split[0]
            elif len(split) == 2:
                self.group = split[0]
                self.version = split[1]

        # Kind
        self.kind = source.get("kind", "")

        # Name and Namespace
        self.namespace = ""
        self.name = ""
        metadata = source.get("metadata")
        if isinstance(metadata, dict):
            self.name = metadata.get("name", "")
            self.namespace = metadata.get("namespace", "")

class Manifest:
    def __init__(self, source):
        util.validate(isinstance(source, dict), "Invalid source passed to Manifest init")

        self.spec = source

        self.tags = set()
        self.vars = {}

    def __str__(self):
        output = yaml.dump(self.spec, explicit_start=True)

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

        info = ManifestInfo(self.spec)

        new_vars["kmt_metadata_group"] = info.group
        new_vars["kmt_metadata_version"] = info.version
        new_vars["kmt_metadata_kind"] = info.kind
        new_vars["kmt_metadata_namespace"] = info.namespace
        new_vars["kmt_metadata_name"] = info.name
        new_vars["kmt_metadata_api_version"] = info.api_version

        return new_vars

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
        self.spec_util = SpecUtil(self.pipeline.common.environment, self.vars)

        self.skip_handler = False

class PipelineSupportHandler:
    def init(self, pipeline, pipeline_vars):
        util.validate(isinstance(pipeline, Pipeline), "Invalid pipeline passed to PipelineSupportHandler")

        self.pipeline = pipeline
        self.pipeline_vars = pipeline_vars
        self.spec_util = SpecUtil(self.pipeline.common.environment, self.pipeline_vars)

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

        spec_util = SpecUtil(environment=self.common.environment, template_vars={})
        logger.debug("Processing pipeline specification")

        # Config defaults - vars that can be overridden by the supplied vars
        # Don't template the vars - These will be templated when processed in a step
        config_defaults = spec_util.extract_property(pipeline_spec, "defaults", default={})
        config_defaults = spec_util.resolve(config_defaults, dict, template=False)
        util.validate(isinstance(config_defaults, dict), "Config 'defaults' is not a dictionary")

        # Config vars - vars that can't be overridden
        # Don't template the vars - These will be templated when processed in a step
        config_vars = spec_util.extract_property(pipeline_spec, "vars", default={})
        config_vars = spec_util.resolve(config_vars, dict, template=False)
        util.validate(isinstance(config_vars, dict), "Config 'vars' is not a dictionary")

        # Pipeline - list of the steps to run for this pipeline
        # Don't template the pipeline steps - These will be templated when they are executed
        config_pipeline = spec_util.extract_property(pipeline_spec, "pipeline", default=[])
        config_pipeline = spec_util.resolve(config_pipeline, list, template=False)
        util.validate(isinstance(config_pipeline, list), "Config 'pipeline' is not a list")
        self.pipeline_steps = config_pipeline

        # Accept manifests - whether to include incoming manifests in pipeline processing
        accept_manifests = spec_util.extract_property(pipeline_spec, "accept_manifests", default=False)
        accept_manifests = spec_util.resolve(accept_manifests, bool)
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

    def run(self):

        # Run the pipeline and capture any manifests, without resolving lookups
        manifests = self.run_no_resolve()

        for manifest in manifests:
            visited = set()
            item_list = [manifest.spec]

            while len(item_list) > 0:
                if len(item_list) > 10000:
                    raise PipelineRunException("Potential recursive loop while templating")

                current = item_list.pop()

                # Check if we've seen this object before
                if id(current) in visited:
                    continue

                # Save this to the visited list, so we don't revisit again, if there is a loop
                # in the origin object
                visited.add(id(current))

                if isinstance(current, dict):
                    for key in current:
                        if isinstance(current[key], (dict, list)):
                            item_list.append(current[key])
                        else:
                            current[key] = self._resolve_reference(current[key])
                elif isinstance(current, list):
                    index = 0
                    while index < len(current):
                        if isinstance(current[index], (dict, list)):
                            item_list.append(current[index])
                        else:
                            current[index] = self._resolve_reference(current[index])

                        index = index + 1
                else:
                    # Anything non dictionary or list should never have ended up in this list, so this
                    # is really an internal error
                    raise PipelineRunException(f"Invalid type for resolve in pipeline run: {type(current)}")

        return manifests

    def _resolve_reference(self, item):
        if isinstance(item, Lookup):
            manifest = util.lookup_manifest(self.manifests, pattern=item.pattern,
                group=item.group, version=item.version, kind=item.kind, namespace=item.namespace, multiple=False)

            metadata = manifest.get("metadata")
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
            step_vars["env"] = os.environ.copy()
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
            spec_util = SpecUtil(self.common.environment, state.vars)
            step_inner = spec_util.extract_property(step_outer, step_type, default={})
            step_inner = spec_util.resolve(step_inner, (dict, type(None)))
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

class SpecUtil:
    def __init__(self, environment, template_vars):
        util.validate(isinstance(environment, jinja2.Environment), "Invalid environment passed to SpecUtil ctor")
        util.validate(isinstance(template_vars, dict), "Invalid template vars passed to SpecUtil")

        self._environment = environment

        # Define the template vars
        # Don't copy the template vars, just reference it. These vars may be changed elsewhere
        # and shouldn't need to be reimported or altered within the SpecUtil
        self.vars = template_vars

    def new_scope(self, new_vars):
        util.validate(isinstance(new_vars, dict), "Invalid new_vars passed to new_scope")

        # Create a new set of vars
        working_vars = self.vars.copy()
        for key in new_vars:
            working_vars[key] = new_vars[key]

        new_spec_util = SpecUtil(self._environment, working_vars)

        return new_spec_util

    def template_if_string(self, val, var_override=None):
        if var_override is not None and not isinstance(var_override, dict):
            raise PipelineRunException("Invalid var override passed to template_if_string")

        # Determine which vars will be used for templating
        template_vars = self.vars
        if var_override is not None:
            template_vars = var_override

        if not isinstance(val, str):
            return val
        
        # Perform at most 'count' passes of the string
        count = 30
        current = val

        while count > 0:
            count = count - 1

            template = self._environment.from_string(current)
            output = template.render(template_vars)
            if output == current:
                return output

            current = output

        raise PipelineRunException(f"Reached recursion limit for string template '{val}'")

    def extract_property(self, spec, key, /, default=None, required=False):
        if not isinstance(spec, dict):
            raise PipelineRunException("Invalid spec passed to extract_property. Must be dict")

        if key not in spec:
            # Raise exception is the key isn't present, but required
            if required:
                raise KeyError(f'Missing key "{key}" in spec or value is null')

            # If the key is not present, return the default
            return default

        # Retrieve value
        val = spec.pop(key)

        return val

    def parse_bool(self, obj) -> bool:
        if obj is None:
            raise PipelineRunException("None value passed to parse_bool")

        if isinstance(obj, bool):
            return obj

        obj = str(obj)

        if obj.lower() in ["true", "1"]:
            return True

        if obj.lower() in ["false", "0"]:
            return False

        raise PipelineRunException(f"Unparseable value ({obj}) passed to parse_bool")

    def coerce_value(self, types, val):
        if types is None:
            # Nothing to do here
            return val

        if isinstance(types, type):
            types = (types,)

        util.validate(isinstance(types, tuple) and all(isinstance(x, type) for x in types),
            "Invalid types passed to coerce_value")

        parsed = None

        for type_item in types:
            # Return val if it is already the correct type
            if isinstance(val, type_item):
                return val

            if type_item == bool:
                try:
                    result = self.parse_bool(val)
                    return result
                except:
                    pass
            elif type_item == str:
                if val is None:
                    # Don't convert None to string. This is likely not wanted.
                    continue

                return str(val)

            # None of the above have worked, try parsing as yaml to see if it
            # becomes the correct type
            if isinstance(val, str):
                try:
                    if parsed is None:
                        parsed = yaml.safe_load(val)

                    if isinstance(parsed, type_item):
                        return parsed
                except yaml.YAMLError as e:
                    pass

        raise PipelineRunException(f"Could not convert value to target types: {types}")

    def resolve(self, value, types, /, template=True, recursive=False, var_override=None):
        util.validate(isinstance(template, bool), "Invalid value for template passed to resolve")

        # Template the value, if it is a string
        if template:
            if recursive:
                value = self.recursive_template(value, var_override=var_override)
            else:
                value = self.template_if_string(value, var_override=var_override)

        value = self.coerce_value(types, value)

        return value

    def recursive_template(self, item, var_override=None):

        # Potentially convert a string to a dict or list type
        item = self.template_if_string(item, var_override=var_override)

        # If the item is still not a dict or list, just return it
        # This may still have converted the item (e.g. string -> bool)
        if not isinstance(item, (dict, list)):
            return item

        visited = set()
        item_list = [item]

        while len(item_list) > 0:
            if len(item_list) > 10000:
                raise PipelineRunException("Potential recursive loop while templating")

            current = item_list.pop()

            # Check if we've seen this object before
            if id(current) in visited:
                continue

            # Save this to the visited list, so we don't revisit again, if there is a loop
            # in the origin object
            visited.add(id(current))

            if isinstance(current, dict):
                for key in current:
                    if isinstance(current[key], (dict, list)):
                        item_list.append(current[key])
                    else:
                        current[key] = self.template_if_string(current[key], var_override=var_override)
            elif isinstance(current, list):
                index = 0
                while index < len(current):
                    if isinstance(current[index], (dict, list)):
                        item_list.append(current[index])
                    else:
                        current[index] = self.template_if_string(current[index], var_override=var_override)

                    index = index + 1
            else:
                # Anything non dictionary or list should never have ended up in this list, so this
                # is really an internal error
                raise PipelineRunException(f"Invalid type for templating in recursive_template: {type(current)}")

        return item
