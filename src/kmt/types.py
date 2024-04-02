import os
import yaml
import jinja2
import inspect
import copy
import logging

from .util import validate, parse_bool
from .exception import PipelineRunException

logger = logging.getLogger(__name__)

# Default handlers, support handlers and filters. These can be amended elsewhere
# and apply to newly created Common objects
default_handlers = {}
default_support_handlers = []
default_filters = {}

# Define a representer for pyyaml to format multiline strings as block quotes
def str_representer(dumper, data):
    if isinstance(data, str) and '\n' in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

yaml.add_representer(str, str_representer)

class TextBlock:
    def __init__(self, text):
        validate(isinstance(text, str), "Invalid text passed to TextBlock init")

        self.text = text

        self.tags = set()
        self.meta = {}

class Common:
    def __init__(self):
        self.environment = jinja2.Environment()
        self.handlers = copy.copy(default_handlers)
        self.support_handlers = copy.copy(default_support_handlers)
        for filter_name in default_filters:
            self.environment.filters[filter_name] = default_filters[filter_name]

    def add_handlers(self, handlers):
        validate(isinstance(handlers, dict), "Invalid handlers passed to add_handlers")
        validate((all(x is None or (inspect.isclass(x) and issubclass(x, Handler))) for x in handlers.values()), "Invalid handlers passed to add_handlers")

        for key in handlers:
            self.handlers[key] = handlers[key]

    def add_support_handlers(self, handlers):
        validate(isinstance(handlers, list), "Invalid handlers passed to add_support_handlers")
        validate((all(inspect.isclass(x) and issubclass(x, SupportHandler)) for x in handlers), "Invalid handlers passed to add_support_handlers")

        for handler in handlers:
            if handler not in self.support_handlers:
                self.support_handlers.append(handler)

    def add_filters(self, filters):
        validate(isinstance(filters, dict), "Invalid filters passed to add_filters")
        validate(all((callable(x) or x is None) for x in filters.values()), "Invalid filters passed to add_filters")

        for key in filters:
            self.environment.filters[key] = filters[key]

class PipelineStepState:
    def __init__(self, pipeline, step_vars, working_blocks):
        validate(isinstance(pipeline, Pipeline) or pipeline is None, "Invalid pipeline passed to PipelineStepState")
        validate(isinstance(step_vars, dict), "Invalid step vars passed to PipelineStepState")
        validate(isinstance(working_blocks, list) and all(isinstance(x, TextBlock) for x in working_blocks),
            "Invalid working blocks passed to PipelineStepState")

        self.pipeline = pipeline
        self.vars = step_vars
        self.working_blocks = working_blocks

class SupportHandler:
    def init(self, state):
        validate(isinstance(state, PipelineStepState), "Invalid step state passed to SupportHandler")

        self.state = state

    def parse(self, step):
        raise PipelineRunException("parse undefined in SupportHandler")

    def pre(self):
        raise PipelineRunException("pre undefined in SupportHandler")

    def post(self):
        raise PipelineRunException("post undefined in SupportHandler")

class Handler:
    def init(self, state):
        validate(isinstance(state, PipelineStepState), "Invalid step state passed to Handler")

        self.state = state

    def parse(self, step):
        raise PipelineRunException("parse undefined in Handler")

    def run(self):
        raise PipelineRunException("run undefined in Handler")

class Pipeline:
    def __init__(self, configdir, common=None, pipeline_vars=None, blocks=None):

        if pipeline_vars is None:
            pipeline_vars = {}

        if blocks is None:
            blocks = []
        
        if common is None:
            common = Common()

        validate(isinstance(configdir, str) and configdir != "", "Invalid configdir passed to Pipeline init")
        validate(isinstance(pipeline_vars, dict), "Invalid pipeline_vars passed to Pipeline init")
        validate(isinstance(blocks, list) and all(isinstance(x, TextBlock) for x in blocks),
            "Invalid blocks passed to Pipeline init")
        validate(isinstance(common, Common), "Invalid common object passed to Pipeline init")

        self.common = common
        self._input_blocks = blocks
        self._blocks = []
        self._vars = {}

        #
        # Read and parse configuration file as yaml
        #

        # Open config file from configdir
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
        config_defaults = spec_util.extract_property(pipeline_spec, "defaults", default={}, template=False)
        validate(isinstance(config_defaults, dict), "Config 'defaults' is not a dictionary")

        # Config vars - vars that can't be overridden
        # Don't template the vars - These will be templated when processed in a step
        config_vars = spec_util.extract_property(pipeline_spec, "vars", default={}, template=False)
        validate(isinstance(config_vars, dict), "Config 'vars' is not a dictionary")

        # Pipeline - list of the steps to run for this pipeline
        # Don't template the pipeline steps - These will be templated when they are executed
        config_pipeline = spec_util.extract_property(pipeline_spec, "pipeline", default=[], template=False)
        validate(isinstance(config_pipeline, list), "Config 'pipeline' is not a list")
        self.pipeline_steps = config_pipeline

        # Accept blocks - whether to include incoming blocks in pipeline processing
        accept_blocks = spec_util.extract_property(pipeline_spec, "accept_blocks", default=False,
            type=bool, template=False)
        validate(isinstance(accept_blocks, bool), "Invalid type for accept_blocks")

        # If accept_blocks is true, we'll apply the pipeline steps to the incoming blocks as well
        if accept_blocks:
            self._blocks = self._input_blocks
            self._input_blocks = []

        # Make sure there are no other properties left on the pipeline spec
        validate(len(pipeline_spec.keys()) == 0, f"Unknown properties on pipeline specification: {pipeline_spec.keys()}")

        #
        # Merge variables in to the pipeline variables in order
        #

        # Merge defaults in to the pipeline vars
        for key in config_defaults:
            self._vars[key] = config_defaults[key]

        # Merge supplied vars in to the pipeline vars
        for key in pipeline_vars:
            self._vars[key] = pipeline_vars[key]

        # Merge 'vars' in to the pipeline vars last as these take preference
        for key in config_vars:
            self._vars[key] = config_vars[key]

    def run(self):
        # Process each of the steps in this pipeline
        for step_outer in self.pipeline_steps:
            logger.debug(f"Processing step with specification: {step_outer}")

            # Create a common state used by each support handler and handler
            step_vars = copy.deepcopy(self._vars)
            step_vars["env"] = os.environ.copy()

            state = PipelineStepState(pipeline=self, step_vars=step_vars,
                working_blocks=self._blocks.copy())

            # Initialise each support handler based on the step definition
            # This is the outer step definition, not the arguments to the handler
            support_handlers = [x() for x in self.common.support_handlers]
            for support in support_handlers:
                support.init(state)
                support.parse(step_outer)
            
            # Once the support handlers have initialised, there should be a single
            # key representing the handler type
            if len(step_outer.keys()) < 1:
                raise PipelineRunException("Missing step type on the step definition")
            
            if len(step_outer.keys()) > 1:
                raise PipelineRunException("Multiple keys remaining on the step definition - cannot determine type")

            # Extract the step type
            step_type = [x for x in step_outer][0]
            if not isinstance(step_type, str) or step_type == "":
                raise PipelineRunException("Invalid step type on step definition")

            logger.debug(f"Step type: {step_type}")

            # Extract the step config and allow it to be templated with vars defined up to this
            # point
            spec_util = SpecUtil(self.common.environment, state.vars)
            step_inner = spec_util.extract_property(step_outer, step_type, types=dict, default={})

            # Create the handler object to process the handler config
            if step_type not in self.common.handlers:
                raise PipelineRunException(f"Missing handler for step type: {step_type}")
            
            handler = self.common.handlers[step_type]()
            handler.init(state)
            handler.parse(step_inner)

            # Make sure there are no remaining properties that the handler wasn't looking for
            if len(step_inner.keys()) > 0:
                raise PipelineRunException(f"Unexpected properties for handler config: {step_inner.keys()}")

            # Run pre for any support handlers
            logger.debug("Running pre support handlers")
            for support_handler in support_handlers:
                os.chdir(self.configdir)
                support_handler.pre()

            # Run the main handler
            logger.debug("Running handler")
            os.chdir(self.configdir)
            handler.run()

            # Run post for any support handlers
            logger.debug("Running post support handlers")
            for support_handler in support_handlers:
                os.chdir(self.configdir)
                support_handler.post()

class SpecUtil:
    def __init__(self, environment, template_vars):
        validate(isinstance(environment, jinja2.Environment), "Invalid environment passed to SpecUtil ctor")
        validate(isinstance(template_vars, dict), "Invalid template vars passed to SpecUtil")

        self._environment = environment

        # Define the template vars
        # Don't copy the template vars, just reference it. These vars may be changed elsewhere
        # and shouldn't need to be reimported or altered within the SpecUtil
        self.vars = template_vars

    def template_if_string(self, val, var_override=None):
        if var_override is not None and not isinstance(var_override, dict):
            raise PipelineRunException("Invalid var override passed to template_if_string")

        # Determine which vars will be used for templating
        template_vars = self.vars
        if var_override is not None:
            template_vars = var_override

        if isinstance(val, str):
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

        return val

    def extract_property(self, spec, key, /, types=None, default=None, required=False, var_override=None, template=True):
        if not isinstance(spec, dict):
            raise PipelineRunException("Invalid spec passed to extract_property. Must be dict")

        if var_override is not None and not isinstance(var_override, dict):
            raise PipelineRunException("Invalid var_override passed to extract_property")

        if key not in spec:
            # Raise exception is the key isn't present, but required
            if required:
                raise KeyError(f'Missing key "{key}" in spec or value is null')

            # If the key is not present, return the default
            return default

        # Retrieve value
        val = spec.pop(key)

        # Recursive template of the object and sub elements, if it is a dict or list
        if template:
            val = self.recursive_template(val, var_override=var_override)

        val = self.coerce_property(types, val)

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

    def coerce_property(self, types, val):
        if types is None:
            # Nothing to do here
            return val

        if isinstance(types, type):
            types = (types,)

        if not isinstance(types, tuple) and all(isinstance(x, type) for x in types):
            raise PipelineRunException("Invalid types passed to coerce_property")

        # Don't bother with conversion if val is None. Also, conversion to str will just give 'None'
        # which is probably not what is wanted
        if val is None:
            return None

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

        return val

    def recursive_template(self, item, var_override=None):

        # Only perform recursive templating if the input object
        # is a dictionary or list
        if not isinstance(item, (dict, list)):
            return self.template_if_string(item, var_override=var_override)

        visited = set()
        item_list = [item]

        while len(item_list) > 0:
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
