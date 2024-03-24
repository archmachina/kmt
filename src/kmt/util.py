
import jinja2
import logging

from .exception import *

logger = logging.getLogger(__name__)

def template_if_string(val, mapping):
    if val is not None and isinstance(val, str):
        try:
            environment = jinja2.Environment()

            template = environment.from_string(val)
            return template.render(mapping)
        except KeyError as e:
            raise PipelineRunException(f"Missing key in template substitution: {e}") from e

    return val

def parse_bool(obj) -> bool:
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
