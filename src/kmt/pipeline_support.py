import logging
import yaml

from . import util
from . import types

logger = logging.getLogger(__name__)

class PipelineSupportSort(types.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):
        keys = [
            "metadata_group",
            "metadata_version",
            "metadata_kind",
            "metadata_namespace",
            "metadata_name",
        ]

        for block in self.pipeline.blocks:
            util.refresh_metadata(block)
            data = tuple([block.vars.get(key, "") for key in keys])
            logger.debug(f"metadata: {data}")

        # Don't need a particular ordering, just consistency in output
        # to allow for easy diff comparison
        self.pipeline.blocks = sorted(self.pipeline.blocks, key=lambda x: "".join([
            x.vars.get(key, "") for key in keys
        ]))

class PipelineSupportYamlFormat(types.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):
        for block in self.pipeline.blocks:
            try:
                manifest = yaml.safe_load(block.text)
                block.text = yaml.dump(manifest, explicit_start=True)
            except yaml.YAMLError as e:
                # Ignore parser errors/Ignore non-yaml blocks
                pass

types.default_pipeline_support_handlers.append(PipelineSupportSort)
types.default_pipeline_support_handlers.append(PipelineSupportYamlFormat)
