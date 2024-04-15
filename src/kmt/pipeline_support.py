import logging
import yaml

from . import util
from . import types

logger = logging.getLogger(__name__)

class PipelineSupportOrdering(types.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):
        keys = [
            "kmt_metadata_group",
            "kmt_metadata_version",
            "kmt_metadata_kind",
            "kmt_metadata_namespace",
            "kmt_metadata_name",
        ]

        for manifest in self.pipeline.manifests:
            data = tuple([manifest.vars.get(key, "") for key in keys])
            logger.debug(f"metadata: {data}")

        # Don't need a particular ordering, just consistency in output
        # to allow for easy diff comparison
        self.pipeline.manifests = sorted(self.pipeline.manifests, key=lambda x: "".join([
            x.vars.get(key, "") for key in keys
        ]))

types.default_pipeline_support_handlers.append(PipelineSupportOrdering)
