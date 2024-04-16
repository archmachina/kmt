import logging
import yaml

from . import util
from . import types

logger = logging.getLogger(__name__)

class PipelineSupportOrdering(types.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):

        def _get_metadata_str(manifest):
            keys = [
                "group",
                "version",
                "kind",
                "namespace",
                "name",
            ]

            info = util.extract_manifest_info(manifest, default_value="")

            return ":".join([info.get(key, "") for key in keys])

        for manifest in self.pipeline.manifests:
            logger.debug(f"metadata: {_get_metadata_str(manifest)}")

        # Don't need a particular ordering, just consistency in output
        # to allow for easy diff comparison
        self.pipeline.manifests = sorted(self.pipeline.manifests, key=lambda x: _get_metadata_str(x))

types.default_pipeline_support_handlers.append(PipelineSupportOrdering)
