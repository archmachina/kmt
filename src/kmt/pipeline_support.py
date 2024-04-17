import logging

import kmt.util as util
import kmt.core as core
import kmt.yaml_types as yaml_types

logger = logging.getLogger(__name__)

class PipelineSupportOrdering(core.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):

        # Don't bother sorting unless we're on a top level pipeline
        if not self.pipeline.root_pipeline:
            return

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

class PipelineSupportRoot(core.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):

        # Only run when we're operating on a root/top level pipeline
        if not self.pipeline.root_pipeline:
            return

        annotations_list = [
            "kmt/original-name",
            "kmt/rename-hash"
        ]

        # Rename any objects that require a hash suffix
        for manifest in self.pipeline.manifests:
            metadata = manifest.spec.get("metadata")
            if not isinstance(metadata, dict):
                continue

            annotations = metadata.get("annotations")
            if not isinstance(annotations, dict):
                continue

            if "kmt/rename-hash" not in annotations:
                continue

            original_name = annotations.get("kmt/original-name")
            if original_name is None:
                continue

            hash = util.hash_manifest(manifest.spec, hash_type="short10")

            new_name = f"{original_name}-{hash}"

            metadata["name"] = new_name

        # Call _resolve_reference for all nodes in the manifest to see if replacement
        # is required
        for manifest in self.pipeline.manifests:
            util.walk_object(manifest.spec, lambda x: self._resolve_reference(manifest, x), update=True)

        # Remove kmt specific annotations
        for manifest in self.pipeline.manifests:
            metadata = manifest.spec.get("metadata")
            if not isinstance(metadata, dict):
                continue

            annotations = metadata.get("annotations")
            if not isinstance(annotations, dict):
                continue

            for key in annotations_list:
                if key in annotations:
                    annotations.pop(key)

    def _resolve_reference(self, current_manifest, item):
        if isinstance(item, yaml_types.YamlTag):
            return item.resolve(current_manifest)

        return item

core.default_pipeline_support_handlers.append(PipelineSupportOrdering)
core.default_pipeline_support_handlers.append(PipelineSupportRoot)
