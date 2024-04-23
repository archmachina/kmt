import logging
import jsonpath_ng
import itertools

import kmt.util as util
import kmt.core as core
import kmt.yaml_types as yaml_types
import kmt.exception as exception

logger = logging.getLogger(__name__)

class PipelineSupportOrdering(core.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):

        # Don't bother sorting unless we're on a top level pipeline
        if not self.pipeline.root_pipeline:
            return

        working = self.pipeline.manifests
        working = sorted(working, key=lambda x: x.spec.get("name", ""))
        working = sorted(working, key=lambda x: x.spec.get("namespace", ""))
        working = sorted(working, key=lambda x: x.spec.get("kind", ""))
        working = sorted(working, key=lambda x: x.spec.get("version", ""))
        working = sorted(working, key=lambda x: x.spec.get("group", ""))
        working = sorted(
            working,
            key=lambda x: x.spec.get("kind", "").casefold() != "configmap" and x.spec.get("kind", "").casefold() != "secret"
        )
        working = sorted(working, key=lambda x: x.spec.get("kind", "").casefold() != "namespace" )

        self.pipeline.manifests = working

        # Display sorted order
        def _get_metadata_str(manifest):
            keys = [
                "group",
                "version",
                "kind",
                "namespace",
                "name",
            ]

            info = manifest.get_info(default_value="")

            return ":".join([info.get(key, "") for key in keys])

        logger.debug("Sorted order:")
        for manifest in self.pipeline.manifests:
            logger.debug(f"metadata: {_get_metadata_str(manifest)}")

class PipelineSupportRenameHash(core.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):

        # Only run when we're operating on a root/top level pipeline
        if not self.pipeline.root_pipeline:
            return

        # Perform rename for any manifests that require hash rename
        self._rename_hash()

        # Update any manifest references to manifests that have been hash renamed
        self._update_hash_refs()

    def _rename_hash(self):

        # Rename any objects that require a hash suffix
        for manifest in self.pipeline.manifests:
            info = manifest.get_info()
            annotations = manifest.get_annotations()

            if "kmt/rename-hash" not in annotations:
                continue

            rename_hash = util.coerce_value((bool, str), annotations.pop("kmt/rename-hash"))

            # Skip if the annotation is null
            if rename_hash is None:
                continue

            # Check if it's a bool type value
            if isinstance(rename_hash, bool):
                if not rename_hash:
                    continue

                rename_hash = "short10"

            # Skip on empty string
            if rename_hash == "":
                continue

            # Not bool and not empty, so check for allowed types
            allowed_suffix = ["short10", "short8"]
            if rename_hash not in allowed_suffix:
                raise exception.KMTManifestException(f"Disallowed type ({rename_hash}) for kmt/rename-hash. Allowed values: {allowed_suffix}, or bool-type expression")

            annotations["kmt/pre-hash-name"] = info["name"]
            hash = util.hash_manifest(manifest.spec, hash_type=rename_hash)

            # Rename based on the current manifest name.
            # Don't use alias as this is only used to find the manifest. The name may have
            # been altered and we should preserve that and just append the hash.
            manifest.spec["metadata"]["name"] = f"{info['name']}-{hash}"

    def _update_hash_refs(self):

        # Create two mappings. One for namespace -> kind -> pre-hash-name and another
        # for namespace -> kind

        rename_map = {}
        kind_map = {}

        for manifest in self.pipeline.manifests:
            info = manifest.get_info()

            annotations = info["annotations"]
            namespace = info["namespace"]
            kind = info["kind"]

            # Add the manifest to the kind map
            if namespace not in kind_map:
                kind_map[namespace] = {}

            if kind not in kind_map[namespace]:
                kind_map[namespace][kind] = []

            kind_map[namespace][kind].append(manifest)

            # If there is no pre-hash-name, don't need to add to the rename map
            if "kmt/pre-hash-name" not in annotations:
                continue

            pre_hash_name = annotations["kmt/pre-hash-name"]
            if not isinstance(pre_hash_name, str) or pre_hash_name == "":
                raise exception.KMTInternalException("Invalid pre-hash-name on manifest")

            # Add the manifest to the rename map
            if namespace not in rename_map:
                rename_map[namespace] = {}

            if kind not in rename_map[namespace]:
                rename_map[namespace][kind] = {}

            if pre_hash_name in rename_map[namespace][kind]:
                raise exception.KMTManifestException("Multiple manifests with the same pre hash name {pre_hash_name} in namespace {Namespace} with kind {kind}")

            rename_map[namespace][kind][pre_hash_name] = info["name"]

        # We now have two mappings, allowing access to resources by namespace and kind and access to
        # a list of pre hash names by namespace and kind

        pattern_mapping = {
            "Job": {
                "Secret": [
                    "spec.template.spec.initContainers[*].env[*].valueFrom.secretKeyRef.name",
                    "spec.template.spec.containers[*].env[*].valueFrom.secretKeyRef.name",
                    "spec.template.spec.initContainers[*].envFrom[*].secretRef.name",
                    "spec.template.spec.containers[*].envFrom[*].secretRef.name",
                    "spec.template.spec.volumes[*].secret.secretName"
                ],
                "ConfigMap": [
                    "spec.template.spec.initContainers[*].env[*].valueFrom.configMapKeyRef.name",
                    "spec.template.spec.containers[*].env[*].valueFrom.configMapKeyRef.name",
                    "spec.template.spec.initContainers[*].envFrom[*].configMapRef.name",
                    "spec.template.spec.containers[*].envFrom[*].configMapRef.name",
                    "spec.template.spec.volumes[*].configMap.name",
                ]
            },
            "Deployment": {
                "Secret": [
                    "spec.template.spec.initContainers[*].env[*].valueFrom.secretKeyRef.name",
                    "spec.template.spec.containers[*].env[*].valueFrom.secretKeyRef.name",
                    "spec.template.spec.initContainers[*].envFrom[*].secretRef.name",
                    "spec.template.spec.containers[*].envFrom[*].secretRef.name",
                    "spec.template.spec.volumes[*].secret.secretName"
                ],
                "ConfigMap": [
                    "spec.template.spec.initContainers[*].env[*].valueFrom.configMapKeyRef.name",
                    "spec.template.spec.containers[*].env[*].valueFrom.configMapKeyRef.name",
                    "spec.template.spec.initContainers[*].envFrom[*].configMapRef.name",
                    "spec.template.spec.containers[*].envFrom[*].configMapRef.name",
                    "spec.template.spec.volumes[*].configMap.name",
                ]
            },
            "StatefulSet": {
                "Secret": [
                    "spec.template.spec.initContainers[*].env[*].valueFrom.secretKeyRef.name",
                    "spec.template.spec.containers[*].env[*].valueFrom.secretKeyRef.name",
                    "spec.template.spec.initContainers[*].envFrom[*].secretRef.name",
                    "spec.template.spec.containers[*].envFrom[*].secretRef.name",
                    "spec.template.spec.volumes[*].secret.secretName"
                ],
                "ConfigMap": [
                    "spec.template.spec.initContainers[*].env[*].valueFrom.configMapKeyRef.name",
                    "spec.template.spec.containers[*].env[*].valueFrom.configMapKeyRef.name",
                    "spec.template.spec.initContainers[*].envFrom[*].configMapRef.name",
                    "spec.template.spec.containers[*].envFrom[*].configMapRef.name",
                    "spec.template.spec.volumes[*].configMap.name",
                ]
            },
            "DaemonSet": {
                "Secret": [
                    "spec.template.spec.initContainers[*].env[*].valueFrom.secretKeyRef.name",
                    "spec.template.spec.containers[*].env[*].valueFrom.secretKeyRef.name",
                    "spec.template.spec.initContainers[*].envFrom[*].secretRef.name",
                    "spec.template.spec.containers[*].envFrom[*].secretRef.name",
                    "spec.template.spec.volumes[*].secret.secretName"
                ],
                "ConfigMap": [
                    "spec.template.spec.initContainers[*].env[*].valueFrom.configMapKeyRef.name",
                    "spec.template.spec.containers[*].env[*].valueFrom.configMapKeyRef.name",
                    "spec.template.spec.initContainers[*].envFrom[*].configMapRef.name",
                    "spec.template.spec.containers[*].envFrom[*].configMapRef.name",
                    "spec.template.spec.volumes[*].configMap.name",
                ]
            }
        }

        for manifest in self.pipeline.manifests:
            info = manifest.get_info()

            source_namespace = info["namespace"]
            source_kind = info["kind"]

            # Check if there is a pattern mapping for this source kind
            if source_kind not in pattern_mapping:
                continue

            for target_kind in pattern_mapping[source_kind]:
                # target kind will be Secret and/or ConfigMap
                for pattern_str in pattern_mapping[source_kind][target_kind]:
                    pattern = jsonpath_ng.parse(pattern_str)

                    for pattern_match in pattern.find(manifest.spec):
                        match_value = pattern_match.value

                        if not isinstance(match_value, str) or match_value == "":
                            continue

                        if source_namespace not in rename_map:
                            continue

                        if match_value in rename_map[source_namespace][target_kind]:
                            pattern_match.full_path.update(
                                manifest.spec,
                                rename_map[source_namespace][target_kind][match_value]
                            )


class PipelineSupportResolveTags(core.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):

        # Only run when we're operating on a root/top level pipeline
        if not self.pipeline.root_pipeline:
            return

        def _resolve_reference(current_manifest, item):
            if isinstance(item, yaml_types.YamlTag):
                return item.resolve(current_manifest)

            return item

        # Call _resolve_reference for all nodes in the manifest to see if replacement
        # is required
        for manifest in self.pipeline.manifests:
            util.walk_object(manifest.spec, lambda x: _resolve_reference(manifest, x), update=True)

class PipelineSupportCleanup(core.PipelineSupportHandler):
    def pre(self):
        pass

    def post(self):

        # Only run when we're operating on a root/top level pipeline
        if not self.pipeline.root_pipeline:
            return

        # List of annotations that we should make sure aren't present on the
        # output manifests
        annotations_list = [
            "kmt/alias",
            "kmt/pre-hash-name",
            "kmt/rename-hash"
        ]

        # Remove kmt specific annotations
        for manifest in self.pipeline.manifests:
            annotations = manifest.get_annotations()

            for key in annotations_list:
                if key in annotations:
                    annotations.pop(key)

            metadata = manifest.get_metadata()

            if "annotations" in metadata and len(metadata["annotations"]) < 1:
                metadata.pop("annotations")

            if "labels" in metadata and len(metadata["labels"]) < 1:
                metadata.pop("labels")

core.default_pipeline_support_handlers.append(PipelineSupportOrdering)
core.default_pipeline_support_handlers.append(PipelineSupportRenameHash)
core.default_pipeline_support_handlers.append(PipelineSupportResolveTags)
core.default_pipeline_support_handlers.append(PipelineSupportCleanup)
