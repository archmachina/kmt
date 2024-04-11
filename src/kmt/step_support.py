import logging
import copy
import yaml
import re

from . import util
from . import types

logger = logging.getLogger(__name__)

class StepSupportRefreshHash(types.StepSupportHandler):
    """
    """
    def extract(self, step_def):
        pass

    def pre(self):
        pass

    def post(self):
        for manifest in self.state.working_manifests:
            manifest.refresh_hash()

        logger.debug(f"RefreshHash: document short sum: {manifest.vars['shortsum']}")

class StepSupportWhen(types.StepSupportHandler):
    def extract(self, step_def):
        # When condition
        self.when = self.state.spec_util.extract_property(step_def, "when", default=[])

        # Filter condition
        self.filter = self.state.spec_util.extract_property(step_def, "filter", default=[])

    def pre(self):
        working_manifests = self.state.working_manifests.copy()
        spec_util = self.state.spec_util

        when = spec_util.resolve(self.when, (list, str))
        if isinstance(when, str):
            when = [when]

        if len(when) > 0:
            for condition in when:
                result = spec_util.resolve("{{" + condition + "}}", bool)
                if not result:
                    self.state.skip_handler = True
                    return

        for manifest in working_manifests:
            manifest_vars = manifest.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(manifest_vars)

            filter = spec_util.resolve(self.filter, (list, str))
            if isinstance(filter, str):
                filter = [filter]

            if len(filter) > 0:
                for condition in filter:
                    result = spec_util.resolve("{{" + condition + "}}", bool)
                    if not result:
                        self.state.working_manifests.remove(manifest)
                        break

    def post(self):
        pass

class StepSupportTags(types.StepSupportHandler):
    def extract(self, step_def):
        # Extract match any tags
        self.match_any_tags = self.state.spec_util.extract_property(step_def, "match_any_tags", default=[])

        # Extract match all tags
        self.match_all_tags = self.state.spec_util.extract_property(step_def, "match_all_tags", default=[])

        # Extract exclude tags
        self.exclude_tags = self.state.spec_util.extract_property(step_def, "exclude_tags", default=[])

        # Apply tags
        self.apply_tags = self.state.spec_util.extract_property(step_def, "apply_tags", default=[])

    def pre(self):
        working_manifests = self.state.working_manifests.copy()

        for manifest in working_manifests:
            manifest_vars = manifest.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(manifest_vars)

            match_any_tags = spec_util.resolve(self.match_any_tags, list)
            match_any_tags = set([spec_util.resolve(x, str) for x in match_any_tags])
            if len(match_any_tags) > 0:
                # If there are any 'match_any_tags', then at least one of them has to match with the document
                if len(match_any_tags.intersection(manifest.tags)) == 0:
                    self.state.working_manifests.remove(manifest)
                    continue

            match_all_tags = spec_util.resolve(self.match_all_tags, list)
            match_all_tags = set([spec_util.resolve(x, str) for x in match_all_tags])
            if len(match_all_tags) > 0:
                # If there are any 'match_all_tags', then all of those tags must match the document
                for tag in match_all_tags:
                    if tag not in manifest.tags:
                        self.state.working_manifests.remove(manifest)
                        continue

            exclude_tags = spec_util.resolve(self.exclude_tags, list)
            exclude_tags = set([spec_util.resolve(x, str) for x in exclude_tags])
            if len(exclude_tags) > 0:
                # If there are any exclude tags and any are present in the manifest, it isn't a match
                for tag in exclude_tags:
                    if tag in manifest.tags:
                        self.state.working_manifests.remove(manifest)
                        continue

    def post(self):

        for manifest in self.state.working_manifests:
            manifest_vars = manifest.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(manifest_vars)

            apply_tags = spec_util.resolve(self.apply_tags, list)
            for tag in apply_tags:
                manifest.tags.add(spec_util.resolve(tag, str))

class StepSupportMetadata(types.StepSupportHandler):
    def extract(self, step_def):
        self.match_group = self.state.spec_util.extract_property(step_def, "match_group")

        self.match_version = self.state.spec_util.extract_property(step_def, "match_version")

        self.match_kind = self.state.spec_util.extract_property(step_def, "match_kind")

        self.match_namespace = self.state.spec_util.extract_property(step_def, "match_namespace")

        self.match_name = self.state.spec_util.extract_property(step_def, "match_name")

    def pre(self):
        working_manifests = self.state.working_manifests.copy()

        for manifest in working_manifests:
            manifest_vars = manifest.create_scoped_vars(self.state.vars)
            spec_util = self.state.spec_util.new_scope(manifest_vars)

            group = manifest_vars["kmt_metadata_group"]
            version = manifest_vars["kmt_metadata_version"]
            kind = manifest_vars["kmt_metadata_kind"]
            namespace = manifest_vars["kmt_metadata_namespace"]
            name = manifest_vars["kmt_metadata_name"]

            # k8s group match
            match_group = spec_util.resolve(self.match_group, (str, type(None)))
            if match_group is not None and not re.search(match_group, group):
                self.state.working_manifests.remove(manifest)
                continue

            # k8s version match
            match_version = spec_util.resolve(self.match_version, (str, type(None)))
            if match_version is not None and not re.search(match_version, version):
                self.state.working_manifests.remove(manifest)
                continue

            # k8s kind match
            match_kind = spec_util.resolve(self.match_kind, (list, str, type(None)))
            if match_kind is not None:
                if isinstance(match_kind, str):
                    match_kind = [match_kind]

                if not any((x.lower() == kind.lower()) for x in match_kind):
                    self.state.working_manifests.remove(manifest)
                    continue

            # k8s namespace match
            match_namespace = spec_util.resolve(self.match_namespace, (str, type(None)))
            if match_namespace is not None and not re.search(match_namespace, namespace):
                self.state.working_manifests.remove(manifest)
                continue

            # k8s name match
            match_name = spec_util.resolve(self.match_name, (str, type(None)))
            if match_name is not None and not re.search(match_name, name):
                self.state.working_manifests.remove(manifest)
                continue

    def post(self):
        pass

types.default_step_support_handlers.append(StepSupportMetadata)
types.default_step_support_handlers.append(StepSupportTags)
types.default_step_support_handlers.append(StepSupportWhen)
# types.default_step_support_handlers.append(StepSupportSum)
