"""
Tests for kmt.step_support
"""

import pytest

import kmt.core as core
import kmt.exception as exception
import kmt.step_support as step_support


def _state_with_manifest(pipeline, manifest):
    return core.PipelineStepState(pipeline=pipeline, working_manifests=[manifest])


class TestStepSupportWhen:
    """
    Tests for step_support.StepSupportWhen
    """

    def test_when_false_skips_the_handler(self, make_pipeline, make_manifest):
        """A false 'when' condition flags the step to be skipped entirely"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportWhen()
        handler.init(state)
        handler.extract({"when": ["false"]})
        handler.pre()

        assert state.skip_handler is True

    def test_when_true_runs_the_handler(self, make_pipeline, make_manifest):
        """A true 'when' condition leaves the step to run normally"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportWhen()
        handler.init(state)
        handler.extract({"when": ["true"]})
        handler.pre()

        assert state.skip_handler is False

    def test_filter_removes_non_matching_manifests(self, make_pipeline, make_manifest):
        """A 'filter' condition removes manifests that don't satisfy it from working_manifests"""
        pipeline = make_pipeline("pipeline: []\n")
        keep = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "keep"}})
        drop = make_manifest(pipeline, {"kind": "Secret", "metadata": {"name": "drop"}})
        state = core.PipelineStepState(pipeline=pipeline, working_manifests=[keep, drop])

        handler = step_support.StepSupportWhen()
        handler.init(state)
        handler.extract({"filter": ["kmt_manifest.kind == 'ConfigMap'"]})
        handler.pre()

        assert state.working_manifests == [keep]


class TestStepSupportTags:
    """
    Tests for step_support.StepSupportTags
    """

    def test_match_any_tags_keeps_manifest_with_one_matching_tag(
        self, make_pipeline, make_manifest
    ):
        """match_any_tags only needs one of the listed tags to be present"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.tags = {"a"}
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportTags()
        handler.init(state)
        handler.extract({"match_any_tags": ["a", "b"]})
        handler.pre()

        assert state.working_manifests == [manifest]

    def test_match_any_tags_removes_manifest_with_no_matching_tag(
        self, make_pipeline, make_manifest
    ):
        """match_any_tags removes a manifest that has none of the listed tags"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.tags = {"c"}
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportTags()
        handler.init(state)
        handler.extract({"match_any_tags": ["a", "b"]})
        handler.pre()

        assert state.working_manifests == []

    def test_match_all_tags_keeps_manifest_with_every_tag(self, make_pipeline, make_manifest):
        """match_all_tags keeps a manifest that has every one of the listed tags"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.tags = {"a", "b"}
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportTags()
        handler.init(state)
        handler.extract({"match_all_tags": ["a", "b"]})
        handler.pre()

        assert state.working_manifests == [manifest]

    def test_match_all_tags_missing_multiple_tags_does_not_crash(
        self, make_pipeline, make_manifest
    ):
        """
        Regression test: a manifest missing more than one of the required match_all_tags
        used to call working_manifests.remove() once per missing tag, raising ValueError
        the second time since the manifest was already removed. It must be removed exactly
        once and the pipeline must keep running.
        """
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.tags = set()  # missing both "a" and "b"
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportTags()
        handler.init(state)
        handler.extract({"match_all_tags": ["a", "b"]})
        handler.pre()

        assert state.working_manifests == []

    def test_exclude_tags_removes_manifest_with_any_excluded_tag(
        self, make_pipeline, make_manifest
    ):
        """exclude_tags removes a manifest that has at least one of the excluded tags"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.tags = {"a"}
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportTags()
        handler.init(state)
        handler.extract({"exclude_tags": ["a", "b"]})
        handler.pre()

        assert state.working_manifests == []

    def test_exclude_tags_with_multiple_present_does_not_crash(self, make_pipeline, make_manifest):
        """
        Regression test: the same double-remove issue as match_all_tags, but for a
        manifest carrying more than one of the excluded tags.
        """
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.tags = {"a", "b"}  # both excluded tags present
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportTags()
        handler.init(state)
        handler.extract({"exclude_tags": ["a", "b"]})
        handler.pre()

        assert state.working_manifests == []

    def test_manifest_removed_by_match_all_tags_is_not_reconsidered_for_exclude(
        self, make_pipeline, make_manifest
    ):
        """
        Once match_all_tags removes a manifest, the exclude_tags check must not try to
        remove it again (it would otherwise raise ValueError: x not in list).
        """
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.tags = {"z"}  # fails match_all_tags, and also carries an excluded tag
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportTags()
        handler.init(state)
        handler.extract({"match_all_tags": ["a"], "exclude_tags": ["z"]})
        handler.pre()

        assert state.working_manifests == []

    def test_apply_tags_adds_tags_to_remaining_manifests_in_post(
        self, make_pipeline, make_manifest
    ):
        """apply_tags is only applied in post(), to whatever manifests survived pre()"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportTags()
        handler.init(state)
        handler.extract({"apply_tags": ["new-tag"]})
        handler.pre()
        handler.post()

        assert "new-tag" in manifest.tags


class TestStepSupportMetadata:
    """
    Tests for step_support.StepSupportMetadata
    """

    def test_match_kind_keeps_matching_kind(self, make_pipeline, make_manifest):
        """match_kind keeps manifests whose kind is in the given list (case-insensitive)"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportMetadata()
        handler.init(state)
        handler.extract({"match_kind": ["configmap"]})
        handler.pre()

        assert state.working_manifests == [manifest]

    def test_exclude_kind_removes_matching_kind(self, make_pipeline, make_manifest):
        """exclude_kind removes manifests whose kind is in the given list"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "Secret", "metadata": {"name": "sec1"}})
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportMetadata()
        handler.init(state)
        handler.extract({"exclude_kind": "Secret"})
        handler.pre()

        assert state.working_manifests == []

    def test_match_namespace_is_a_regex(self, make_pipeline, make_manifest):
        """match_namespace is applied as a regex search, not an exact match"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {
            "kind": "ConfigMap",
            "metadata": {"name": "cm1", "namespace": "team-a-prod"},
        })
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportMetadata()
        handler.init(state)
        handler.extract({"match_namespace": "^team-a"})
        handler.pre()

        assert state.working_manifests == [manifest]

    def test_match_name_excludes_non_matching_name(self, make_pipeline, make_manifest):
        """match_name removes manifests whose name doesn't satisfy the regex"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "other"}})
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportMetadata()
        handler.init(state)
        handler.extract({"match_name": "^cm"})
        handler.pre()

        assert state.working_manifests == []


class TestStepSupportValidate:
    """
    Tests for step_support.StepSupportValidate
    """

    def test_post_validates_working_manifests(self, make_pipeline, make_manifest):
        """post() calls validate() on every surviving working manifest"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.spec.pop("kind")  # make the manifest invalid
        state = _state_with_manifest(pipeline, manifest)

        handler = step_support.StepSupportValidate()
        handler.init(state)
        handler.extract({})
        handler.pre()

        with pytest.raises(exception.KMTManifestException):
            handler.post()
