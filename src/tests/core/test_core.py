"""
Tests for kmt.core
"""

import jinja2
import pytest

import kmt.core as core
import kmt.exception as exception


class TestManifestInit:
    """
    Tests for core.Manifest construction
    """

    def test_requires_metadata(self, make_pipeline):
        """A manifest with no metadata at all is rejected"""
        pipeline = make_pipeline("pipeline: []\n")

        with pytest.raises(exception.KMTManifestException):
            core.Manifest({"kind": "ConfigMap"}, pipeline=pipeline)

    def test_requires_metadata_name(self, make_pipeline):
        """A manifest with metadata but no name is rejected"""
        pipeline = make_pipeline("pipeline: []\n")

        with pytest.raises(exception.KMTManifestException):
            core.Manifest({"kind": "ConfigMap", "metadata": {}}, pipeline=pipeline)

    def test_rejects_empty_name(self, make_pipeline):
        """An empty string name is not a valid name"""
        pipeline = make_pipeline("pipeline: []\n")

        with pytest.raises(exception.KMTManifestException):
            core.Manifest({"kind": "ConfigMap", "metadata": {"name": ""}}, pipeline=pipeline)

    def test_default_alias_matches_name(self, make_pipeline, make_manifest):
        """When no kmt/alias annotation is given, it defaults to the manifest's name"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})

        assert manifest.spec["metadata"]["annotations"]["kmt/alias"] == "cm1"

    def test_explicit_alias_is_preserved(self, make_pipeline, make_manifest):
        """An explicitly supplied kmt/alias annotation is not overwritten"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {
            "kind": "ConfigMap",
            "metadata": {"name": "cm1", "annotations": {"kmt/alias": "custom"}},
        })

        assert manifest.spec["metadata"]["annotations"]["kmt/alias"] == "custom"


class TestManifestValidate:
    """
    Tests for core.Manifest.validate
    """

    def test_valid_manifest_passes(self, make_pipeline, make_manifest):
        """A well-formed manifest passes validation without raising"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})

        manifest.validate()

    def test_missing_kind_fails(self, make_pipeline, make_manifest):
        """A manifest missing 'kind' fails validation"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.spec.pop("kind")

        with pytest.raises(exception.KMTManifestException):
            manifest.validate()

    def test_invalid_annotations_type_fails(self, make_pipeline, make_manifest):
        """Non-dict annotations fail validation"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        manifest.spec["metadata"]["annotations"] = "not-a-dict"

        with pytest.raises(exception.KMTManifestException):
            manifest.validate()


class TestManifestGetInfo:
    """
    Tests for core.Manifest.get_info
    """

    def test_splits_group_and_version_from_api_version(self, make_pipeline, make_manifest):
        """A grouped apiVersion (group/version) splits in to separate group and version"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "dep1"},
        })

        info = manifest.get_info()

        assert info["group"] == "apps"
        assert info["version"] == "v1"

    def test_core_api_version_has_no_group(self, make_pipeline, make_manifest):
        """A core apiVersion (no slash) has only a version, no group"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cm1"},
        })

        info = manifest.get_info()

        assert info["group"] is None
        assert info["version"] == "v1"

    def test_missing_api_version_uses_default_value(self, make_pipeline, make_manifest):
        """A missing apiVersion falls back to the given default_value for every related field"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})

        info = manifest.get_info(default_value="")

        assert info["group"] == ""
        assert info["version"] == ""
        assert info["api_version"] == ""

    def test_includes_alias_and_namespace(self, make_pipeline, make_manifest):
        """get_info surfaces alias and namespace alongside the core identity fields"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {
            "kind": "ConfigMap",
            "metadata": {"name": "cm1", "namespace": "ns1"},
        })

        info = manifest.get_info()

        assert info["namespace"] == "ns1"
        assert info["alias"] == "cm1"
        assert info["manifest"] is manifest


class TestManifestAnnotationsAndLabels:
    """
    Tests for core.Manifest.get_annotations/get_labels
    """

    def test_get_annotations_creates_dict_if_absent(self, make_pipeline, make_manifest):
        """get_annotations materialises an empty dict if annotations were absent"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        # Manifest.__init__ already adds the default kmt/alias annotation
        manifest.spec["metadata"].pop("annotations")

        annotations = manifest.get_annotations()

        assert annotations == {}
        assert manifest.spec["metadata"]["annotations"] is annotations

    def test_get_labels_creates_dict_if_absent(self, make_pipeline, make_manifest):
        """get_labels materialises an empty dict if labels were absent"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})

        labels = manifest.get_labels()

        assert labels == {}
        assert manifest.spec["metadata"]["labels"] is labels


class TestManifestGetTemplater:
    """
    Tests for core.Manifest.get_templater
    """

    def test_templater_can_see_manifest_metadata_vars(self, make_pipeline, make_manifest):
        """The manifest templater exposes kmt_metadata_* vars derived from refresh_metadata"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cm1", "namespace": "ns1"},
        })

        templater = manifest.get_templater()

        assert templater.template_if_string("{{ kmt_metadata_name }}") == "cm1"
        assert templater.template_if_string("{{ kmt_metadata_namespace }}") == "ns1"

    def test_templater_can_see_kmt_manifest(self, make_pipeline, make_manifest):
        """The manifest templater exposes the manifest's own spec as kmt_manifest"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})

        templater = manifest.get_templater()

        assert templater.template_if_string("{{ kmt_manifest.kind }}") == "ConfigMap"


class TestPipelineInit:
    """
    Tests for core.Pipeline construction/config parsing
    """

    def test_requires_config_yaml(self, tmp_path):
        """A pipeline directory with no config.yaml is rejected"""
        with pytest.raises(exception.PipelineRunException):
            core.Pipeline(str(tmp_path))

    def test_requires_existing_directory(self, tmp_path):
        """A non-existent configdir is rejected"""
        with pytest.raises(exception.PipelineRunException):
            core.Pipeline(str(tmp_path / "does-not-exist"))

    def test_rejects_unknown_top_level_keys(self, make_pipeline):
        """An unrecognised top-level key in config.yaml is rejected"""
        with pytest.raises(exception.ValidationException):
            make_pipeline("bogus_key: 1\n")

    def test_defaults_can_be_overridden_by_pipeline_vars(self, make_pipeline):
        """A 'defaults' var can be overridden by a var passed in to the pipeline"""
        pipeline = make_pipeline(
            """
            defaults:
              greeting: "hello"
            pipeline: []
            """,
            pipeline_vars={"greeting": "overridden"},
        )

        assert pipeline.vars["greeting"] == "overridden"

    def test_vars_cannot_be_overridden_by_pipeline_vars(self, make_pipeline):
        """A 'vars' entry always wins over anything passed in from the caller"""
        pipeline = make_pipeline(
            """
            vars:
              greeting: "fixed"
            accept_vars:
              - greeting
            pipeline: []
            """,
            pipeline_vars={"greeting": "attempted-override"},
        )

        assert pipeline.vars["greeting"] == "fixed"

    def test_disallowed_pipeline_var_raises(self, make_pipeline):
        """Passing a var that isn't in accept_vars or defaults is rejected"""
        with pytest.raises(exception.KMTConfigException):
            make_pipeline("pipeline: []\n", pipeline_vars={"not_declared": "value"})

    def test_accept_manifests_moves_input_manifests_in_to_pipeline(self, make_pipeline):
        """accept_manifests: true makes incoming manifests subject to this pipeline's own steps"""
        pipeline = make_pipeline(
            "accept_manifests: true\npipeline: []\n",
            manifests=[{"kind": "ConfigMap", "metadata": {"name": "cm1"}}],
        )

        assert len(pipeline.manifests) == 1
        assert pipeline._input_manifests == []  # pylint: disable=protected-access

    def test_accept_manifests_false_keeps_input_manifests_separate(self, make_pipeline):
        """accept_manifests: false (the default) passes incoming manifests straight through"""
        pipeline = make_pipeline(
            "pipeline: []\n",
            manifests=[{"kind": "ConfigMap", "metadata": {"name": "cm1"}}],
        )

        assert pipeline.manifests == []
        assert len(pipeline._input_manifests) == 1  # pylint: disable=protected-access

    def test_run_returns_input_manifests_plus_generated(self, make_pipeline):
        """run() returns pipeline-generated manifests plus any untouched input manifests"""
        pipeline = make_pipeline(
            "pipeline: []\n",
            manifests=[{"kind": "ConfigMap", "metadata": {"name": "cm1"}}],
        )

        result = pipeline.run()

        assert len(result) == 1
        assert result[0].spec["metadata"]["name"] == "cm1"


class TestPipelineGetTemplater:
    """
    Tests for core.Pipeline.get_templater
    """

    def test_templater_can_see_pipeline_vars(self, make_pipeline):
        """The pipeline templater exposes the pipeline's own vars"""
        pipeline = make_pipeline(
            """
            vars:
              greeting: "hello"
            pipeline: []
            """
        )

        templater = pipeline.get_templater()

        assert templater.template_if_string("{{ greeting }}") == "hello"


class TestTemplater:
    """
    Tests for core.Templater
    """

    def _environment(self):
        return jinja2.Environment(undefined=jinja2.StrictUndefined, keep_trailing_newline=True)

    def test_template_if_string_renders_strings(self):
        """A string value is rendered through jinja2"""
        templater = core.Templater(self._environment(), {"name": "world"})

        assert templater.template_if_string("hello {{ name }}") == "hello world"

    def test_template_if_string_passes_through_non_strings(self):
        """Non-string values are returned unmodified"""
        templater = core.Templater(self._environment(), {})

        assert templater.template_if_string(123) == 123

    def test_resolve_without_template_skips_rendering(self):
        """template=False skips jinja2 rendering entirely"""
        templater = core.Templater(self._environment(), {})

        assert templater.resolve("{{ name }}", template=False) == "{{ name }}"

    def test_resolve_coerces_to_requested_type(self):
        """resolve() can template and then coerce the templated result"""
        templater = core.Templater(self._environment(), {"flag": "true"})

        assert templater.resolve("{{ flag }}", bool) is True

    def test_resolve_recursive_templates_nested_strings(self):
        """recursive=True templates every string leaf in a nested structure"""
        templater = core.Templater(self._environment(), {"name": "world"})

        result = templater.resolve({"greeting": "hello {{ name }}"}, dict, recursive=True)

        assert result == {"greeting": "hello world"}

    def test_strict_undefined_raises_on_missing_var(self):
        """The jinja2 environment uses StrictUndefined, so unknown vars raise"""
        templater = core.Templater(self._environment(), {})

        with pytest.raises(jinja2.exceptions.UndefinedError):
            templater.template_if_string("{{ missing }}")


class TestCommon:
    """
    Tests for core.Common
    """

    def test_add_handlers_rejects_non_dict(self):
        """add_handlers requires a dict argument"""
        common = core.Common()

        with pytest.raises(exception.ValidationException):
            common.add_handlers(["not", "a", "dict"])

    def test_add_filters_registers_in_environment(self):
        """add_filters makes the filter callable from the jinja2 environment"""
        common = core.Common()

        common.add_filters({"shout": lambda x: f"{x}!"})

        assert common.environment.filters["shout"]("hi") == "hi!"

    def test_add_filters_rejects_non_callable(self):
        """Filter values must be callable (or None)"""
        common = core.Common()

        with pytest.raises(exception.ValidationException):
            common.add_filters({"bad": "not-callable"})
