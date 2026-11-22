"""
Tests for kmt.j2support

Filters/globals are exercised through real pipeline templating rather than
calling the @pass_context functions directly, since most of them depend on
a live jinja2 context carrying kmt_pipeline/kmt_manifest.
"""

import pytest

import kmt.exception as exception
import kmt.util as util


class TestFilters:
    """
    Tests for the hash_string/b64encode/b64decode jinja2 filters
    """

    def test_hash_string_filter(self, make_pipeline):
        """The hash_string filter calls through to util.hash_string"""
        pipeline = make_pipeline("pipeline: []\n")
        templater = pipeline.get_templater()

        result = templater.template_if_string("{{ 'hello' | hash_string }}")

        assert result == util.hash_string("hello")

    def test_b64encode_and_b64decode_round_trip(self, make_pipeline):
        """b64encode followed by b64decode returns the original value"""
        pipeline = make_pipeline("pipeline: []\n")
        templater = pipeline.get_templater()

        encoded = templater.template_if_string("{{ 'hello world' | b64encode }}")
        assert encoded == "aGVsbG8gd29ybGQ="

        decoded = templater.template_if_string("{{ '" + encoded + "' | b64decode }}")
        assert decoded == "hello world"


class TestGlobalEnv:
    """
    Tests for environment variable access in templates

    Every templater (Pipeline.get_templater and Manifest.get_templater) also
    injects a builtin "env" template *variable* (a plain dict of os.environ),
    which shadows the "env" jinja2 *global function* registered in j2support.
    As a result, calling env(...) as a function always fails with
    "'dict' object is not callable" -- only the "env.NAME"/"env['NAME']"
    variable-access form is actually reachable in practice.
    """

    def test_env_var_exposes_named_variable(self, make_pipeline, monkeypatch):
        """The builtin 'env' template var exposes a single environment variable by key"""
        monkeypatch.setenv("KMT_TEST_VAR", "test-value")
        pipeline = make_pipeline("pipeline: []\n")
        templater = pipeline.get_templater()

        assert templater.template_if_string("{{ env.KMT_TEST_VAR }}") == "test-value"

    def test_env_global_function_is_shadowed_by_the_env_var(self, make_pipeline, monkeypatch):
        """
        Regression-style documentation test: the env() global is always shadowed
        by the builtin env var, so calling it raises TypeError rather than
        returning anything.
        """
        monkeypatch.setenv("KMT_TEST_VAR", "test-value")
        pipeline = make_pipeline("pipeline: []\n")
        templater = pipeline.get_templater()

        with pytest.raises(TypeError):
            templater.template_if_string("{{ env('KMT_TEST_VAR') }}")


class TestGlobalFail:
    """
    Tests for the fail() jinja2 global
    """

    def test_fail_raises_kmt_template_exception(self, make_pipeline):
        """fail(message) always raises KMTTemplateException from inside a template"""
        pipeline = make_pipeline("pipeline: []\n")
        templater = pipeline.get_templater()

        with pytest.raises(exception.KMTTemplateException):
            templater.template_if_string("{{ fail('boom') }}")


class TestGlobalLookupManifestName:
    """
    Tests for the lookup_manifest_name() jinja2 global
    """

    def test_lookup_manifest_name_from_a_manifest_template(self, make_pipeline):
        """
        lookup_manifest_name resolves to the matching manifest's name. The ConfigMap must be
        imported in an earlier step than the Deployment referencing it: import templates a
        whole file's raw text (and so resolves any lookups in it) before parsing it in to
        manifests, so a lookup can only see manifests that already exist from prior steps.
        """
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "configmap.yaml"
              - import:
                  files:
                    - "deployment.yaml"
            """,
            files={
                "configmap.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: cm1
                """,
                "deployment.yaml": """
                    kind: Deployment
                    metadata:
                      name: dep1
                    spec:
                      configName: "{{ lookup_manifest_name(kind='ConfigMap') }}"
                """,
            },
        )

        manifests = pipeline.run()

        deployment = next(m for m in manifests if m.spec["kind"] == "Deployment")
        assert deployment.spec["spec"]["configName"] == "cm1"


class TestGlobalHashSelf:
    """
    Tests for the hash_self() jinja2 global
    """

    def test_hash_self_requires_a_current_manifest(self, make_pipeline):
        """hash_self() raises KMTTemplateException when there is no current manifest"""
        pipeline = make_pipeline("pipeline: []\n")
        templater = pipeline.get_templater()

        with pytest.raises(exception.KMTTemplateException):
            templater.template_if_string("{{ hash_self() }}")

    def test_hash_self_matches_hash_manifest_of_own_spec(self, make_pipeline, make_manifest):
        """hash_self() returns the same value as util.hash_manifest() of the manifest's own spec"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})

        templater = manifest.get_templater()
        result = templater.template_if_string("{{ hash_self() }}")

        assert result == util.hash_manifest(manifest.spec)


class TestGlobalIncludeFileStr:
    """
    Tests for the include_file_str() jinja2 global
    """

    def test_includes_raw_file_content_as_a_json_escaped_string(self, make_pipeline, tmp_path):
        """include_file_str reads a file and returns its content json-escaped"""
        pipeline = make_pipeline("pipeline: []\n")
        templater = pipeline.get_templater()

        target = tmp_path / "included.txt"
        target.write_text('line with "quotes"\n', encoding="utf-8")

        result = templater.template_if_string(
            "{{ include_file_str('" + str(target) + "') }}"
        )

        assert result == 'line with \\"quotes\\"\\n'

    def test_can_template_the_included_content(self, make_pipeline, tmp_path):
        """include_file_str(..., template=True) templates the file's content before escaping"""
        pipeline = make_pipeline(
            """
            vars:
              name: "world"
            pipeline: []
            """
        )
        templater = pipeline.get_templater()

        target = tmp_path / "included.txt"
        target.write_text("hello {{ name }}", encoding="utf-8")

        result = templater.template_if_string(
            "{{ include_file_str('" + str(target) + "', template=True) }}"
        )

        assert result == "hello world"
