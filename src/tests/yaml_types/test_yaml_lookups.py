"""
Tests for the resolve() behaviour of kmt.yaml_types lookup tags
"""

import pytest

import kmt.exception as exception
import kmt.util as util
import kmt.yaml_types as yaml_types


@pytest.fixture(name="pipeline_with_manifests")
def fixture_pipeline_with_manifests(make_pipeline, make_manifest):
    """A pipeline with a ConfigMap, a Secret and a Deployment to look up against"""
    pipeline = make_pipeline("pipeline: []\n")

    configmap = make_manifest(pipeline, {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm1",
            "namespace": "ns1",
            "annotations": {"kmt/alias": "my-config"},
        },
        "data": {"a": "1"},
    })
    secret = make_manifest(pipeline, {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "sec1",
            "namespace": "ns1",
            "annotations": {"kmt/alias": "my-secret"},
        },
        "data": {},
    })
    deployment = make_manifest(pipeline, {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "dep1", "namespace": "ns1"},
        "spec": {},
    })

    pipeline.manifests = [configmap, secret, deployment]

    return pipeline, configmap, secret, deployment


class TestLookup:
    """
    Tests for yaml_types.Lookup
    """

    def test_resolve_returns_matching_manifest_spec(self, pipeline_with_manifests):
        """resolve() returns the full spec dict of the single matching manifest"""
        _pipeline, configmap, _secret, deployment = pipeline_with_manifests

        lookup = yaml_types.Lookup({"kind": "ConfigMap"})

        assert lookup.resolve(deployment) is configmap.spec

    def test_resolve_from_pipeline_scope_uses_no_current_namespace(self, pipeline_with_manifests):
        """Resolving from the pipeline itself (rather than a manifest) has no current namespace"""
        pipeline, configmap, _secret, _deployment = pipeline_with_manifests

        lookup = yaml_types.Lookup({"kind": "ConfigMap", "namespace": "ns1"})

        assert lookup.resolve(pipeline) is configmap.spec

    def test_rejects_non_dict_spec(self):
        """Lookup requires a dict specification"""
        with pytest.raises(exception.ValidationException):
            yaml_types.Lookup("not-a-dict")

    def test_rejects_invalid_search_keys(self):
        """Lookup validates its spec against the allowed find_manifests keys"""
        with pytest.raises(exception.PipelineRunException):
            yaml_types.Lookup({"bogus": "value"})


class TestLookupName:
    """
    Tests for yaml_types.LookupName
    """

    def test_resolve_returns_matching_manifest_name(self, pipeline_with_manifests):
        """resolve() returns the metadata.name of the single matching manifest"""
        _pipeline, _configmap, secret, deployment = pipeline_with_manifests

        lookup = yaml_types.LookupName({"kind": "Secret"})

        assert lookup.resolve(deployment) == secret.spec["metadata"]["name"]

    def test_resolve_raises_on_invalid_metadata(self, make_pipeline, make_manifest):
        """A matching manifest with non-dict metadata is a KMTManifestException, not a KeyError"""
        pipeline = make_pipeline("pipeline: []\n")
        broken = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "placeholder"}})
        # Corrupt the metadata after construction, bypassing Manifest's own validation
        broken.spec["metadata"] = "not-a-dict"
        pipeline.manifests = [broken]

        lookup = yaml_types.LookupName({"kind": "ConfigMap"})

        with pytest.raises(exception.KMTManifestException):
            lookup.resolve(pipeline)


class TestLookupHash:
    """
    Tests for yaml_types.LookupHash
    """

    def test_resolve_returns_hash_of_matching_manifest(self, pipeline_with_manifests):
        """resolve() returns util.hash_manifest() of the matching manifest's spec"""
        _pipeline, configmap, _secret, deployment = pipeline_with_manifests

        lookup = yaml_types.LookupHash({"kind": "ConfigMap"})

        assert lookup.resolve(deployment) == util.hash_manifest(configmap.spec, hash_type="sha1")

    def test_hash_type_is_extracted_from_spec_and_defaults_to_sha1(self):
        """hash_type is read out of the spec, defaulting to sha1 when not given"""
        lookup = yaml_types.LookupHash({"kind": "ConfigMap", "hash_type": "short10"})
        assert lookup.spec["hash_type"] == "short10"

        default_lookup = yaml_types.LookupHash({"kind": "ConfigMap"})
        assert default_lookup.spec["hash_type"] == "sha1"


class TestLookupConfigMapAndSecret:
    """
    Tests for yaml_types.LookupConfigMap and yaml_types.LookupSecret
    """

    def test_configmap_resolves_to_renamed_name_via_alias(self, pipeline_with_manifests):
        """LookupConfigMap finds the manifest by its kmt/alias annotation, not its literal name"""
        _pipeline, configmap, _secret, deployment = pipeline_with_manifests

        lookup = yaml_types.LookupConfigMap("my-config")

        assert lookup.resolve(deployment) == configmap.spec["metadata"]["name"]

    def test_secret_resolves_to_renamed_name_via_alias(self, pipeline_with_manifests):
        """LookupSecret finds the manifest by its kmt/alias annotation, not its literal name"""
        _pipeline, _configmap, secret, deployment = pipeline_with_manifests

        lookup = yaml_types.LookupSecret("my-secret")

        assert lookup.resolve(deployment) == secret.spec["metadata"]["name"]

    def test_configmap_raises_when_no_match(self, pipeline_with_manifests):
        """An alias with no matching ConfigMap raises KMTTemplateException"""
        _pipeline, _configmap, _secret, deployment = pipeline_with_manifests

        lookup = yaml_types.LookupConfigMap("does-not-exist")

        with pytest.raises(exception.KMTTemplateException):
            lookup.resolve(deployment)

    def test_secret_raises_when_multiple_matches(self, make_pipeline, make_manifest):
        """Multiple Secrets sharing the same alias raise KMTTemplateException"""
        pipeline = make_pipeline("pipeline: []\n")
        secret1 = make_manifest(pipeline, {
            "kind": "Secret",
            "metadata": {"name": "sec1", "annotations": {"kmt/alias": "shared"}},
        })
        secret2 = make_manifest(pipeline, {
            "kind": "Secret",
            "metadata": {"name": "sec2", "annotations": {"kmt/alias": "shared"}},
        })
        pipeline.manifests = [secret1, secret2]

        lookup = yaml_types.LookupSecret("shared")

        with pytest.raises(exception.KMTTemplateException):
            lookup.resolve(pipeline)

    def test_rejects_non_string_name(self):
        """LookupConfigMap/LookupSecret require a string name"""
        with pytest.raises(exception.ValidationException):
            yaml_types.LookupConfigMap(123)

        with pytest.raises(exception.ValidationException):
            yaml_types.LookupSecret(123)
