"""
Tests for kmt.pipeline_support

These run through a real core.Pipeline.run(), since the pipeline support
handlers only do anything meaningful as post-processing on a completed,
root-level pipeline run.
"""


class TestPipelineSupportOrdering:
    """
    Tests for pipeline_support.PipelineSupportOrdering
    """

    def test_namespaces_are_sorted_first(self, make_pipeline):
        """Namespace manifests are always ordered ahead of everything else"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifests.yaml"
            """,
            files={
                "manifests.yaml": """
                    ---
                    kind: ConfigMap
                    metadata:
                      name: cm1
                    ---
                    kind: Namespace
                    metadata:
                      name: ns1
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["kind"] == "Namespace"

    def test_configmaps_and_secrets_are_sorted_before_other_kinds(self, make_pipeline):
        """ConfigMap/Secret manifests are ordered ahead of other kinds (e.g. Deployments
        that may reference them), though still after Namespace manifests"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifests.yaml"
            """,
            files={
                "manifests.yaml": """
                    ---
                    kind: ConfigMap
                    metadata:
                      name: cm1
                    ---
                    kind: Deployment
                    metadata:
                      name: dep1
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["kind"] == "ConfigMap"


class TestPipelineSupportRenameHash:
    """
    Tests for pipeline_support.PipelineSupportRenameHash
    """

    def test_rename_hash_true_appends_a_short10_hash_suffix(self, make_pipeline):
        """kmt/rename-hash: true appends a short10 content hash to the manifest's name"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: cm1
                      annotations:
                        kmt/rename-hash: "true"
                    data:
                      a: "1"
                """,
            },
        )

        manifests = pipeline.run()

        name = manifests[0].spec["metadata"]["name"]
        assert name.startswith("cm1-")
        assert len(name) == len("cm1-") + 10

    def test_rename_hash_false_leaves_name_unchanged(self, make_pipeline):
        """kmt/rename-hash: false is a no-op"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: cm1
                      annotations:
                        kmt/rename-hash: "false"
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["name"] == "cm1"

    def test_renamed_configmap_reference_is_updated_in_referencing_deployment(self, make_pipeline):
        """A Deployment's configMapRef is updated to the ConfigMap's new, hash-suffixed name"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifests.yaml"
            """,
            files={
                "manifests.yaml": """
                    ---
                    kind: ConfigMap
                    metadata:
                      name: cm1
                      namespace: ns1
                      annotations:
                        kmt/rename-hash: "true"
                    data:
                      a: "1"
                    ---
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: dep1
                      namespace: ns1
                    spec:
                      template:
                        spec:
                          containers:
                            - envFrom:
                                - configMapRef:
                                    name: cm1
                """,
            },
        )

        manifests = pipeline.run()

        configmap = next(m for m in manifests if m.spec["kind"] == "ConfigMap")
        deployment = next(m for m in manifests if m.spec["kind"] == "Deployment")

        new_name = configmap.spec["metadata"]["name"]
        ref_name = deployment.spec["spec"]["template"]["spec"]["containers"][0][
            "envFrom"
        ][0]["configMapRef"]["name"]

        assert new_name != "cm1"
        assert ref_name == new_name

    def test_deployment_referencing_renamed_configmap_and_plain_secret_does_not_crash(
        self, make_pipeline
    ):
        """
        Regression test: a Deployment referencing both a hash-renamed ConfigMap and a
        plain (never-renamed) Secret in the same namespace used to raise KeyError, because
        _update_hash_refs only recorded a namespace -> kind entry for kinds that actually
        got renamed, then unconditionally indexed in to it for every pattern-matched kind.
        """
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifests.yaml"
            """,
            files={
                "manifests.yaml": """
                    ---
                    kind: ConfigMap
                    metadata:
                      name: cm1
                      namespace: ns1
                      annotations:
                        kmt/rename-hash: "true"
                    data:
                      a: "1"
                    ---
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: dep1
                      namespace: ns1
                    spec:
                      template:
                        spec:
                          containers:
                            - envFrom:
                                - configMapRef:
                                    name: cm1
                                - secretRef:
                                    name: some-unrelated-secret
                """,
            },
        )

        manifests = pipeline.run()

        deployment = next(m for m in manifests if m.spec["kind"] == "Deployment")
        env_from = deployment.spec["spec"]["template"]["spec"]["containers"][0]["envFrom"]

        # The Secret reference was never renamed, so it must be left untouched
        assert env_from[1]["secretRef"]["name"] == "some-unrelated-secret"


class TestPipelineSupportResolveTags:
    """
    Tests for pipeline_support.PipelineSupportResolveTags
    """

    def test_resolves_name_lookup_tag(self, make_pipeline):
        """A !name tag is resolved to the matching manifest's name"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifests.yaml"
            """,
            files={
                "manifests.yaml": """
                    ---
                    kind: ConfigMap
                    metadata:
                      name: cm1
                    ---
                    kind: Deployment
                    metadata:
                      name: dep1
                    spec:
                      configName: !name
                        kind: ConfigMap
                """,
            },
        )

        manifests = pipeline.run()

        deployment = next(m for m in manifests if m.spec["kind"] == "Deployment")
        assert deployment.spec["spec"]["configName"] == "cm1"


class TestPipelineSupportCleanup:
    """
    Tests for pipeline_support.PipelineSupportCleanup
    """

    def test_removes_internal_kmt_annotations(self, make_pipeline):
        """kmt/alias and other internal annotations are stripped from the final output"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: cm1
                """,
            },
        )

        manifests = pipeline.run()

        metadata = manifests[0].spec["metadata"]
        assert "annotations" not in metadata

    def test_leaves_user_annotations_in_place(self, make_pipeline):
        """Non-kmt annotations survive cleanup"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: cm1
                      annotations:
                        team: platform
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["annotations"] == {"team": "platform"}
