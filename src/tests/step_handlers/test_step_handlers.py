"""
Tests for kmt.step_handlers

These exercise each handler through a real core.Pipeline.run(), since the
handlers depend heavily on PipelineStepState/Templater wiring that would be
tedious (and less representative) to fake out by hand.
"""

import io

import pytest

import kmt.exception as exception


class TestStepHandlerImport:
    """
    Tests for step_handlers.StepHandlerImport
    """

    def test_imports_matching_files_in_sorted_order(self, make_pipeline):
        """import reads every file matching the glob(s) given, in sorted order"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifests/*.yaml"
            """,
            files={
                "manifests/b.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: cm-b
                """,
                "manifests/a.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: cm-a
                """,
            },
        )

        manifests = pipeline.run()

        assert [m.spec["metadata"]["name"] for m in manifests] == ["cm-a", "cm-b"]

    def test_templates_file_content_by_default(self, make_pipeline):
        """Imported file content is templated against pipeline vars before parsing"""
        pipeline = make_pipeline(
            """
            vars:
              cm_name: "templated-name"
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: "{{ cm_name }}"
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["name"] == "templated-name"

    def test_template_false_imports_raw_content(self, make_pipeline):
        """template: false imports the raw file content without jinja2 rendering"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
                  template: false
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: "literal-{{ not_a_var }}"
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["name"] == "literal-{{ not_a_var }}"

    def test_imports_multiple_documents_from_one_file(self, make_pipeline):
        """A single file can contain multiple yaml documents, each becoming a manifest"""
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
                    kind: ConfigMap
                    metadata:
                      name: cm2
                """,
            },
        )

        manifests = pipeline.run()

        assert sorted(m.spec["metadata"]["name"] for m in manifests) == ["cm1", "cm2"]


class TestStepHandlerStdin:
    """
    Tests for step_handlers.StepHandlerStdin
    """

    def test_reads_manifest_from_stdin(self, make_pipeline, monkeypatch):
        """
        Regression test: StepHandlerStdin used to call core.Manifest(doc, pipeline)
        positionally, but pipeline is a keyword-only argument, so every stdin step
        raised TypeError. It must now successfully create and return the manifest.
        """
        monkeypatch.setattr(
            "sys.stdin",
            io.StringIO("kind: ConfigMap\nmetadata:\n  name: from-stdin\n"),
        )

        pipeline = make_pipeline(
            """
            pipeline:
              - stdin:
                  template: false
            """
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["name"] == "from-stdin"

    def test_templates_stdin_content_by_default(self, make_pipeline, monkeypatch):
        """stdin content is templated against pipeline vars by default"""
        monkeypatch.setattr(
            "sys.stdin",
            io.StringIO("kind: ConfigMap\nmetadata:\n  name: '{{ cm_name }}'\n"),
        )

        pipeline = make_pipeline(
            """
            vars:
              cm_name: "templated-stdin-name"
            pipeline:
              - stdin: {}
            """
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["name"] == "templated-stdin-name"


class TestStepHandlerVars:
    """
    Tests for step_handlers.StepHandlerVars
    """

    def test_inline_vars_set_at_pipeline_scope_by_default(self, make_pipeline):
        """Inline vars default to pipeline scope and become available for later templating"""
        pipeline = make_pipeline(
            """
            pipeline:
              - vars:
                  inline:
                    greeting: "hello"
              - import:
                  files:
                    - "manifest.yaml"
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: "{{ greeting }}"
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["name"] == "hello"

    def test_manifest_scope_sets_local_vars_on_working_manifests(self, make_pipeline):
        """scope: manifest sets the var as a per-manifest local var instead of a pipeline var"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
              - vars:
                  scope: manifest
                  inline:
                    local_greeting: "hi-there"
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

        assert manifests[0].local_vars["local_greeting"] == "hi-there"

    def test_files_read_as_key_value_lines(self, make_pipeline):
        """files reads NAME=VALUE lines, one var per line"""
        pipeline = make_pipeline(
            """
            pipeline:
              - vars:
                  files:
                    - "extra.env"
              - import:
                  files:
                    - "manifest.yaml"
            """,
            files={
                "extra.env": "from_file=file-value\n",
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: "{{ from_file }}"
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["name"] == "file-value"

    def test_invalid_scope_raises(self, make_pipeline):
        """An unrecognised scope value is rejected"""
        pipeline = make_pipeline(
            """
            pipeline:
              - vars:
                  scope: "bogus"
            """
        )

        with pytest.raises(exception.ValidationException):
            pipeline.run()


class TestStepHandlerJsonPatch:
    """
    Tests for step_handlers.StepHandlerJsonPatch
    """

    def test_applies_patch_to_each_working_manifest(self, make_pipeline):
        """Each working manifest's spec is patched independently via jsonpatch"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
              - jsonpatch:
                  patches:
                    - op: add
                      path: /data/extra
                      value: "added"
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: cm1
                    data: {}
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["data"]["extra"] == "added"


class TestStepHandlerDelete:
    """
    Tests for step_handlers.StepHandlerDelete
    """

    def test_removes_working_manifests_from_the_pipeline(self, make_pipeline):
        """delete removes every working manifest from both the working list and the pipeline"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
              - delete: {}
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

        assert manifests == []

    def test_filter_can_scope_which_manifests_are_deleted(self, make_pipeline):
        """Combining 'filter' (a support handler) with delete removes only matching manifests"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifests.yaml"
              - delete: {}
                filter:
                  - "kmt_manifest.metadata.name == 'drop-me'"
            """,
            files={
                "manifests.yaml": """
                    ---
                    kind: ConfigMap
                    metadata:
                      name: keep-me
                    ---
                    kind: ConfigMap
                    metadata:
                      name: drop-me
                """,
            },
        )

        manifests = pipeline.run()

        assert [m.spec["metadata"]["name"] for m in manifests] == ["keep-me"]


class TestStepHandlerMetadata:
    """
    Tests for step_handlers.StepHandlerMetadata
    """

    def test_sets_name_namespace_annotations_and_labels(self, make_pipeline):
        """metadata can set name, namespace, annotations and labels in one step"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
              - metadata:
                  name: "renamed"
                  namespace: "new-ns"
                  annotations:
                    team: "platform"
                  labels:
                    app: "sample"
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: original
                """,
            },
        )

        manifests = pipeline.run()
        info = manifests[0].get_info()

        assert info["name"] == "renamed"
        assert info["namespace"] == "new-ns"
        assert info["annotations"]["team"] == "platform"
        assert info["labels"]["app"] == "sample"

    def test_omitted_fields_are_left_unchanged(self, make_pipeline):
        """A metadata step with only some fields set leaves the others alone"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
              - metadata:
                  namespace: "new-ns"
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: original
                """,
            },
        )

        manifests = pipeline.run()
        info = manifests[0].get_info()

        assert info["name"] == "original"
        assert info["namespace"] == "new-ns"


class TestStepHandlerPipeline:
    """
    Tests for step_handlers.StepHandlerPipeline
    """

    def test_runs_a_nested_pipeline_and_merges_its_manifests(self, make_pipeline):
        """A 'pipeline' step runs another pipeline dir and merges its output manifests back in"""
        pipeline = make_pipeline(
            """
            pipeline:
              - pipeline:
                  path: "nested"
            """,
            files={
                "nested/config.yaml": """
                    pipeline:
                      - import:
                          files:
                            - "manifest.yaml"
                """,
                "nested/manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: from-nested
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["name"] == "from-nested"

    def test_pass_vars_forwards_selected_pipeline_vars(self, make_pipeline):
        """pass_vars forwards named pipeline vars (optionally renamed) in to the nested pipeline"""
        pipeline = make_pipeline(
            """
            vars:
              source_var: "forwarded-value"
            pipeline:
              - pipeline:
                  path: "nested"
                  pass_vars:
                    - "target_var=source_var"
            """,
            files={
                "nested/config.yaml": """
                    accept_vars:
                      - target_var
                    pipeline:
                      - import:
                          files:
                            - "manifest.yaml"
                """,
                "nested/manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: "{{ target_var }}"
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["name"] == "forwarded-value"

    def test_pass_manifests_moves_working_manifests_to_nested_pipeline(self, make_pipeline):
        """pass_manifests: true removes the working manifests from this pipeline and hands
        them to the nested one, which is responsible for returning them (or not)"""
        pipeline = make_pipeline(
            """
            pipeline:
              - import:
                  files:
                    - "manifest.yaml"
              - pipeline:
                  path: "nested"
                  pass_manifests: true
            """,
            files={
                "manifest.yaml": """
                    kind: ConfigMap
                    metadata:
                      name: cm1
                """,
                "nested/config.yaml": """
                    accept_manifests: true
                    pipeline:
                      - metadata:
                          annotations:
                            touched-by-nested: "true"
                """,
            },
        )

        manifests = pipeline.run()

        assert manifests[0].spec["metadata"]["annotations"]["touched-by-nested"] == "true"
