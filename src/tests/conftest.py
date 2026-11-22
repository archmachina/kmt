"""
Shared pytest fixtures for the kmt test suite
"""

import textwrap

import pytest

# Importing kmt.cli pulls in every step/support handler module, which is what
# registers them against core.default_handlers/default_step_support_handlers/
# default_pipeline_support_handlers at import time. Without this, a Pipeline
# built directly against kmt.core would have no handlers available to it.
import kmt.cli  # noqa: F401  pylint: disable=unused-import
import kmt.core as core


def write_pipeline_files(base_dir, config_yaml, files=None):
    """
    Writes a pipeline config.yaml (and any accompanying files) under base_dir
    """
    config_path = base_dir / "config.yaml"
    config_path.write_text(textwrap.dedent(config_yaml), encoding="utf-8")

    if files is not None:
        for relpath, content in files.items():
            file_path = base_dir / relpath
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(textwrap.dedent(content), encoding="utf-8")

    return base_dir


@pytest.fixture(name="make_pipeline")
def fixture_make_pipeline(tmp_path):
    """
    Returns a factory that writes a pipeline directory under tmp_path and
    constructs a core.Pipeline against it
    """

    def _make_pipeline(config_yaml, files=None, **kwargs):
        pipeline_dir = write_pipeline_files(tmp_path, config_yaml, files=files)
        return core.Pipeline(str(pipeline_dir), **kwargs)

    return _make_pipeline


@pytest.fixture(name="make_manifest")
def fixture_make_manifest():
    """
    Returns a factory that creates a Manifest belonging to the given pipeline
    """

    def _make_manifest(pipeline, spec):
        return core.Manifest(spec, pipeline=pipeline)

    return _make_manifest
