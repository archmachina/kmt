#!/usr/bin/env python3

import os

import build_tool
from build_tool import action, bash, Util

REGISTRY_IMAGE = "archmachina/kmt"
REGISTRY_HOST = "docker.io"
REGISTRY_USER = "archmachina"

# BuildSession - tracks/parses the version for this build
bs = build_tool.BuildSession([f"0.1.0-rolling+{Util.git_sha()}"])

# PythonSession - builds/publishes src/kmt as a PyPI package
pys = build_tool.PythonSession()

# ContainerSession - builds/pushes the kmt image under src/
cs = build_tool.ContainerSession(REGISTRY_IMAGE, "src/Dockerfile", "src")

# TestingSession - runs the Dockerfile's "testing" stage via dfbar
ts = build_tool.TestingSession([
    "dfbar -f ./src/Dockerfile -m test ./src",
])

# LintingSession - lints the python source and the Dockerfile
ls = build_tool.LintingSession([
    "pylint src",
    "black --diff src",
    "docker run --rm -i hadolint/hadolint < ./src/Dockerfile",
])


def push_image():
    cs.login(REGISTRY_HOST, REGISTRY_USER, os.environ["REGISTRY_TOKEN"])
    cs.push()


@action
def build():
    # BUILD_VERSION is referenced by setup.py
    os.environ["BUILD_VERSION"] = bs.v_mmp
    pys.setup()
    pys.build()

    cs.build_args.append(f"VERSION={bs.v_full}")
    cs.build()


@action
def test():
    ts.run()


@action
def lint():
    ls.run()


@action
def clean():
    pys.clean_venv()
    pys.clean_dist()
    bash(f"docker image rm {REGISTRY_IMAGE} --force", ignore_rc=True)


@action
def pr_main():
    # A PR into main just needs to build and test cleanly
    build()
    test()


@action
def push_main():
    # Commits to main update the registry image's "main" and "latest" tags
    cs.tags = ["main", "latest"]

    build()
    test()
    push_image()


@action
def release():
    # Make sure we have a github version to base the release on
    ref = os.environ.get("GITHUB_REF", "")
    if ref == "":
        Util.fail("Missing github version reference")

    bs.update_version([ref])

    # Tag the image with the full semver components
    cs.tags = [
        bs.v_major,
        f"{bs.v_major}.{bs.v_minor}",
        f"{bs.v_major}.{bs.v_minor}.{bs.v_patch}",
    ]

    build()
    test()
    push_image()

    # Publish the package to PyPI
    pys.publish("__token__", os.environ["PYPI_TOKEN"])

    # Create a github release for this tag
    Util.github_release(
        bs.v_stripped,
        name=f"Version {bs.v_full}",
        prerelease=bs.v_is_prerelease,
        github_token=os.environ["GITHUB_TOKEN"],
        repo=os.environ["GITHUB_REPOSITORY"],
    )


# run_action will run the action and also control the exit of
# the process (i.e. does not return)
build_tool.configure_logging(debug=True)
build_tool.run_action()
