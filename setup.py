import os

from setuptools import find_packages, setup

with open("README.md") as readme_file:
    README = readme_file.read()

setup_args = {
    "name": "kmt",
    "version": os.environ["BUILD_VERSION"],
    "description": "Kubernetes Manifest Transform",
    "long_description_content_type": "text/markdown",
    "long_description": README,
    "license": "MIT",
    "packages": find_packages(where="src", include=["kmt", "kmt.*"]),
    "author": "Jesse Reichman",
    "keywords": ["Kubernetes", "Manifest", "Transform"],
    "url": "https://github.com/archmachina/kmt",
    "download_url": "https://pypi.org/project/kmt/",
    "entry_points": {"console_scripts": ["kmt = kmt.cli:main"]},
    "package_dir": {"": "src"},
    "install_requires": ["PyYAML>=6.0.0", "Jinja2>=3.1.0", "jsonpatch>=1.33", "jsonpath-ng>=1.6.1"],
}

if __name__ == "__main__":
    setup(**setup_args, include_package_data=True)
