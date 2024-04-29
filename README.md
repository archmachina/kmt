# KMT - Kubernetes Manifest Transform

KMT (Kubernetes Manifest Transform) is a tool for manipulation of kubernetes manifests based on a transform specification prior to deployment on a kubernetes environment.

The aim for KMT is to provide a flexible method of transforming manifests using a combination of templating to produce valid yaml and inplace modifications of resources.

KMT should provide the user with flexibility in the approach for transforming their manifests to support a wide spectrum of use cases

Transforms of manifests are performed by discrete steps, which form a pipeline. Pipelines may also call other pipelines to generate new resources and/or transform existing resources.

KMT supports [Jinja2](https://jinja.palletsprojects.com/en/3.1.x/) for templating and [jsonpatch](https://pypi.org/project/jsonpatch/) for modifications to resources, along with builtin tasks to import files, delete resources, update metadata, etc.

## Installation

### Pip

kmt can be installed from pypi using pip:
```
$ pip install kmt

...

$ kmt --help
usage: kmt [-h] [-d] path

Kubernetes Manifest Transform

positional arguments:
  path        Pipeline directory path

options:
  -h, --help  show this help message and exit
  -d          Enable debug output
```

or in a virtual environment:
```
$ python3 -m venv test
$ . ./test/bin/activate
(test) $ pip install kmt
(test) $ kmt --help
usage: kmt [-h] [-d] path

Kubernetes Manifest Transform

positional arguments:
  path        Pipeline directory path

options:
  -h, --help  show this help message and exit
  -d          Enable debug output
```

### Docker

A docker image is also available from [hub.docker.com](https://hub.docker.com/r/archmachina/kmt) and can be run as follows:
```
$ docker run --rm -it archmachina/kmt:0.3.3 --help
usage: kmt [-h] [-d] path

Kubernetes Manifest Transform

positional arguments:
  path        Pipeline directory path

options:
  -h, --help  show this help message and exit
  -d          Enable debug output
```

*Note: The `latest` tag for the docker image represents the latest build from the main branch and may have non-backwards compatible changes.*

## Usage

### Step types

#### pipeline

#### import

#### vars

#### stdin

#### jsonpatch

#### metadata

#### delete

### Common step parameters

#### when

#### filter

#### match_any_tags

#### match_all_tags

#### exclude_tags

#### apply_tags

#### match_group

#### match_version

#### match_kind

#### exclude_kind

#### match_namespace

#### match_name

### Features

#### Ordering

#### Hash rename

#### Hash lookup

#### Templating
