
import yaml

# Define a representer for pyyaml to format multiline strings as block quotes
def str_representer(dumper, data):
    if isinstance(data, str) and '\n' in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

yaml.add_representer(str, str_representer)

def dump(source):
    dumper = yaml.SafeDumper

    return yaml.dump(source, Dumper=dumper, explicit_start=True, sort_keys=False, indent=2)

def dump_all(source):
    dumper = yaml.SafeDumper

    return yaml.dump_all(source, Dumper=dumper, explicit_start=True, sort_keys=False, indent=2)

def load(source):
    loader = yaml.SafeLoader

    return yaml.load(source, Loader=loader)

def load_all(source):
    loader = yaml.SafeLoader

    return yaml.load_all(source, Loader=loader)
