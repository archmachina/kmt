
import yaml
import kmt.yaml_types as yaml_types


class TestYamlTypes:
    def test_secret_tag_dumps_and_reloads(self):
        # Regression test: lookup_secret_representer used to call
        # dumper.represent_str("!secret", name), but represent_str is
        # bound to the built-in str tag and doesn't accept a tag
        # argument, so dumping an unresolved !secret tag raised a
        # TypeError. represent_scalar is the correct general-purpose call.
        tag = yaml_types.LookupSecret("my-secret")

        dumped = yaml.dump(tag, Dumper=yaml.SafeDumper)
        reloaded = yaml.load(dumped, Loader=yaml.SafeLoader)

        assert isinstance(reloaded, yaml_types.LookupSecret)
        assert reloaded.name == "my-secret"

    def test_configmap_tag_dumps_and_reloads(self):
        tag = yaml_types.LookupConfigMap("my-configmap")

        dumped = yaml.dump(tag, Dumper=yaml.SafeDumper)
        reloaded = yaml.load(dumped, Loader=yaml.SafeLoader)

        assert isinstance(reloaded, yaml_types.LookupConfigMap)
        assert reloaded.name == "my-configmap"

    def test_secret_tag_dumps_when_nested(self):
        # The original bug only surfaced once the tag was nested inside a
        # dict/list, the same shape a manifest spec takes when a Secret or
        # ConfigMap reference hasn't been resolved yet (e.g. while kmt is
        # hashing a manifest's own spec for kmt/rename-hash, which happens
        # before tag resolution).
        document = {
            "containers": [
                {"env": [{"name": "SECRET_NAME", "value": yaml_types.LookupSecret("bot-tokens")}]}
            ]
        }

        dumped = yaml.dump(document, Dumper=yaml.SafeDumper)
        reloaded = yaml.load(dumped, Loader=yaml.SafeLoader)

        resolved_value = reloaded["containers"][0]["env"][0]["value"]
        assert isinstance(resolved_value, yaml_types.LookupSecret)
        assert resolved_value.name == "bot-tokens"
