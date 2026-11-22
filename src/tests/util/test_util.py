"""
Tests for kmt.util
"""

import pytest

import kmt.exception as exception
import kmt.util as util


class TestHashString:
    """
    Tests for util.hash_string
    """

    def test_sha1_is_deterministic(self):
        """Hashing the same string twice produces the same sha1 digest"""
        assert util.hash_string("hello") == util.hash_string("hello")

    def test_different_input_different_hash(self):
        """Different input strings produce different digests"""
        assert util.hash_string("hello") != util.hash_string("world")

    def test_short10_is_first_10_chars_of_sha256(self):
        """short10 truncates the sha256 digest to 10 hex characters"""
        import hashlib

        expected = hashlib.sha256("hello".encode("utf-8")).hexdigest()[:10]
        assert util.hash_string("hello", hash_type="short10") == expected

    def test_short8_is_deterministic_and_fixed_width(self):
        """short8 XORs 8-char chunks of the sha256 digest in to a short hex value"""
        result = util.hash_string("hello", hash_type="short8")

        assert result == util.hash_string("hello", hash_type="short8")
        assert all(c in "0123456789abcdef" for c in result)

    def test_non_string_source_is_rejected(self):
        """hash_string requires an actual str; it does not stringify other types"""
        with pytest.raises(exception.ValidationException):
            util.hash_string(123)


class TestHashObjectAndManifest:
    """
    Tests for util.hash_object and util.hash_manifest
    """

    def test_hash_object_is_deterministic_for_equivalent_dicts(self):
        """Hashing an object relies on a deterministic yaml dump"""
        obj1 = {"a": 1, "b": 2}
        obj2 = {"a": 1, "b": 2}

        assert util.hash_object(obj1) == util.hash_object(obj2)

    def test_hash_object_rejects_none(self):
        """None is not a valid source for hash_object"""
        with pytest.raises(exception.ValidationException):
            util.hash_object(None)

    def test_hash_manifest_ignores_metadata_changes(self):
        """Metadata is stripped before hashing, so changing it doesn't change the hash"""
        spec1 = {"kind": "ConfigMap", "data": {"a": "1"}, "metadata": {"name": "one"}}
        spec2 = {"kind": "ConfigMap", "data": {"a": "1"}, "metadata": {"name": "two"}}

        assert util.hash_manifest(spec1) == util.hash_manifest(spec2)

    def test_hash_manifest_reflects_data_changes(self):
        """Non-metadata changes do change the hash"""
        spec1 = {"kind": "ConfigMap", "data": {"a": "1"}, "metadata": {"name": "one"}}
        spec2 = {"kind": "ConfigMap", "data": {"a": "2"}, "metadata": {"name": "one"}}

        assert util.hash_manifest(spec1) != util.hash_manifest(spec2)

    def test_hash_manifest_does_not_mutate_source(self):
        """hash_manifest copies the source rather than popping metadata in place"""
        spec = {"kind": "ConfigMap", "data": {}, "metadata": {"name": "one"}}

        util.hash_manifest(spec)

        assert "metadata" in spec


class TestWalkObject:
    """
    Tests for util.walk_object
    """

    def test_visits_top_level_scalar(self):
        """A plain scalar is still passed to the callback once"""
        seen = []
        util.walk_object("value", seen.append)

        assert seen == ["value"]

    def test_visits_nested_dict_and_list_values(self):
        """All nested dict/list leaf values are visited"""
        seen = []
        obj = {"a": [1, 2, {"b": 3}]}
        util.walk_object(obj, seen.append)

        # Top level dict, the list, then each of its contained values
        assert obj in seen
        assert 1 in seen
        assert 2 in seen
        assert {"b": 3} in seen
        assert 3 in seen

    def test_update_replaces_values_in_place(self):
        """When update=True, the callback's return value replaces each node"""
        obj = {"a": 1, "b": [2, 3]}

        def double(x):
            if isinstance(x, int):
                return x * 2
            return x

        result = util.walk_object(obj, double, update=True)

        assert result == {"a": 2, "b": [4, 6]}

    def test_update_can_replace_the_root(self):
        """When update=True, a replaced root object is returned"""
        result = util.walk_object("a", lambda x: x.upper(), update=True)

        assert result == "A"

    def test_handles_cyclic_references_without_infinite_loop(self):
        """A self-referencing structure does not cause unbounded recursion"""
        obj = {}
        obj["self"] = obj

        seen = []
        util.walk_object(obj, seen.append)

        # The callback sees obj as the root and once more as the value of
        # "self", but the cycle must not be expanded again beyond that
        assert seen.count(obj) == 2

    def test_rejects_non_callable_callback(self):
        """callback must be callable"""
        with pytest.raises(exception.ValidationException):
            util.walk_object({}, "not callable")

    def test_rejects_none_object(self):
        """object must not be None"""
        with pytest.raises(exception.ValidationException):
            util.walk_object(None, lambda x: x)


class TestCoerceValue:
    """
    Tests for util.coerce_value
    """

    def test_none_types_returns_value_unchanged(self):
        """types=None means no coercion at all"""
        assert util.coerce_value(None, 123) == 123

    def test_value_already_correct_type_is_returned_as_is(self):
        """If val already matches one of the types, it is returned unchanged"""
        assert util.coerce_value(int, 5) == 5

    def test_single_type_is_normalised_to_tuple(self):
        """Passing a single type behaves the same as a 1-tuple of that type"""
        assert util.coerce_value(str, 5) == util.coerce_value((str,), 5)

    def test_bool_from_true_like_string(self):
        """A recognised truthy string coerces to bool True"""
        assert util.coerce_value(bool, "true") is True

    def test_bool_from_false_like_string(self):
        """A recognised falsy string coerces to bool False"""
        assert util.coerce_value(bool, "0") is False

    def test_str_from_non_string(self):
        """Non-string, non-None values are stringified for the str type"""
        assert util.coerce_value(str, 5) == "5"

    def test_str_does_not_convert_none(self):
        """None is deliberately not converted to the string 'None'"""
        with pytest.raises(exception.KMTConversionException):
            util.coerce_value(str, None)

    def test_yaml_parses_string_in_to_requested_container_type(self):
        """A yaml-looking string is parsed if that produces the target type"""
        assert util.coerce_value(list, "[1, 2, 3]") == [1, 2, 3]
        assert util.coerce_value(dict, "{a: 1}") == {"a": 1}

    def test_first_matching_type_in_tuple_wins(self):
        """Types are tried in the order given, and bool is attempted before str"""
        assert util.coerce_value((bool, str), "true") is True

    def test_raises_when_nothing_matches(self):
        """An unconvertable value raises KMTConversionException"""
        with pytest.raises(exception.KMTConversionException):
            util.coerce_value(list, "not a list")

    def test_rejects_invalid_types_argument(self):
        """types must be a type or tuple of types"""
        with pytest.raises(exception.ValidationException):
            util.coerce_value("not-a-type", "value")


class TestParseBool:
    """
    Tests for util.parse_bool
    """

    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", True])
    def test_truthy_values(self, value):
        """Recognised truthy representations parse to True"""
        assert util.parse_bool(value) is True

    @pytest.mark.parametrize("value", ["false", "False", "FALSE", "0", False])
    def test_falsy_values(self, value):
        """Recognised falsy representations parse to False"""
        assert util.parse_bool(value) is False

    def test_rejects_none(self):
        """None is not a valid input"""
        with pytest.raises(exception.ValidationException):
            util.parse_bool(None)

    def test_rejects_unparseable_string(self):
        """An unrecognised string raises KMTConversionException"""
        with pytest.raises(exception.KMTConversionException):
            util.parse_bool("maybe")


class TestExtractProperty:
    """
    Tests for util.extract_property
    """

    def test_returns_and_removes_present_key(self):
        """The key's value is returned and popped from the spec"""
        spec = {"a": 1, "b": 2}

        assert util.extract_property(spec, "a") == 1
        assert "a" not in spec

    def test_missing_key_returns_default(self):
        """A missing, non-required key returns the given default"""
        assert util.extract_property({}, "a", default="fallback") == "fallback"

    def test_missing_required_key_raises(self):
        """A missing, required key raises KeyError"""
        with pytest.raises(KeyError):
            util.extract_property({}, "a", required=True)

    def test_none_value_returns_default_even_if_required(self):
        """An explicit null value is treated the same as 'absent' and returns the default"""
        spec = {"a": None}

        assert util.extract_property(spec, "a", default="fallback", required=True) == "fallback"

    def test_rejects_non_dict_spec(self):
        """spec must be a dict"""
        with pytest.raises(exception.ValidationException):
            util.extract_property(["not", "a", "dict"], "a")


class TestResolveVarRefs:
    """
    Tests for util.resolve_var_refs
    """

    def _environment(self):
        import jinja2

        return jinja2.Environment(undefined=jinja2.StrictUndefined, keep_trailing_newline=True)

    def test_resolves_simple_dependency_chain(self):
        """A value that references another var is resolved after its dependency"""
        source_vars = {"a": "1", "b": "{{ a }}2"}

        result = util.resolve_var_refs(source_vars, self._environment())

        assert result["a"] == "1"
        assert result["b"] == "12"

    def test_does_not_mutate_source_by_default(self):
        """Without inplace=True, the original dict is left untouched"""
        source_vars = {"a": "1", "b": "{{ a }}2"}

        util.resolve_var_refs(source_vars, self._environment())

        assert source_vars["b"] == "{{ a }}2"

    def test_inplace_mutates_source(self):
        """With inplace=True, resolution happens on (and mutates) the source dict"""
        source_vars = {"a": "1", "b": "{{ a }}2"}

        result = util.resolve_var_refs(source_vars, self._environment(), inplace=True)

        assert result is source_vars
        assert source_vars["b"] == "12"

    def test_circular_reference_raises(self):
        """A circular dependency between vars cannot be resolved"""
        source_vars = {"a": "{{ b }}", "b": "{{ a }}"}

        with pytest.raises(exception.KMTResolveException):
            util.resolve_var_refs(source_vars, self._environment())

    def test_ignore_list_skips_dependency_calculation(self):
        """A key in ignore_list is left untouched and not templated"""
        source_vars = {"a": "{{ untemplated_elsewhere }}"}

        result = util.resolve_var_refs(source_vars, self._environment(), ignore_list=["a"])

        assert result["a"] == "{{ untemplated_elsewhere }}"

    def test_rejects_non_dict_source(self):
        """source_vars must be a dict"""
        with pytest.raises(exception.ValidationException):
            util.resolve_var_refs(["not", "a", "dict"], self._environment())


class TestYamlDumpLoad:
    """
    Tests for util.yaml_dump/yaml_dump_all/yaml_load/yaml_load_all
    """

    def test_round_trip_dict(self):
        """A dict round trips through yaml_dump/yaml_load"""
        obj = {"a": 1, "b": [1, 2, 3]}

        assert util.yaml_load(util.yaml_dump(obj)) == obj

    def test_dump_all_and_load_all_round_trip_multiple_documents(self):
        """Multiple documents round trip through yaml_dump_all/yaml_load_all"""
        docs = [{"a": 1}, {"b": 2}]

        dumped = util.yaml_dump_all(docs)
        loaded = list(util.yaml_load_all(dumped))

        assert loaded == docs


class TestCheckFindManifestsKeys:
    """
    Tests for util.check_find_manifests_keys
    """

    def test_accepts_known_string_keys(self):
        """All documented search keys with string values are accepted"""
        util.check_find_manifests_keys({"group": "apps", "kind": "Deployment"})

    def test_rejects_unknown_key(self):
        """An unrecognised search key is rejected"""
        with pytest.raises(exception.PipelineRunException):
            util.check_find_manifests_keys({"bogus": "value"})

    def test_rejects_non_string_value(self):
        """A known key with a non-string value is rejected"""
        with pytest.raises(exception.PipelineRunException):
            util.check_find_manifests_keys({"kind": 123})


class TestFindManifests:
    """
    Tests for util.find_manifests
    """

    def test_matches_on_kind_and_namespace(self, make_pipeline, make_manifest):
        """A search narrows by every key supplied"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(pipeline, {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cm1", "namespace": "ns1"},
        })

        search = {"kind": "ConfigMap", "namespace": "ns1"}
        result = util.find_manifests(search, [manifest], multiple=False)

        assert result is manifest

    def test_no_match_raises_when_single_result_required(self, make_pipeline):
        """No matches and multiple=False raises PipelineRunException"""
        make_pipeline("pipeline: []\n")

        with pytest.raises(exception.PipelineRunException):
            util.find_manifests({"kind": "ConfigMap"}, [], multiple=False)

    def test_multiple_matches_raises_when_single_result_required(
        self, make_pipeline, make_manifest
    ):
        """More than one match and multiple=False raises PipelineRunException"""
        pipeline = make_pipeline("pipeline: []\n")
        manifests = [
            make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}}),
            make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm2"}}),
        ]

        with pytest.raises(exception.PipelineRunException):
            util.find_manifests({"kind": "ConfigMap"}, manifests, multiple=False)

    def test_multiple_true_returns_a_list_including_empty(self, make_pipeline):
        """multiple=True returns a (possibly empty) list instead of raising"""
        make_pipeline("pipeline: []\n")
        result = util.find_manifests({"kind": "ConfigMap"}, [], multiple=True)

        assert result == []

    def test_namespace_omitted_matches_current_namespace_or_clusterwide(
        self, make_pipeline, make_manifest
    ):
        """
        Without an explicit namespace filter, only resources in the current namespace
        or with no namespace at all (cluster-scoped) are matched
        """
        pipeline = make_pipeline("pipeline: []\n")
        same_ns = make_manifest(
            pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1", "namespace": "ns1"}}
        )
        other_ns = make_manifest(
            pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm2", "namespace": "ns2"}}
        )
        clusterwide = make_manifest(pipeline, {"kind": "ClusterRole", "metadata": {"name": "cr1"}})

        result = util.find_manifests(
            {}, [same_ns, other_ns, clusterwide], multiple=True, current_namespace="ns1"
        )

        assert same_ns in result
        assert clusterwide in result
        assert other_ns not in result

    def test_alias_match_excludes_the_default_self_alias(self, make_pipeline, make_manifest):
        """
        An alias search only matches an explicitly-set alias; it deliberately excludes
        manifests whose alias still defaults to their own name (core.Manifest.__init__
        sets kmt/alias = name whenever no explicit alias annotation was given)
        """
        pipeline = make_pipeline("pipeline: []\n")
        default_alias = make_manifest(pipeline, {"kind": "ConfigMap", "metadata": {"name": "cm1"}})
        explicit_alias = make_manifest(pipeline, {
            "kind": "ConfigMap",
            "metadata": {"name": "cm2", "annotations": {"kmt/alias": "my-alias"}},
        })

        result = util.find_manifests(
            {"alias": "my-alias"}, [default_alias, explicit_alias], multiple=True
        )

        assert result == [explicit_alias]

    def test_pattern_matches_against_name_as_regex(self, make_pipeline, make_manifest):
        """pattern is matched as a regex search against the manifest name"""
        pipeline = make_pipeline("pipeline: []\n")
        manifest = make_manifest(
            pipeline, {"kind": "ConfigMap", "metadata": {"name": "my-app-config"}}
        )

        assert util.find_manifests({"pattern": "app-conf"}, [manifest], multiple=True) == [manifest]
        assert util.find_manifests({"pattern": "^nomatch$"}, [manifest], multiple=True) == []

    def test_rejects_non_manifest_members(self, make_pipeline):
        """Every entry in manifests must be a core.Manifest"""
        with pytest.raises(exception.ValidationException):
            util.find_manifests({}, ["not a manifest"], multiple=True)
