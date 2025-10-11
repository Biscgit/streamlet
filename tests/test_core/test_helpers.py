from voluptuous import Schema

from core import Settings
from core.helpers import flatten
from core.validation import walk_similar_key


class TestHelpers:
    def test_walk_keys(self):
        config = {"a": {"b": {"ccd": 10}}}
        path = ["a", "b"]
        key = "ccd"
        schema = Schema({"a": {"b": {"ccc": int, "ddd": int}}})

        match = walk_similar_key(schema, config, path, key)
        assert match is not None

        match_key = match[0][0]
        value = match[1]

        assert match_key == "ccc"
        assert value == "10"

    def test_flatten_normal(self):
        Settings.nested_attr_seperator = "."
        obj = {
            "a": 1,
            "b": True,
            "c": [1, 2, 3],
            "d": {"e": 1.5, "f": {"g": False}},
            "h.i": {"j": -1},
        }
        expected = {
            "a": 1,
            "b": True,
            "c.0": 1,
            "c.1": 2,
            "c.2": 3,
            "d.e": 1.5,
            "d.f.g": False,
            "h.i.j": -1,
        }

        result = flatten(obj, Settings.nested_attr_seperator)
        assert expected == result

    def test_flatten_no_seperator(self):
        obj = {
            "a": 1,
            "b": True,
            "c": [1, 2, 3],
            "d": {"e": 1.5, "f": {"g": False}},
            "h.i": {"j": -1},
        }
        expected = {
            ("a",): 1,
            ("b",): True,
            ("c", 0): 1,
            ("c", 1): 2,
            ("c", 2): 3,
            ("d", "e"): 1.5,
            ("d", "f", "g"): False,
            ("h.i", "j"): -1,
        }

        result = flatten(obj)
        assert expected == result
