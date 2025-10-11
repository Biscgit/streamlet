import pytest

from core.metric import Metric, MetricFrame
from core.settings import Settings


class TestMetricAndData:
    """Tests related to Dataframes and Metrics."""

    @staticmethod
    def get_frame(data=None, metric_field: str | None = "metric", metric=None, name=""):
        frame = MetricFrame(name)
        for d in data or []:
            m = metric or (d.pop(metric_field) if metric_field else None)
            frame.append(Metric(frame, d, m, metric_field))
        return frame

    def test_dataframe_create(self):
        data = [{"metric": 1, "attr": 2}, {"metric": 3, "attr": 4}]
        frame = self.get_frame(data)

        assert frame[0].metric == 1
        assert frame[0].attributes["attr"] == 2
        assert frame[1].metric == 3
        assert frame[1].attributes["attr"] == 4

        with pytest.raises(KeyError):
            _ = frame[0].attributes["metric"]

        with pytest.raises(KeyError):
            _ = frame[1].attributes["metric"]

    def test_dataframe_freeze(self):
        data = [{"metric": 1, "attr": 2}, {"metric": 3, "attr": 4}]
        frame = self.get_frame(data)

        metric = frame[0]
        metric["new_field"] = "hello"

        frame.freeze()

        # check changing is not possible
        with pytest.raises(TypeError):
            metric["new_field"] = "world"

        # check old value is unchanged
        assert metric["new_field"] == "hello"

    def test_create_no_metric_forbidden(self):
        data = [{"metric": 1, "attr": 2}, {"metric": 3, "attr": 4}]
        with pytest.raises(ValueError):
            _ = self.get_frame(data, metric_field=None)

    def test_create_no_metric_allowed(self):
        Settings.allow_none_metric = True

        data = [{"metric": 1, "attr": 2}, {"metric": 3, "attr": 4}]
        frame = self.get_frame(data, metric_field=None)

        assert frame[0].attributes == {"metric": 1, "attr": 2}
        assert frame[0].metric is None
        assert frame[1].attributes == {"metric": 3, "attr": 4}
        assert frame[1].metric is None

    def test_add_new_metric(self):
        data = [{"metric": 1, "attr": 2}, {"metric": 3, "attr": 4}]
        frame = self.get_frame(data)

        frame.append(Metric(frame, {"attr": 6}, 5, "metric"))

        assert len(frame) == 3
        assert frame[2].metric == 5

    def test_metric_normalization(self):
        Settings.nested_attr_seperator = "#"

        data = {"field": {"nested": 5}, "field2": "a"}
        metric = Metric(self.get_frame(), data, 2, "metric")

        flattened = metric.flatten()

        assert "metric" not in flattened
        assert all(not isinstance(k, (list | dict)) for k in flattened)
        assert flattened["field2"] == "a"
        assert flattened["field#nested"] == 5

    def test_metric_setitem(self):
        Settings.nested_attr_seperator = "."
        data = {"field": {"nested": 5}, "field2": "a"}
        metric = Metric(self.get_frame(), data, 2, "metric")

        metric["some_field"] = 123
        metric["field.nested2"] = 9

        expected = {"some_field": 123, "field": {"nested": 5, "nested2": 9}, "field2": "a"}
        assert metric.attributes == expected
