from core.settings import Settings


class TestSettings:
    """Tests for the Settings class."""

    def test_set_setting(self):
        Settings.run_once = False
        Settings.set("run_once", True)

        assert Settings.run_once is True

    def test_persistent_argv(self):
        Settings.log_level = 10

        Settings.set("log_level", 20, persistent=True)
        assert Settings.log_level == 20

        Settings.set("log_level", 30, persistent=False)
        Settings.set("log_level", 40, persistent=True)
        assert Settings.log_level == 20
