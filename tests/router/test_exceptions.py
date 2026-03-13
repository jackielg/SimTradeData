import pytest

from simtradedata.router.exceptions import DataSourceError, NoSourceAvailable


class TestExceptions:
    def test_data_source_error_is_exception(self):
        with pytest.raises(Exception):
            raise DataSourceError("all sources failed")

    def test_no_source_available_is_data_source_error(self):
        with pytest.raises(DataSourceError):
            raise NoSourceAvailable("no source for minute_bars/us")

    def test_no_source_available_message(self):
        err = NoSourceAvailable("no source for minute_bars/us")
        assert "minute_bars" in str(err)
