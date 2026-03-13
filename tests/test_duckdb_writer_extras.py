"""Tests for DuckDB writer: money_flow, lhb, and margin_trading tables."""

import pandas as pd
import pytest

from simtradedata.writers.duckdb_writer import DuckDBWriter


@pytest.mark.unit
class TestWriteMoneyFlow:
    def setup_method(self):
        self.writer = DuckDBWriter(db_path=":memory:")

    def teardown_method(self):
        self.writer.close()

    def test_write_and_read(self):
        df = pd.DataFrame({
            "date": pd.to_datetime(["2025-01-02"]),
            "net_main": [1000.0], "net_super": [-500.0],
            "net_large": [1500.0], "net_medium": [-800.0], "net_small": [-200.0],
        })
        self.writer.write_money_flow("000001.SZ", df)
        result = self.writer.conn.execute(
            "SELECT * FROM money_flow WHERE symbol = '000001.SZ'"
        ).fetchdf()
        assert len(result) == 1
        assert result.iloc[0]["net_main"] == pytest.approx(1000.0)

    def test_upsert_replaces(self):
        df1 = pd.DataFrame({
            "date": pd.to_datetime(["2025-01-02"]),
            "net_main": [1000.0], "net_super": [0.0],
            "net_large": [0.0], "net_medium": [0.0], "net_small": [0.0],
        })
        df2 = pd.DataFrame({
            "date": pd.to_datetime(["2025-01-02"]),
            "net_main": [2000.0], "net_super": [0.0],
            "net_large": [0.0], "net_medium": [0.0], "net_small": [0.0],
        })
        self.writer.write_money_flow("000001.SZ", df1)
        self.writer.write_money_flow("000001.SZ", df2)
        result = self.writer.conn.execute(
            "SELECT net_main FROM money_flow WHERE symbol = '000001.SZ'"
        ).fetchone()
        assert result[0] == pytest.approx(2000.0)


@pytest.mark.unit
class TestWriteLhb:
    def setup_method(self):
        self.writer = DuckDBWriter(db_path=":memory:")

    def teardown_method(self):
        self.writer.close()

    def test_write_and_read(self):
        df = pd.DataFrame({
            "symbol": ["000001.SZ"],
            "date": pd.to_datetime(["2025-01-02"]),
            "reason": ["daily_limit"],
            "net_buy": [5e7], "buy_amount": [8e7], "sell_amount": [3e7],
        })
        self.writer.write_lhb(df)
        result = self.writer.conn.execute("SELECT * FROM lhb").fetchdf()
        assert len(result) == 1


@pytest.mark.unit
class TestWriteMarginTrading:
    def setup_method(self):
        self.writer = DuckDBWriter(db_path=":memory:")

    def teardown_method(self):
        self.writer.close()

    def test_write_and_read(self):
        df = pd.DataFrame({
            "date": pd.to_datetime(["2025-01-02"]),
            "rzye": [1e9], "rqyl": [5e7], "rzrqye": [1.05e9],
        })
        self.writer.write_margin_trading("000001.SZ", df)
        result = self.writer.conn.execute(
            "SELECT * FROM margin_trading WHERE symbol = '000001.SZ'"
        ).fetchdf()
        assert len(result) == 1
