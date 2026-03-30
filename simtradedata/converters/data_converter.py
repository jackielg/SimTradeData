"""
Data converter to transform fetched data into PTrade-compatible format
"""

import logging
from typing import Dict

import numpy as np
import pandas as pd

from simtradedata.config.field_mappings import (
    FUNDAMENTAL_FIELD_MAP,
    MARKET_FIELD_MAP,
    VALUATION_FIELD_MAP,
)

logger = logging.getLogger(__name__)


class DataConverter:
    """
    Convert data from various sources to PTrade format

    This handles:
    - Field name mapping
    - Data type conversion
    - Index formatting
    - Data structure reorganization
    """

    # Import field mappings from config
    MARKET_FIELD_MAP = MARKET_FIELD_MAP
    VALUATION_FIELD_MAP = VALUATION_FIELD_MAP
    FUNDAMENTAL_FIELD_MAP = FUNDAMENTAL_FIELD_MAP

    def convert_market_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Convert market data to PTrade format

        Args:
            df: Raw market data from data source
            symbol: Stock code in PTrade format

        Returns:
            DataFrame in PTrade format with columns:
            [open, high, low, close, volume, money]
        """
        if df.empty:
            return df

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            if "date" in df.columns:
                df.set_index("date", inplace=True)
            df.index = pd.to_datetime(df.index)

        # Normalize datetime to remove time component (SimTradeLab uses date only)
        df.index = df.index.normalize()

        # Efficiently select and rename columns
        # Create a map of existing source columns to target names
        rename_map = {
            src: tgt
            for src, tgt in self.MARKET_FIELD_MAP.items()
            if src in df.columns and src != "date"
        }
        result = df.rename(columns=rename_map)

        # Ensure all required columns are present, and in the correct order
        column_order = ["close", "open", "high", "low", "volume", "money"]
        result = result.reindex(columns=column_order)

        # Convert to appropriate data types with validation
        failed_cols = []
        for col in result.columns:
            try:
                result[col] = pd.to_numeric(result[col])
            except (ValueError, TypeError):
                failed_cols.append(col)

        # If any column failed strict conversion, use coerce with warning
        if failed_cols:
            logger.warning(
                f"{symbol}: Market data columns {failed_cols} have invalid values, "
                f"using coerce (may introduce NaN)"
            )
            for col in failed_cols:
                result[col] = pd.to_numeric(result[col], errors="coerce")

                nan_count = result[col].isna().sum()
                if nan_count > 0:
                    logger.warning(
                        f"{symbol}.{col}: {nan_count}/{len(result)} values converted to NaN"
                    )

        logger.info(
            f"Converted market data for {symbol}: {len(result)} rows, "
            f"{len(result.columns)} columns"
        )

        return result

    def convert_valuation_data(
        self, df: pd.DataFrame, market_df: pd.DataFrame, symbol: str
    ) -> pd.DataFrame:
        """
        Convert valuation data to PTrade format

        PTrade valuation includes: float_value, pb, pcf, pe_ttm, ps_ttm,
        total_shares, total_value, turnover_rate

        Args:
            df: Raw valuation data from BaoStock
            market_df: Market data (needed for calculating market cap)
            symbol: Stock code

        Returns:
            DataFrame with valuation indicators
        """
        if df.empty:
            return df

        # Strict validation: ensure we have the expected raw fields from BaoStock
        expected_fields = ["peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "turn"]
        missing_fields = [f for f in expected_fields if f not in df.columns]
        if missing_fields:
            raise ValueError(
                f"Missing expected valuation fields: {missing_fields}. "
                f"Got columns: {list(df.columns)}"
            )

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            if "date" in df.columns:
                df.set_index("date", inplace=True)
            else:
                raise ValueError(
                    f"Valuation data must have 'date' column or DatetimeIndex. "
                    f"Got columns: {list(df.columns)}, index: {type(df.index).__name__}"
                )
            df.index = pd.to_datetime(df.index)

        # Rename columns
        result = df.rename(columns=self.VALUATION_FIELD_MAP)

        # Select PTrade fields
        ptrade_fields = ["pe_ttm", "pb", "ps_ttm", "pcf", "turnover_rate"]
        result = result[[col for col in ptrade_fields if col in result.columns]]

        # Note: Market cap fields (total_shares, total_value, float_value) are
        # calculated in the download script using market_cap_calculator.py
        # They require combining daily valuation data with quarterly fundamental data

        logger.info(f"Converted valuation data for {symbol}: {len(result)} rows")

        return result

    def convert_fundamentals(
        self,
        profit_df: pd.DataFrame,
        operation_df: pd.DataFrame,
        growth_df: pd.DataFrame,
        balance_df: pd.DataFrame,
        cash_flow_df: pd.DataFrame,
        symbol: str,
    ) -> pd.DataFrame:
        """
        Merge and convert fundamental data from multiple BaoStock APIs

        PTrade fundamentals has 23 fields, sourced from:
        - profit data (盈利能力)
        - operation data (营运能力)
        - growth data (成长能力)
        - balance data (偿债能力)
        - cash flow data (现金流量)

        Args:
            profit_df: Profit ability data
            operation_df: Operation ability data
            growth_df: Growth ability data
            balance_df: Balance ability data
            cash_flow_df: Cash flow data
            symbol: Stock code

        Returns:
            DataFrame with 23 fundamental indicators
        """
        # Prepare a list of dataframes to merge
        dfs_to_merge = []
        for df in [profit_df, operation_df, growth_df, balance_df, cash_flow_df]:
            if not df.empty and "statDate" in df.columns:
                df_copy = df.copy()
                df_copy["end_date"] = pd.to_datetime(df_copy["statDate"])
                df_copy.set_index("end_date", inplace=True)
                # Drop original statDate to avoid duplication
                dfs_to_merge.append(df_copy.drop(columns=["statDate"]))

        if not dfs_to_merge:
            return pd.DataFrame()

        # Efficiently merge all DataFrames at once
        result = pd.concat(dfs_to_merge, axis=1)

        # Remove duplicated columns from join artifacts
        result = result.loc[:, ~result.columns.duplicated()]

        # Map fields to PTrade names
        rename_map = {
            src: tgt
            for src, tgt in self.FUNDAMENTAL_FIELD_MAP.items()
            if src in result.columns
        }
        mapped_result = result.rename(columns=rename_map)

        # Note: TTM fields should be calculated using ttm_calculator.calculate_ttm_indicators()
        # before calling this method. This ensures proper rolling calculations.
        # We only add placeholders for TTM fields that are truly unavailable.

        # Add missing fields with NaN and ensure correct order
        ptrade_fields = [
            "accounts_receivables_turnover_rate",
            "basic_eps_yoy",
            "current_assets_turnover_rate",
            "current_ratio",
            "debt_equity_ratio",
            "gross_income_ratio",
            "gross_income_ratio_ttm",
            "interest_cover",
            "inventory_turnover_rate",
            "net_profit_grow_rate",
            "net_profit_ratio",
            "net_profit_ratio_ttm",
            "np_parent_company_yoy",
            "operating_revenue_grow_rate",
            "quick_ratio",
            "roa",
            "roa_ebit_ttm",
            "roa_ttm",
            "roe",
            "roe_ttm",
            "roic",
            "total_asset_grow_rate",
            "total_asset_turnover_rate",
        ]
        # Reindex ensures all required columns are present (with NaN if missing) and in order
        # Important: reindex preserves existing values, only adds NaN for truly missing columns
        final_result = mapped_result.reindex(columns=ptrade_fields)

        logger.info(
            f"Converted fundamentals for {symbol}: {len(final_result)} quarters, "
            f"{len(final_result.columns)} indicators"
        )

        return final_result

    def convert_exrights_data(
        self, dividend_df: pd.DataFrame, adjust_df: pd.DataFrame, symbol: str
    ) -> pd.DataFrame:
        """
        Convert dividend and adjust factor data to PTrade exrights format

        PTrade exrights fields:
        - allotted_ps: 配股比例
        - rationed_ps: 配股价格比例
        - rationed_px: 配股价格
        - bonus_ps: 送股比例
        - exer_forward_a, exer_forward_b: 前复权因子
        - exer_backward_a, exer_backward_b: 后复权因子

        Args:
            dividend_df: Dividend data from BaoStock
            adjust_df: Adjust factor data from BaoStock
            symbol: Stock code

        Returns:
            DataFrame with exrights information
        """
        if dividend_df.empty:
            return pd.DataFrame()

        result = pd.DataFrame()

        # Map dividend fields
        if "dividOperateDate" in dividend_df.columns:
            result["date"] = pd.to_datetime(
                dividend_df["dividOperateDate"], format="%Y-%m-%d", errors="coerce"
            )
            result["date"] = result["date"].dt.strftime("%Y%m%d").astype(int)

        result["allotted_ps"] = dividend_df.get("allotmentRatio", 0.0)
        result["rationed_ps"] = 0.0  # Not directly available
        result["rationed_px"] = dividend_df.get("allotmentPrice", 0.0)
        result["bonus_ps"] = dividend_df.get("perShareDivRatio", 0.0)

        # Merge adjust factors
        if not adjust_df.empty:
            # adjust_df now has 'date' as column (not index)
            adjust_df_copy = adjust_df.copy()
            adjust_df_copy["date"] = (
                adjust_df_copy["date"].dt.strftime("%Y%m%d").astype(int)
            )

            result = result.merge(
                adjust_df_copy[["date", "foreAdjustFactor", "backAdjustFactor"]],
                on="date",
                how="left",
            )

            result["exer_forward_a"] = result.get("foreAdjustFactor", np.nan)
            result["exer_forward_b"] = np.nan  # Needs calculation
            result["exer_backward_a"] = result.get("backAdjustFactor", np.nan)
            result["exer_backward_b"] = np.nan  # Needs calculation
        else:
            result["exer_forward_a"] = np.nan
            result["exer_forward_b"] = np.nan
            result["exer_backward_a"] = np.nan
            result["exer_backward_b"] = np.nan

        # Set date as index
        if "date" in result.columns:
            result.set_index("date", inplace=True)

        # Select PTrade fields
        ptrade_fields = [
            "allotted_ps",
            "rationed_ps",
            "rationed_px",
            "bonus_ps",
            "exer_forward_a",
            "exer_forward_b",
            "exer_backward_a",
            "exer_backward_b",
        ]
        result = result[[col for col in ptrade_fields if col in result.columns]]

        logger.info(f"Converted exrights data for {symbol}: {len(result)} records")

        return result

    def convert_stock_metadata(self, basic_df: pd.DataFrame, symbol: str) -> Dict:
        """
        Convert stock basic info to PTrade metadata format

        PTrade stock_metadata fields:
        - blocks: JSON string with block/concept info
        - de_listed_date: Delisting date
        - has_info: Boolean flag
        - listed_date: IPO date
        - stock_name: Stock name

        Args:
            basic_df: Stock basic info from BaoStock
            symbol: Stock code

        Returns:
            Dictionary with metadata
        """
        if basic_df.empty:
            return {}

        row = basic_df.iloc[0]

        metadata = {
            "stock_name": row.get("code_name", ""),
            "listed_date": pd.to_datetime(row.get("ipoDate", ""), errors="coerce"),
            "de_listed_date": pd.to_datetime(row.get("outDate", ""), errors="coerce"),
            "has_info": True,
            "blocks": "{}",  # TODO: Fetch industry classification
        }

        logger.info(f"Converted metadata for {symbol}")

        return metadata
