"""
FINVALUE ID to PTrade field mapping for mootdx financial data.

This file maps mootdx FINVALUE array indices to PTrade field names.
Reference: docs/reference/mootdx_api/docs/api/fields.md
"""

# FINVALUE position -> (PTrade field name, description, unit)
# Note: FINVALUE data is 0-indexed array from mootdx finance() API
FINVALUE_TO_PTRADE = {
    # Report date (YYMMDD format, e.g., 150930 = 2015Q3)
    0: ("_report_date_raw", "Report period (YYMMDD)", None),

    # Per-share indicators
    1: ("basic_eps", "Basic EPS", "yuan"),
    2: ("eps_deducted", "EPS after non-recurring", "yuan"),
    3: ("undistributed_profit_ps", "Undistributed profit per share", "yuan"),
    4: ("nav_ps", "Net asset value per share", "yuan"),
    5: ("capital_reserve_ps", "Capital reserve per share", "yuan"),
    6: ("roe", "Return on equity", "percent"),
    7: ("operating_cash_flow_ps", "Operating cash flow per share", "yuan"),

    # Balance sheet - key items
    8: ("cash", "Cash and equivalents", "yuan"),
    11: ("accounts_receivable", "Accounts receivable", "yuan"),
    17: ("inventory", "Inventory", "yuan"),
    21: ("current_assets", "Total current assets", "yuan"),
    27: ("fixed_assets", "Fixed assets", "yuan"),
    33: ("intangible_assets", "Intangible assets", "yuan"),
    39: ("non_current_assets", "Total non-current assets", "yuan"),
    40: ("total_assets", "Total assets", "yuan"),
    41: ("short_term_debt", "Short-term borrowings", "yuan"),
    44: ("accounts_payable", "Accounts payable", "yuan"),
    54: ("current_liabilities", "Total current liabilities", "yuan"),
    55: ("long_term_debt", "Long-term borrowings", "yuan"),
    62: ("non_current_liabilities", "Total non-current liabilities", "yuan"),
    63: ("total_liabilities", "Total liabilities", "yuan"),
    64: ("paid_in_capital", "Paid-in capital (share capital)", "yuan"),
    68: ("retained_earnings", "Undistributed profits", "yuan"),
    72: ("total_equity", "Total shareholders' equity", "yuan"),

    # Income statement
    74: ("operating_revenue", "Operating revenue", "yuan"),
    75: ("operating_cost", "Operating cost", "yuan"),
    80: ("finance_expense", "Finance expenses", "yuan"),
    86: ("operating_profit", "Operating profit", "yuan"),
    92: ("total_profit", "Total profit", "yuan"),
    95: ("net_profit", "Net profit", "yuan"),
    96: ("np_parent_company", "Net profit attributable to parent", "yuan"),

    # Cash flow statement
    107: ("operating_cash_flow_net", "Net cash from operations", "yuan"),
    119: ("investing_cash_flow_net", "Net cash from investing", "yuan"),
    128: ("financing_cash_flow_net", "Net cash from financing", "yuan"),

    # Solvency analysis
    159: ("current_ratio", "Current ratio", "ratio"),
    160: ("quick_ratio", "Quick ratio", "ratio"),
    162: ("interest_cover", "Interest coverage ratio", "ratio"),

    # Operating efficiency analysis
    172: ("accounts_receivables_turnover_rate", "A/R turnover rate", "times"),
    173: ("inventory_turnover_rate", "Inventory turnover rate", "times"),
    175: ("total_asset_turnover_rate", "Total asset turnover rate", "times"),
    179: ("current_assets_turnover_rate", "Current assets turnover rate", "times"),

    # Growth analysis
    183: ("operating_revenue_grow_rate", "Revenue YoY growth", "percent"),
    184: ("net_profit_grow_rate", "Net profit YoY growth", "percent"),
    185: ("net_asset_grow_rate", "Net asset YoY growth", "percent"),
    187: ("total_asset_grow_rate", "Total asset YoY growth", "percent"),

    # Profitability analysis
    197: ("roe_weighted", "Weighted ROE", "percent"),
    199: ("net_profit_ratio", "Net profit margin", "percent"),
    200: ("total_asset_return_rate", "ROA", "percent"),
    202: ("gross_income_ratio", "Gross profit margin", "percent"),

    # Capital structure
    210: ("debt_equity_ratio", "Debt to asset ratio", "percent"),

    # Share capital
    238: ("total_shares", "Total shares", "shares"),
    239: ("a_floats", "Float A shares (listed circulating)", "shares"),
    242: ("shareholder_count", "Number of shareholders", "count"),

    # TTM indicators
    276: ("net_profit_ttm", "Net profit TTM", "yuan"),
    283: ("operating_revenue_ttm", "Operating revenue TTM (10k yuan)", "wan_yuan"),

    # Announcement dates
    314: ("_publ_date_raw", "Financial report date (YYMMDD)", None),
}

# Reverse mapping: PTrade field name -> FINVALUE position
PTRADE_TO_FINVALUE = {v[0]: k for k, v in FINVALUE_TO_PTRADE.items()}

# Core fields commonly used in analysis
CORE_FUNDAMENTAL_FIELDS = [
    # Per-share
    "basic_eps",
    "nav_ps",
    "roe",

    # Growth
    "operating_revenue_grow_rate",
    "net_profit_grow_rate",

    # Profitability
    "net_profit_ratio",
    "gross_income_ratio",

    # Solvency
    "current_ratio",
    "quick_ratio",
    "debt_equity_ratio",

    # Efficiency
    "accounts_receivables_turnover_rate",
    "total_asset_turnover_rate",
    "interest_cover",

    # Share data
    "total_shares",
    "a_floats",
]


def parse_finvalue_date(raw_date: int) -> str | None:
    """
    Parse FINVALUE date to ISO date string.

    Supports both formats:
    - YYMMDD (6-digit): e.g., 231231 -> '2023-12-31'
    - YYYYMMDD (8-digit): e.g., 20231231 -> '2023-12-31'

    Args:
        raw_date: Date in YYMMDD or YYYYMMDD format

    Returns:
        ISO date string (YYYY-MM-DD), or None if invalid
    """
    if not raw_date or raw_date == 0:
        return None

    raw_str = str(int(raw_date))

    if len(raw_str) == 8:
        # YYYYMMDD format
        return f"{raw_str[:4]}-{raw_str[4:6]}-{raw_str[6:8]}"

    # YYMMDD format (pad to 6 digits)
    raw_str = raw_str.zfill(6)
    year_prefix = "20" if int(raw_str[:2]) < 50 else "19"
    return f"{year_prefix}{raw_str[:2]}-{raw_str[2:4]}-{raw_str[4:6]}"
