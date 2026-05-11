"""Raw source adapter layer for pilot AkShare sources."""

from .akshare_disclosure import fetch_stock_yysj_em
from .akshare_index import fetch_stock_zh_index_hist_csindex
from .akshare_industry import (
    fetch_index_component_sw,
    fetch_index_hist_sw,
    fetch_stock_board_concept_index_ths,
    fetch_stock_board_concept_summary_ths,
    fetch_stock_board_industry_index_ths,
    fetch_stock_industry_change_cninfo,
    fetch_stock_industry_clf_hist_sw,
    fetch_sw_index_first_info,
    fetch_sw_index_second_info,
    fetch_sw_index_third_info,
)
from .akshare_margin import fetch_stock_margin_detail_sse, fetch_stock_margin_detail_szse
from .akshare_market import fetch_stock_zh_a_hist
from .base import SourceFetchResult, build_source_metadata, write_source_fetch_result

__all__ = [
    "SourceFetchResult",
    "build_source_metadata",
    "write_source_fetch_result",
    "fetch_stock_zh_a_hist",
    "fetch_stock_zh_index_hist_csindex",
    "fetch_stock_yysj_em",
    "fetch_stock_margin_detail_sse",
    "fetch_stock_margin_detail_szse",
    "fetch_stock_industry_clf_hist_sw",
    "fetch_index_component_sw",
    "fetch_index_hist_sw",
    "fetch_stock_industry_change_cninfo",
    "fetch_sw_index_first_info",
    "fetch_sw_index_second_info",
    "fetch_sw_index_third_info",
    "fetch_stock_board_industry_index_ths",
    "fetch_stock_board_concept_index_ths",
    "fetch_stock_board_concept_summary_ths",
]
