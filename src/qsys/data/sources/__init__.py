"""Raw source adapter layer for pilot AkShare sources."""

from .akshare_disclosure import fetch_stock_yysj_em
from .akshare_index import fetch_stock_zh_index_hist_csindex
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
]
