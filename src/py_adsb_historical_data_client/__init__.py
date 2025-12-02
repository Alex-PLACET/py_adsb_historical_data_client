from pyreadsb.heatmap_decoder import HeatmapDecoder
from pyreadsb.traces_decoder import TraceEntry

from .cache import (
    Cache,
    clear_cache,
    disable_cache,
    get_cache,
    set_cache,
)
from .historical import (
    ADSBClientError,
    DownloadError,
    FullHeatmapEntry,
    HTTPError,
    TraceSession,
    download_heatmap,
    download_traces,
    get_heatmap,
    get_heatmap_entries,
    get_traces,
    get_zoned_heatmap_entries,
    haversine_distance,
    is_valid_location,
)
from .logger_config import get_logger, setup_logger

__all__ = [
    # Cache
    "Cache",
    "set_cache",
    "get_cache",
    "clear_cache",
    "disable_cache",
    # Exceptions
    "ADSBClientError",
    "DownloadError",
    "HTTPError",
    # Session
    "TraceSession",
    # Logger
    "setup_logger",
    "get_logger",
    # Heatmap functions
    "download_heatmap",
    "get_heatmap",
    "get_heatmap_entries",
    "get_zoned_heatmap_entries",
    "FullHeatmapEntry",
    # Trace functions
    "download_traces",
    "get_traces",
    # Utilities
    "haversine_distance",
    "is_valid_location",
    # Re-exports from pyreadsb
    "TraceEntry",
    "HeatmapDecoder",
]
