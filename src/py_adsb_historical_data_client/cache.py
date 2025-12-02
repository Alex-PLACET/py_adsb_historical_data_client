"""
Cache module for storing downloaded ADSB historical data.

This module provides a file-based caching mechanism to avoid redundant downloads
of heatmap and trace data from the ADSB Exchange historical data API.
"""

import shutil
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Final

from .logger_config import get_logger

logger: Logger = get_logger(__name__)


class Cache:
    """
    A file-based cache for storing downloaded ADSB data.

    The cache stores files in a structured directory hierarchy:
    - heatmaps: {cache_path}/heatmaps/{YYYY}/{MM}/{DD}/{filename}.bin.ttf
    - traces: {cache_path}/traces/{YYYY}/{MM}/{DD}/{subfolder}/{filename}.json

    Example:
        cache = Cache("/path/to/cache")

        # Check if data exists
        if cache.has_heatmap(timestamp):
            data = cache.get_heatmap(timestamp)
        else:
            data = download_heatmap(timestamp)
            cache.put_heatmap(timestamp, data)
    """

    def __init__(self, cache_path: str | Path) -> None:
        """
        Initialize the cache.

        :param cache_path: The path to the cache directory.
                          Will be created if it doesn't exist.
        """
        self._cache_path = Path(cache_path)
        self._cache_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cache initialized at {self._cache_path}")

    @property
    def cache_path(self) -> Path:
        """Return the cache directory path."""
        return self._cache_path

    def _get_heatmap_path(self, timestamp: datetime) -> Path:
        """Get the cache file path for a heatmap."""
        date_path = timestamp.strftime("%Y/%m/%d")
        filename: Final[int] = timestamp.hour * 2 + (timestamp.minute // 30)
        return self._cache_path / "heatmaps" / date_path / f"{filename}.bin.ttf"

    def _get_trace_path(self, icao: str, timestamp: datetime) -> Path:
        """Get the cache file path for a trace."""
        date_path = timestamp.strftime("%Y/%m/%d")
        sub_folder: Final[str] = icao.lower()[-2:]
        filename: Final[str] = f"trace_full_{icao.lower()}.json"
        return self._cache_path / "traces" / date_path / sub_folder / filename

    def has_heatmap(self, timestamp: datetime) -> bool:
        """
        Check if a heatmap is cached.

        :param timestamp: The timestamp of the heatmap.
        :return: True if the heatmap is cached, False otherwise.
        """
        path = self._get_heatmap_path(timestamp)
        exists = path.exists()
        if exists:
            logger.debug(f"Cache hit for heatmap at {timestamp}")
        return exists

    def get_heatmap(self, timestamp: datetime) -> bytes | None:
        """
        Get a cached heatmap.

        :param timestamp: The timestamp of the heatmap.
        :return: The heatmap data as bytes, or None if not cached.
        """
        path = self._get_heatmap_path(timestamp)
        try:
            data = path.read_bytes()
            logger.debug(f"Reading heatmap from cache: {path}")
            return data
        except FileNotFoundError:
            return None

    def put_heatmap(self, timestamp: datetime, data: bytes) -> None:
        """
        Store a heatmap in the cache.

        :param timestamp: The timestamp of the heatmap.
        :param data: The heatmap data as bytes.
        """
        path = self._get_heatmap_path(timestamp)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        logger.debug(f"Cached heatmap at {path} ({len(data)} bytes)")

    def has_trace(self, icao: str, timestamp: datetime) -> bool:
        """
        Check if a trace is cached.

        :param icao: The ICAO code of the aircraft.
        :param timestamp: The timestamp (date) of the trace.
        :return: True if the trace is cached, False otherwise.
        """
        path = self._get_trace_path(icao, timestamp)
        exists = path.exists()
        if exists:
            logger.debug(f"Cache hit for trace {icao} at {timestamp.date()}")
        return exists

    def get_trace(self, icao: str, timestamp: datetime) -> bytes | None:
        """
        Get a cached trace.

        :param icao: The ICAO code of the aircraft.
        :param timestamp: The timestamp (date) of the trace.
        :return: The trace data as bytes, or None if not cached.
        """
        path = self._get_trace_path(icao, timestamp)
        try:
            data = path.read_bytes()
            logger.debug(f"Reading trace from cache: {path}")
            return data
        except FileNotFoundError:
            return None

    def put_trace(self, icao: str, timestamp: datetime, data: bytes) -> None:
        """
        Store a trace in the cache.

        :param icao: The ICAO code of the aircraft.
        :param timestamp: The timestamp (date) of the trace.
        :param data: The trace data as bytes.
        """
        path = self._get_trace_path(icao, timestamp)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        logger.debug(f"Cached trace at {path} ({len(data)} bytes)")

    def clear(self) -> None:
        """
        Clear all cached data.

        Warning: This will delete all files in the cache directory.
        """
        if self._cache_path.exists():
            shutil.rmtree(self._cache_path)
            self._cache_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Cache cleared at {self._cache_path}")

    def get_cache_size(self) -> int:
        """
        Get the total size of the cache in bytes.

        :return: The total size of all cached files in bytes.
        """
        total_size = 0
        for file_path in self._cache_path.rglob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size
        return total_size

    def get_cache_stats(self) -> dict[str, int]:
        """
        Get statistics about the cache.

        :return: A dictionary with cache statistics.
        """
        heatmap_count = 0
        trace_count = 0
        heatmap_size = 0
        trace_size = 0

        heatmaps_path = self._cache_path / "heatmaps"
        traces_path = self._cache_path / "traces"

        if heatmaps_path.exists():
            for file_path in heatmaps_path.rglob("*.bin.ttf"):
                if file_path.is_file():
                    heatmap_count += 1
                    heatmap_size += file_path.stat().st_size

        if traces_path.exists():
            for file_path in traces_path.rglob("*.json"):
                if file_path.is_file():
                    trace_count += 1
                    trace_size += file_path.stat().st_size

        return {
            "heatmap_count": heatmap_count,
            "trace_count": trace_count,
            "heatmap_size_bytes": heatmap_size,
            "trace_size_bytes": trace_size,
            "total_size_bytes": heatmap_size + trace_size,
        }


# Global cache instance (can be set by user)
_global_cache: Cache | None = None


def set_cache(cache_path: str | Path) -> Cache:
    """
    Set the global cache path.

    :param cache_path: The path to the cache directory.
    :return: The created Cache instance.

    Example:
        from py_adsb_historical_data_client import set_cache

        set_cache("/path/to/cache")
        # Now all downloads will be cached automatically
    """
    global _global_cache
    _global_cache = Cache(cache_path)
    return _global_cache


def get_cache() -> Cache | None:
    """
    Get the global cache instance.

    :return: The global Cache instance, or None if not set.
    """
    return _global_cache


def clear_cache() -> None:
    """
    Clear the global cache and disable caching.
    """
    global _global_cache
    if _global_cache is not None:
        _global_cache.clear()
    _global_cache = None


def disable_cache() -> None:
    """
    Disable the global cache without clearing it.
    """
    global _global_cache
    _global_cache = None
