"""
Tests for the cache module.
"""

from datetime import datetime
from pathlib import Path

from src.py_adsb_historical_data_client.cache import (
    Cache,
    clear_cache,
    disable_cache,
    get_cache,
    set_cache,
)


class TestCache:
    """Test cases for the Cache class."""

    def test_cache_initialization(self, tmp_path: Path) -> None:
        """Test that cache initializes and creates directory."""
        cache_path = tmp_path / "test_cache"
        cache = Cache(cache_path)

        assert cache.cache_path == cache_path
        assert cache_path.exists()

    def test_heatmap_cache_miss(self, tmp_path: Path) -> None:
        """Test that get_heatmap returns None for uncached data."""
        cache = Cache(tmp_path / "cache")
        timestamp = datetime(2023, 6, 15, 14, 45)

        result = cache.get_heatmap(timestamp)
        assert result is None

    def test_heatmap_cache_hit(self, tmp_path: Path) -> None:
        """Test that cached heatmap data is returned correctly."""
        cache = Cache(tmp_path / "cache")
        timestamp = datetime(2023, 6, 15, 14, 45)
        test_data = b"heatmap_test_data"

        # Store data
        cache.put_heatmap(timestamp, test_data)

        # Verify cache hit
        assert cache.has_heatmap(timestamp) is True
        result = cache.get_heatmap(timestamp)
        assert result == test_data

    def test_heatmap_cache_path_structure(self, tmp_path: Path) -> None:
        """Test that heatmap files are stored in correct path structure."""
        cache = Cache(tmp_path / "cache")
        timestamp = datetime(2023, 6, 15, 14, 45)  # hour 14, minute 45 -> filename 29
        test_data = b"test"

        cache.put_heatmap(timestamp, test_data)

        expected_path = tmp_path / "cache" / "heatmaps" / "2023" / "06" / "15" / "29.bin.ttf"
        assert expected_path.exists()
        assert expected_path.read_bytes() == test_data

    def test_trace_cache_miss(self, tmp_path: Path) -> None:
        """Test that get_trace returns None for uncached data."""
        cache = Cache(tmp_path / "cache")
        icao = "ABC123"
        timestamp = datetime(2023, 6, 15, 14, 45)

        result = cache.get_trace(icao, timestamp)
        assert result is None

    def test_trace_cache_hit(self, tmp_path: Path) -> None:
        """Test that cached trace data is returned correctly."""
        cache = Cache(tmp_path / "cache")
        icao = "ABC123"
        timestamp = datetime(2023, 6, 15, 14, 45)
        test_data = b'{"icao": "ABC123", "traces": []}'

        # Store data
        cache.put_trace(icao, timestamp, test_data)

        # Verify cache hit
        assert cache.has_trace(icao, timestamp) is True
        result = cache.get_trace(icao, timestamp)
        assert result == test_data

    def test_trace_cache_path_structure(self, tmp_path: Path) -> None:
        """Test that trace files are stored in correct path structure."""
        cache = Cache(tmp_path / "cache")
        icao = "ABC123"
        timestamp = datetime(2023, 6, 15, 14, 45)
        test_data = b'{"test": true}'

        cache.put_trace(icao, timestamp, test_data)

        # ICAO ABC123 -> subfolder "23", filename "trace_full_abc123.json"
        expected_path = tmp_path / "cache" / "traces" / "2023" / "06" / "15" / "23" / "trace_full_abc123.json"
        assert expected_path.exists()
        assert expected_path.read_bytes() == test_data

    def test_trace_icao_case_insensitive(self, tmp_path: Path) -> None:
        """Test that ICAO codes are normalized to lowercase."""
        cache = Cache(tmp_path / "cache")
        timestamp = datetime(2023, 6, 15)
        test_data = b"test"

        # Store with uppercase
        cache.put_trace("ABC123", timestamp, test_data)

        # Retrieve with lowercase
        result = cache.get_trace("abc123", timestamp)
        assert result == test_data

    def test_cache_clear(self, tmp_path: Path) -> None:
        """Test that cache clear removes all data."""
        cache = Cache(tmp_path / "cache")
        timestamp = datetime(2023, 6, 15, 14, 45)

        # Add some data
        cache.put_heatmap(timestamp, b"heatmap")
        cache.put_trace("ABC123", timestamp, b"trace")

        # Clear cache
        cache.clear()

        # Verify data is gone
        assert cache.get_heatmap(timestamp) is None
        assert cache.get_trace("ABC123", timestamp) is None

    def test_cache_stats(self, tmp_path: Path) -> None:
        """Test cache statistics."""
        cache = Cache(tmp_path / "cache")
        timestamp = datetime(2023, 6, 15, 14, 45)

        # Empty cache
        stats = cache.get_cache_stats()
        assert stats["heatmap_count"] == 0
        assert stats["trace_count"] == 0

        # Add data
        cache.put_heatmap(timestamp, b"heatmap_data")
        cache.put_trace("ABC123", timestamp, b"trace_data")

        stats = cache.get_cache_stats()
        assert stats["heatmap_count"] == 1
        assert stats["trace_count"] == 1
        assert stats["heatmap_size_bytes"] == len(b"heatmap_data")
        assert stats["trace_size_bytes"] == len(b"trace_data")

    def test_cache_size(self, tmp_path: Path) -> None:
        """Test total cache size calculation."""
        cache = Cache(tmp_path / "cache")
        timestamp = datetime(2023, 6, 15, 14, 45)

        heatmap_data = b"heatmap_data"
        trace_data = b"trace_data"

        cache.put_heatmap(timestamp, heatmap_data)
        cache.put_trace("ABC123", timestamp, trace_data)

        expected_size = len(heatmap_data) + len(trace_data)
        assert cache.get_cache_size() == expected_size


class TestGlobalCache:
    """Test cases for global cache functions."""

    def test_set_and_get_cache(self, tmp_path: Path) -> None:
        """Test setting and getting global cache."""
        try:
            cache = set_cache(tmp_path / "global_cache")
            assert get_cache() is cache
            assert cache.cache_path == tmp_path / "global_cache"
        finally:
            disable_cache()

    def test_disable_cache(self, tmp_path: Path) -> None:
        """Test disabling global cache."""
        try:
            set_cache(tmp_path / "cache")
            assert get_cache() is not None

            disable_cache()
            assert get_cache() is None
        finally:
            disable_cache()

    def test_clear_cache(self, tmp_path: Path) -> None:
        """Test clearing global cache."""
        try:
            cache = set_cache(tmp_path / "cache")
            cache.put_heatmap(datetime(2023, 6, 15), b"test")

            clear_cache()
            assert get_cache() is None
        finally:
            disable_cache()

    def test_get_cache_when_not_set(self) -> None:
        """Test that get_cache returns None when no cache is set."""
        try:
            disable_cache()
            assert get_cache() is None
        finally:
            disable_cache()
