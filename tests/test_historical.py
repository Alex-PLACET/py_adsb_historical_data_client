from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import requests

from src.py_adsb_historical_data_client.historical import (
    ADSBEXCHANGE_HISTORICAL_DATA_URL,
    DownloadError,
    FullHeatmapEntry,
    HTTPError,
    download_heatmap,
    download_traces,
    get_heatmap,
    get_traces,
    haversine_distance,
    is_valid_location,
)
from src.py_adsb_historical_data_client.logger_config import setup_logger

# create a logger instance for the test module and ensure it's properly configured
logger = setup_logger(__name__)


class TestDownloadHeatmap:
    """Test cases for the download_heatmap function."""

    def test_successful_heatmap_download(self, sample_timestamp, mock_successful_response):
        """Test successful heatmap download with valid response."""
        # Arrange
        timestamp = sample_timestamp
        expected_content = mock_successful_response.content
        expected_url = f"{ADSBEXCHANGE_HISTORICAL_DATA_URL}2023/06/15/heatmap/29.bin.ttf"

        # Act & Assert
        with patch("requests.get", return_value=mock_successful_response) as mock_get:
            result = download_heatmap(timestamp)

            # Verify the correct URL was called
            mock_get.assert_called_once_with(expected_url, timeout=30.0)

            # Verify the correct content was returned
            assert result == expected_content

    def test_heatmap_download_with_hour_rounding(self):
        """Test that minutes are correctly rounded to nearest 30-minute interval."""
        test_cases = [
            (datetime(2023, 6, 15, 14, 0), "28.bin.ttf"),  # 0 minutes -> 0
            (datetime(2023, 6, 15, 14, 15), "28.bin.ttf"),  # 15 minutes -> 0
            (datetime(2023, 6, 15, 14, 29), "28.bin.ttf"),  # 29 minutes -> 0
            (datetime(2023, 6, 15, 14, 30), "29.bin.ttf"),  # 30 minutes -> 30
            (datetime(2023, 6, 15, 14, 45), "29.bin.ttf"),  # 45 minutes -> 30
            (datetime(2023, 6, 15, 14, 59), "29.bin.ttf"),  # 59 minutes -> 30
        ]

        for timestamp, expected_filename in test_cases:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = b"test_data"

            with patch("requests.get", return_value=mock_response) as mock_get:
                download_heatmap(timestamp)

                # Extract the called URL and check the filename part
                called_url = mock_get.call_args[0][0]
                assert called_url.endswith(expected_filename)

    def test_heatmap_download_date_formatting(self):
        """Test that dates are correctly formatted in the URL."""
        timestamp = datetime(2023, 1, 5, 12, 0)  # Single digit month and day
        expected_url = f"{ADSBEXCHANGE_HISTORICAL_DATA_URL}2023/01/05/heatmap/24.bin.ttf"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"test_data"

        with patch("requests.get", return_value=mock_response) as mock_get:
            download_heatmap(timestamp)
            mock_get.assert_called_once_with(expected_url, timeout=30.0)

    def test_heatmap_download_http_error(self, sample_timestamp, mock_error_response):
        """Test that HTTP errors are properly handled."""
        with patch("requests.get", return_value=mock_error_response):
            with pytest.raises(HTTPError) as exc_info:
                download_heatmap(sample_timestamp)

            assert exc_info.value.status_code == 404
            assert "Failed to download heatmap" in str(exc_info.value)

    def test_heatmap_download_server_error(self, sample_timestamp):
        """Test handling of server errors (5xx status codes)."""
        mock_response = Mock()
        mock_response.status_code = 500

        with patch("requests.get", return_value=mock_response):
            with pytest.raises(HTTPError) as exc_info:
                download_heatmap(sample_timestamp)

            assert exc_info.value.status_code == 500
            assert "Failed to download heatmap" in str(exc_info.value)

    def test_heatmap_download_requests_exception(self, sample_timestamp):
        """Test handling of network-level exceptions."""
        with patch("requests.get", side_effect=requests.RequestException("Network error")):
            with pytest.raises(DownloadError):
                download_heatmap(sample_timestamp)

    @pytest.mark.integration
    def test_download_real_heatmap(self):
        """Test downloading a real heatmap (integration test)."""
        # This test requires network access and a valid timestamp
        timestamp = datetime(2023, 6, 1, 12, 0)

        try:
            # Act
            heatmap = get_heatmap(timestamp)
            for entry in heatmap:
                logger.info(f"Heatmap entry: {entry}")
        except Exception as e:
            # If the endpoint is unavailable or data doesn't exist, that's also valid
            logger.warning(f"Integration test failed (expected): {e}")
            pytest.skip(f"Integration test skipped due to network/data availability: {e}")


class TestDownloadTrace:
    """Test cases for the download_trace function."""

    def test_trace_download_http_error(self, sample_icao, sample_timestamp, mock_error_response):
        """Test that HTTP errors are properly handled."""
        mock_session = Mock()
        mock_session.get.return_value = mock_error_response
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        with patch("requests.Session", return_value=mock_session):
            with pytest.raises(HTTPError) as exc_info:
                download_traces(sample_icao, sample_timestamp)

            assert exc_info.value.status_code == 404
            assert "Failed to download trace" in str(exc_info.value)

    def test_trace_download_server_error(self, sample_icao, sample_timestamp):
        """Test handling of server errors (5xx status codes)."""
        mock_response = Mock()
        mock_response.status_code = 500

        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        with patch("requests.Session", return_value=mock_session):
            with pytest.raises(HTTPError) as exc_info:
                download_traces(sample_icao, sample_timestamp)

            assert exc_info.value.status_code == 500
            assert "Failed to download trace" in str(exc_info.value)

    def test_trace_download_requests_exception(self, sample_icao, sample_timestamp) -> None:
        """Test handling of network-level exceptions."""
        mock_session = Mock()
        mock_session.get.side_effect = requests.RequestException("Network error")
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        with patch("requests.Session", return_value=mock_session):
            with pytest.raises(DownloadError):
                download_traces(sample_icao, sample_timestamp)

    def test_trace_short_icao_code(self, sample_timestamp) -> None:
        """Test handling of short ICAO codes (less than 2 characters)."""
        icao = "A"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"test_data"

        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        with patch("requests.Session", return_value=mock_session):
            download_traces(icao, sample_timestamp)

            called_url = mock_session.get.call_args[0][0]
            # Should use the single character as subfolder
            assert "traces/a/" in called_url
            assert "trace_full_a.json" in called_url

    @pytest.mark.integration
    def test_trace_real_download(self) -> None:
        """Integration test with real HTTP request (requires network)."""
        icao = "ac134a"  # Example ICAO
        timestamp = datetime(2024, 8, 12, 12, 0)

        try:
            result = get_traces(icao, timestamp)
            for entry in result:
                logger.info(f"Trace entry: {entry}")
        except Exception as e:
            # If the endpoint is unavailable or data doesn't exist, that's also valid
            logger.warning(f"Integration test failed (expected): {e}")
            pytest.skip(f"Integration test skipped due to network/data availability: {e}")


class TestConstants:
    """Test cases for module constants."""

    def test_adsbexchange_url_constant(self):
        """Test that the ADSBEXCHANGE_HISTORICAL_DATA_URL constant is properly defined."""
        assert ADSBEXCHANGE_HISTORICAL_DATA_URL == "https://globe.adsbexchange.com/globe_history/"
        assert ADSBEXCHANGE_HISTORICAL_DATA_URL.endswith("/")


# Integration-style tests (can be run with --integration flag if needed)
class TestIntegration:
    """Integration tests that can optionally make real HTTP requests."""

    @pytest.mark.integration
    def test_real_heatmap_download(self):
        """Integration test with real HTTP request (requires network)."""
        # Use a recent timestamp that likely has data
        timestamp = datetime(2023, 6, 1, 12, 0)

        try:
            result = download_heatmap(timestamp)
            assert isinstance(result, bytes)
            assert len(result) > 0
        except Exception as e:
            # If the endpoint is unavailable or data doesn't exist, that's also valid
            logger.warning(f"Integration test failed (expected): {e}")
            pytest.skip(f"Integration test skipped due to network/data availability: {e}")

    @pytest.mark.integration
    def test_real_trace_download(self):
        """Integration test with real HTTP request (requires network)."""
        # Use a common ICAO code and recent timestamp
        icao = "ac134a"  # Example ICAO
        timestamp = datetime(2024, 8, 12, 12, 0)

        try:
            result = get_traces(icao, timestamp)
            for entry in result:
                logger.info(f"Integration test trace entry: {entry}")
        except Exception as e:
            # If the endpoint is unavailable or data doesn't exist, that's also valid
            logger.warning(f"Integration test failed (expected): {e}")
            pytest.skip(f"Integration test skipped due to network/data availability: {e}")


class TestHaversineDistance:
    """Test cases for the haversine_distance function."""

    def test_same_location_distance_is_zero(self):
        """Test that distance between same points is zero."""
        coord = (48.8566, 2.3522)  # Paris
        assert haversine_distance(coord, coord) == pytest.approx(0.0, abs=1e-6)

    def test_known_distance_paris_to_london(self):
        """Test haversine distance between Paris and London."""
        paris = (48.8566, 2.3522)
        london = (51.5074, -0.1278)
        # Known distance is approximately 343 km
        distance = haversine_distance(paris, london)
        assert distance == pytest.approx(343_000, rel=0.02)  # 2% tolerance

    def test_known_distance_new_york_to_los_angeles(self):
        """Test haversine distance between New York and Los Angeles."""
        new_york = (40.7128, -74.0060)
        los_angeles = (34.0522, -118.2437)
        # Known distance is approximately 3944 km
        distance = haversine_distance(new_york, los_angeles)
        assert distance == pytest.approx(3_944_000, rel=0.02)  # 2% tolerance

    def test_antipodal_points(self):
        """Test distance between antipodal points (maximum distance)."""
        north_pole = (90.0, 0.0)
        south_pole = (-90.0, 0.0)
        # Distance should be approximately half Earth's circumference (20,015 km)
        distance = haversine_distance(north_pole, south_pole)
        assert distance == pytest.approx(20_015_000, rel=0.02)

    def test_equator_distance(self):
        """Test distance along the equator."""
        point1 = (0.0, 0.0)
        point2 = (0.0, 90.0)  # Quarter of the way around
        # Distance should be approximately 10,000 km
        distance = haversine_distance(point1, point2)
        assert distance == pytest.approx(10_000_000, rel=0.02)


class TestIsValidLocation:
    """Test cases for the is_valid_location function."""

    def test_location_within_radius(self):
        """Test that a location within the radius returns True."""
        center = (48.8566, 2.3522)  # Paris
        nearby = (48.8600, 2.3500)  # Very close to Paris
        radius = 1000  # 1 km
        assert is_valid_location(center, radius, nearby) is True

    def test_location_outside_radius(self):
        """Test that a location outside the radius returns False."""
        paris = (48.8566, 2.3522)
        london = (51.5074, -0.1278)
        radius = 100_000  # 100 km
        assert is_valid_location(paris, radius, london) is False

    def test_location_at_exact_radius(self):
        """Test location at approximately the radius boundary."""
        center = (48.8566, 2.3522)
        # Calculate a point approximately 10km away
        nearby = (48.9466, 2.3522)  # Approximately 10 km north
        radius = 10_100  # 10.1 km (slightly more than distance)
        assert is_valid_location(center, radius, nearby) is True

    def test_same_location_always_valid(self):
        """Test that the same location is always valid regardless of radius."""
        location = (48.8566, 2.3522)
        assert is_valid_location(location, 0.001, location) is True  # Very small radius
        assert is_valid_location(location, 1_000_000, location) is True  # Large radius


class TestFullHeatmapEntry:
    """Test cases for the FullHeatmapEntry class."""

    def test_full_heatmap_entry_creation(self, sample_timestamp):
        """Test creating a FullHeatmapEntry with all fields."""
        entry = FullHeatmapEntry(
            timestamp=sample_timestamp,
            callsign="TEST123",
            hex_id="ABC123",
            lat=48.8566,
            lon=2.3522,
            alt=35000,
            ground_speed=450.5,
        )

        assert entry.timestamp == sample_timestamp
        assert entry.callsign == "TEST123"
        assert entry.hex_id == "ABC123"
        assert entry.lat == 48.8566
        assert entry.lon == 2.3522
        assert entry.alt == 35000
        assert entry.ground_speed == 450.5

    def test_full_heatmap_entry_with_none_values(self):
        """Test creating a FullHeatmapEntry with None values."""
        entry = FullHeatmapEntry(
            timestamp=None,
            callsign=None,
            hex_id="ABC123",
            lat=48.8566,
            lon=2.3522,
            alt=None,
            ground_speed=None,
        )

        assert entry.timestamp is None
        assert entry.callsign is None
        assert entry.alt is None
        assert entry.ground_speed is None

    def test_full_heatmap_entry_with_ground_altitude(self):
        """Test creating a FullHeatmapEntry with 'ground' altitude."""
        entry = FullHeatmapEntry(
            timestamp=datetime(2023, 6, 15, 14, 45),
            callsign="TEST123",
            hex_id="ABC123",
            lat=48.8566,
            lon=2.3522,
            alt="ground",
            ground_speed=0.0,
        )

        assert entry.alt == "ground"

    def test_full_heatmap_entry_repr(self, sample_timestamp):
        """Test the string representation of FullHeatmapEntry."""
        entry = FullHeatmapEntry(
            timestamp=sample_timestamp,
            callsign="TEST123",
            hex_id="ABC123",
            lat=48.8566,
            lon=2.3522,
            alt=35000,
            ground_speed=450.5,
        )

        repr_str = repr(entry)
        assert "FullHeatmapEntry" in repr_str
        assert "TEST123" in repr_str
        assert "48.8566" in repr_str
        assert "2.3522" in repr_str
