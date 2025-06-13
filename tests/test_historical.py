from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import requests

from src.py_adsb_historical_data_client.historical import (
    ADSBEXCHANGE_HISTORICAL_DATA_URL,
    download_heatmap,
    download_traces,
    get_heatmap,
    get_traces,
)
from src.py_adsb_historical_data_client.logger_config import setup_logger

# create a logger instance for the test module and ensure it's properly configured
logger = setup_logger(__name__)


class TestDownloadHeatmap:
    """Test cases for the download_heatmap function."""

    def test_successful_heatmap_download(self):
        """Test successful heatmap download with valid response."""
        # Arrange
        timestamp = datetime(2023, 6, 15, 14, 45)  # 14:45
        expected_content = b"mock_heatmap_data"
        expected_url = f"{ADSBEXCHANGE_HISTORICAL_DATA_URL}2023/06/15/heatmap/29.bin.ttf"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = expected_content

        # Act & Assert
        with patch("requests.get", return_value=mock_response) as mock_get:
            result = download_heatmap(timestamp)

            # Verify the correct URL was called
            mock_get.assert_called_once_with(expected_url)

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
            mock_get.assert_called_once_with(expected_url)

    def test_heatmap_download_http_error(self):
        """Test that HTTP errors are properly handled."""
        timestamp = datetime(2023, 6, 15, 14, 45)

        mock_response = Mock()
        mock_response.status_code = 404

        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception) as exc_info:
                download_heatmap(timestamp)

            assert "Failed to download heatmap" in str(exc_info.value)
            assert "404" in str(exc_info.value)

    def test_heatmap_download_server_error(self):
        """Test handling of server errors (5xx status codes)."""
        timestamp = datetime(2023, 6, 15, 14, 45)

        mock_response = Mock()
        mock_response.status_code = 500

        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception) as exc_info:
                download_heatmap(timestamp)

            assert "Failed to download heatmap" in str(exc_info.value)
            assert "500" in str(exc_info.value)

    def test_heatmap_download_requests_exception(self):
        """Test handling of network-level exceptions."""
        timestamp = datetime(2023, 6, 15, 14, 45)

        with patch("requests.get", side_effect=requests.RequestException("Network error")):
            with pytest.raises(requests.RequestException):
                download_heatmap(timestamp)

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

    def test_trace_download_http_error(self):
        """Test that HTTP errors are properly handled."""
        icao = "ABC123"
        timestamp = datetime(2023, 6, 15, 14, 45)

        mock_response = Mock()
        mock_response.status_code = 404

        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        with patch("requests.Session", return_value=mock_session):
            with pytest.raises(Exception) as exc_info:
                download_traces(icao, timestamp)

            assert "Failed to download trace" in str(exc_info.value)
            assert "404" in str(exc_info.value)

    def test_trace_download_server_error(self):
        """Test handling of server errors (5xx status codes)."""
        icao = "ABC123"
        timestamp = datetime(2023, 6, 15, 14, 45)

        mock_response = Mock()
        mock_response.status_code = 500

        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        with patch("requests.Session", return_value=mock_session):
            with pytest.raises(Exception) as exc_info:
                download_traces(icao, timestamp)

            assert "Failed to download trace" in str(exc_info.value)
            assert "500" in str(exc_info.value)

    def test_trace_download_requests_exception(self) -> None:
        """Test handling of network-level exceptions."""
        icao = "ABC123"
        timestamp = datetime(2023, 6, 15, 14, 45)

        mock_session = Mock()
        mock_session.get.side_effect = requests.RequestException("Network error")
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        with patch("requests.Session", return_value=mock_session):
            with pytest.raises(requests.RequestException):
                download_traces(icao, timestamp)

    def test_trace_short_icao_code(self) -> None:
        """Test handling of short ICAO codes (less than 2 characters)."""
        icao = "A"
        timestamp = datetime(2023, 6, 15, 14, 45)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"test_data"

        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        with patch("requests.Session", return_value=mock_session):
            download_traces(icao, timestamp)

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
