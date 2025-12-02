# import pyreadsb
import math
from collections.abc import Generator
from datetime import UTC, datetime
from logging import Logger
from typing import TYPE_CHECKING, Final

import requests
from pyreadsb.heatmap_decoder import HeatmapDecoder
from pyreadsb.traces_decoder import TraceEntry, process_traces_from_json_bytes

from .logger_config import get_logger

if TYPE_CHECKING:
    from .cache import Cache

logger: Logger = get_logger(__name__)

ADSBEXCHANGE_HISTORICAL_DATA_URL = "https://globe.adsbexchange.com/globe_history/"


class ADSBClientError(Exception):
    """Base exception for ADSB client errors."""

    pass


class DownloadError(ADSBClientError):
    """Exception raised when a download fails."""

    pass


class HTTPError(DownloadError):
    """Exception raised when an HTTP request fails."""

    def __init__(self, url: str, status_code: int, message: str | None = None) -> None:
        self.url = url
        self.status_code = status_code
        super().__init__(message or f"HTTP {status_code} error for {url}")


def download_heatmap(
    timestamp: datetime,
    timeout: float = 30.0,
    cache: "Cache | None" = None,
) -> bytes:
    """
    Download the heatmap for a given timestamp.

    :param timestamp: The timestamp to download the heatmap for.
    :param timeout: Request timeout in seconds.
    :param cache: Optional cache instance. If None, uses global cache if set.
    :return: The heatmap data as bytes.
    """
    # Import here to avoid circular imports
    from .cache import get_cache

    # Use provided cache or fall back to global cache
    effective_cache = cache if cache is not None else get_cache()

    # Check cache first
    if effective_cache is not None:
        cached_data = effective_cache.get_heatmap(timestamp)
        if cached_data is not None:
            logger.debug(f"Using cached heatmap for {timestamp}")
            return cached_data

    date_str: Final[str] = timestamp.strftime("%Y/%m/%d")
    filename: Final[int] = timestamp.hour * 2 + (timestamp.minute // 30)
    url: Final[str] = f"{ADSBEXCHANGE_HISTORICAL_DATA_URL}{date_str}/heatmap/{filename}.bin.ttf"

    logger.info(f"Downloading heatmap from {url}")

    try:
        response: Final[requests.Response] = requests.get(url, timeout=timeout)

        if response.status_code == 200:
            content = response.content
            logger.debug(f"Successfully downloaded heatmap, size: {len(content)} bytes")

            # Store in cache
            if effective_cache is not None:
                effective_cache.put_heatmap(timestamp, content)

            return content
        else:
            error_msg = f"Failed to download heatmap {url}: {response.status_code}"
            logger.error(error_msg)
            raise HTTPError(url, response.status_code, error_msg)
    except requests.RequestException as e:
        logger.error(f"Network error downloading heatmap from {url}: {e}")
        raise DownloadError(f"Network error downloading heatmap from {url}: {e}") from e


def get_heatmap(
    timestamp: datetime,
) -> Generator[
    HeatmapDecoder.HeatEntry | HeatmapDecoder.CallsignEntry | HeatmapDecoder.TimestampSeparator,
    None,
    None,
]:
    data: Final[bytes] = download_heatmap(timestamp)
    heatmap_decoder: Final[HeatmapDecoder] = HeatmapDecoder()
    return heatmap_decoder.decode_from_bytes(data)


def haversine_distance(coord1: tuple[float, float], coord2: tuple[float, float]) -> float:
    """
    Calculate the Haversine distance between two geographical coordinates.
    :param coord1: A tuple containing the latitude and longitude of the first point.
    :param coord2: A tuple containing the latitude and longitude of the second point.
    :return: The Haversine distance in meters.
    """
    lat1, lon1 = coord1
    lat2, lon2 = coord2

    # Convert to radians using math module (faster for scalar values)
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Earth radius in meters
    return 6371000.0 * c


def is_valid_location(valid_location: tuple[float, float], radius: float, location: tuple[float, float]) -> bool:
    """
    Check if a given location is within a valid radius of a valid location.
    :param valid_location: A tuple containing the valid latitude and longitude.
    :param radius: The radius in meters within which the location is considered valid.
    :param location: A tuple containing the latitude and longitude to check.
    :return: True if the location is within the valid radius, False otherwise.
    """
    return haversine_distance(valid_location, location) <= radius


class FullHeatmapEntry(HeatmapDecoder.HeatEntry):
    """
    A full heatmap entry that includes the timestamp and callsign.
    """

    timestamp: datetime | None
    callsign: str | None

    def __init__(
        self,
        timestamp: datetime | None,
        callsign: str | None,
        hex_id: str,
        lat: float,
        lon: float,
        alt: int | str | None,
        ground_speed: float | None,
    ) -> None:
        super().__init__(hex_id, lat, lon, alt, ground_speed)
        self.timestamp = timestamp
        self.callsign = callsign

    def __repr__(self) -> str:
        return f"FullHeatmapEntry(timestamp={self.timestamp}, callsign={self.callsign}, lat={self.lat}, lon={self.lon})"


def get_heatmap_entries(timestamp: datetime) -> Generator[FullHeatmapEntry, None, None]:
    """
    Get heatmap entries for a given timestamp.
    :param timestamp: The timestamp to get the heatmap entries for.
    :return: A generator of heatmap entries.
    """
    heatmap_entries = get_heatmap(timestamp)
    icao_callsigns_map: dict[str, str | None] = {}
    current_timestamp: datetime = timestamp
    # rounds minutes by half hour
    current_timestamp = current_timestamp.replace(minute=(current_timestamp.minute // 30) * 30, second=0, microsecond=0)
    for entry in heatmap_entries:
        if isinstance(entry, HeatmapDecoder.CallsignEntry):
            icao_callsigns_map[entry.hex_id] = entry.callsign
        elif isinstance(entry, HeatmapDecoder.TimestampSeparator):
            current_timestamp = entry.timestamp.replace(tzinfo=UTC)
        elif isinstance(entry, HeatmapDecoder.HeatEntry):
            yield FullHeatmapEntry(
                timestamp=current_timestamp,
                callsign=icao_callsigns_map.get(entry.hex_id),
                hex_id=entry.hex_id,
                lat=entry.lat,
                lon=entry.lon,
                alt=entry.alt,
                ground_speed=entry.ground_speed,
            )


def get_zoned_heatmap_entries(
    timestamp: datetime, latitude: float, longitude: float, radius: float
) -> Generator[FullHeatmapEntry, None, None]:
    """
    Get a zoned heatmap for a given timestamp, latitude, longitude, and radius.
    :param timestamp: The timestamp to get the heatmap for.
    :param latitude: The latitude of the center of the zone.
    :param longitude: The longitude of the center of the zone.
    :param radius: The radius of the zone in meters.
    :return: A zoned heatmap object.
    """
    # Pre-compute bounding box for fast rejection (approximate)
    # 1 degree of latitude ≈ 111,320 meters
    # 1 degree of longitude ≈ 111,320 * cos(latitude) meters
    lat_delta = radius / 111320.0
    lon_delta = radius / (111320.0 * math.cos(math.radians(latitude)))

    min_lat = latitude - lat_delta
    max_lat = latitude + lat_delta
    min_lon = longitude - lon_delta
    max_lon = longitude + lon_delta

    # Pre-compute center point values for haversine
    center_lat_rad = math.radians(latitude)
    cos_center_lat = math.cos(center_lat_rad)

    heatmap_entries = get_heatmap_entries(timestamp)
    for entry in heatmap_entries:
        # Fast bounding box rejection
        if not (min_lat <= entry.lat <= max_lat and min_lon <= entry.lon <= max_lon):
            continue

        # Precise haversine check (inlined and optimized)
        lat_rad = math.radians(entry.lat)
        delta_lat = lat_rad - center_lat_rad
        delta_lon = math.radians(entry.lon - longitude)

        a = math.sin(delta_lat / 2) ** 2 + cos_center_lat * math.cos(lat_rad) * math.sin(delta_lon / 2) ** 2
        distance = 6371000.0 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        if distance <= radius:
            yield entry


# Module-level headers constant to avoid recreating dict on every call
_TRACE_HEADERS: Final[dict[str, str]] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://globe.adsbexchange.com/",
    "Origin": "https://globe.adsbexchange.com",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def download_traces(
    icao: str,
    timestamp: datetime,
    cache: "Cache | None" = None,
) -> bytes:
    """
    Download the trace for a given ICAO and timestamp.

    :param icao: The ICAO code of the aircraft.
    :param timestamp: The timestamp to download the trace for.
    :param cache: Optional cache instance. If None, uses global cache if set.
    :return: The trace data as bytes.
    """
    # Import here to avoid circular imports
    from .cache import get_cache

    # Use provided cache or fall back to global cache
    effective_cache = cache if cache is not None else get_cache()

    # Check cache first
    if effective_cache is not None:
        cached_data = effective_cache.get_trace(icao, timestamp)
        if cached_data is not None:
            logger.debug(f"Using cached trace for {icao} at {timestamp.date()}")
            return cached_data

    date_str: Final[str] = timestamp.strftime("%Y/%m/%d")
    sub_folder: Final[str] = icao.lower()[-2:]
    filename: Final[str] = f"trace_full_{icao.lower()}.json"
    url: Final[str] = f"{ADSBEXCHANGE_HISTORICAL_DATA_URL}{date_str}/traces/{sub_folder}/{filename}"

    logger.info(f"Downloading trace for ICAO {icao} from {url}")

    try:
        # Use a session to maintain cookies and connection state
        with requests.Session() as session:
            # Set session headers (use module-level constant)
            session.headers.update(_TRACE_HEADERS)

            # Make the request
            response: Final[requests.Response] = session.get(url, timeout=30)

            if response.status_code == 200:
                logger.debug(f"Successfully downloaded trace for {icao}, size: {len(response.content)} bytes")

                # Store in cache
                if effective_cache is not None:
                    effective_cache.put_trace(icao, timestamp, response.content)

                return response.content
            else:
                error_msg = f"Failed to download trace {url}: {response.status_code}"
                logger.error(error_msg)
                raise HTTPError(url, response.status_code, error_msg)
    except requests.RequestException as e:
        logger.error(f"Network error downloading trace for {icao} from {url}: {e}")
        raise DownloadError(f"Network error downloading trace for {icao} from {url}: {e}") from e


def get_traces(icao: str, timestamp: datetime) -> Generator[TraceEntry, None, None]:
    """
    Get the trace for a given ICAO and timestamp.
    :param icao: The ICAO code of the aircraft.
    :param timestamp: The timestamp to get the trace for.
    :return: A generator yielding trace entries.
    """
    logger.debug(f"Getting traces for ICAO {icao} at timestamp {timestamp}")
    data: Final[bytes] = download_traces(icao, timestamp)
    return process_traces_from_json_bytes(data)


class TraceSession:
    """
    A session-based trace downloader for efficient bulk trace downloads.

    Reuses HTTP connections across multiple requests, significantly improving
    performance when downloading traces for multiple aircraft.

    Example:
        with TraceSession() as session:
            for icao in icao_list:
                traces = session.get_traces(icao, timestamp)
                for trace in traces:
                    process(trace)

        # With caching:
        cache = Cache("/path/to/cache")
        with TraceSession(cache=cache) as session:
            traces = session.get_traces(icao, timestamp)
    """

    def __init__(
        self,
        timeout: float = 30.0,
        cache: "Cache | None" = None,
    ) -> None:
        """
        Initialize the trace session.

        :param timeout: Request timeout in seconds.
        :param cache: Optional cache instance. If None, uses global cache if set.
        """
        self._session: requests.Session | None = None
        self._timeout = timeout
        self._cache = cache

    def __enter__(self) -> "TraceSession":
        self._session = requests.Session()
        self._session.headers.update(_TRACE_HEADERS)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._session:
            self._session.close()
            self._session = None

    def download_traces(self, icao: str, timestamp: datetime) -> bytes:
        """
        Download the trace for a given ICAO and timestamp using the session.

        :param icao: The ICAO code of the aircraft.
        :param timestamp: The timestamp to download the trace for.
        :return: The trace data as bytes.
        """
        if self._session is None:
            msg = "TraceSession must be used as a context manager"
            raise RuntimeError(msg)

        # Import here to avoid circular imports
        from .cache import get_cache

        # Use provided cache or fall back to global cache
        effective_cache = self._cache if self._cache is not None else get_cache()

        # Check cache first
        if effective_cache is not None:
            cached_data = effective_cache.get_trace(icao, timestamp)
            if cached_data is not None:
                logger.debug(f"Using cached trace for {icao} at {timestamp.date()}")
                return cached_data

        date_str: Final[str] = timestamp.strftime("%Y/%m/%d")
        sub_folder: Final[str] = icao.lower()[-2:]
        filename: Final[str] = f"trace_full_{icao.lower()}.json"
        url: Final[str] = f"{ADSBEXCHANGE_HISTORICAL_DATA_URL}{date_str}/traces/{sub_folder}/{filename}"

        logger.info(f"Downloading trace for ICAO {icao} from {url}")

        try:
            response: Final[requests.Response] = self._session.get(url, timeout=self._timeout)

            if response.status_code == 200:
                logger.debug(f"Successfully downloaded trace for {icao}, size: {len(response.content)} bytes")

                # Store in cache
                if effective_cache is not None:
                    effective_cache.put_trace(icao, timestamp, response.content)

                return response.content
            else:
                error_msg = f"Failed to download trace {url}: {response.status_code}"
                logger.error(error_msg)
                raise HTTPError(url, response.status_code, error_msg)
        except requests.RequestException as e:
            logger.error(f"Network error downloading trace for {icao} from {url}: {e}")
            raise DownloadError(f"Network error downloading trace for {icao} from {url}: {e}") from e

    def get_traces(self, icao: str, timestamp: datetime) -> Generator[TraceEntry, None, None]:
        """
        Get the trace for a given ICAO and timestamp.

        :param icao: The ICAO code of the aircraft.
        :param timestamp: The timestamp to get the trace for.
        :return: A generator yielding trace entries.
        """
        logger.debug(f"Getting traces for ICAO {icao} at timestamp {timestamp}")
        data: Final[bytes] = self.download_traces(icao, timestamp)
        return process_traces_from_json_bytes(data)
