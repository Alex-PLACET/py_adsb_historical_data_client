# import pyreadsb
from collections.abc import Generator
from datetime import datetime
from logging import Logger
from typing import Final

import numpy as np
import requests
from pyreadsb.heatmap_decoder import HeatmapDecoder
from pyreadsb.traces_decoder import TraceEntry, process_traces_from_json_bytes

from .logger_config import get_logger

logger: Logger = get_logger(__name__)

ADSBEXCHANGE_HISTORICAL_DATA_URL = "https://globe.adsbexchange.com/globe_history/"


def download_heatmap(timestamp: datetime) -> bytes:
    """
    Download the heatmap for a given timestamp.
    :param timestamp: The timestamp to download the heatmap for.
    :return: The path to the downloaded heatmap file.
    """
    date_str: Final[str] = timestamp.strftime("%Y/%m/%d")
    filename: Final[int] = timestamp.hour * 2 + (timestamp.minute // 30)
    url: Final[str] = f"{ADSBEXCHANGE_HISTORICAL_DATA_URL}{date_str}/heatmap/{filename}.bin.ttf"

    logger.info(f"Downloading heatmap from {url}")

    try:
        response: Final[requests.Response] = requests.get(url)

        if response.status_code == 200:
            content = response.content
            if type(content) is not bytes:
                error_msg = f"Expected bytes, got {type(content)} from {url} for timestamp {timestamp}"
                logger.error(error_msg)
                raise TypeError(error_msg)
            logger.debug(f"Successfully downloaded heatmap, size: {len(content)} bytes")
            return content
        else:
            error_msg = f"Failed to download heatmap {url}: {response.status_code}"
            logger.error(error_msg)
            raise Exception(error_msg)
    except requests.RequestException as e:
        logger.error(f"Network error downloading heatmap from {url}: {e}")
        raise


def get_heatmap(
    timestamp: datetime,
) -> Generator[
    HeatmapDecoder.HeatEntry | HeatmapDecoder.CallsignEntry | HeatmapDecoder.TimestampSeparator,
    None,
    None,
]:
    data: Final[bytes] = download_heatmap(timestamp)
    heatmap_decoder: Final[HeatmapDecoder] = HeatmapDecoder(False)
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

    # Convert to numpy arrays and radians
    lat1_rad = np.radians(lat1)
    lat2_rad = np.radians(lat2)
    delta_lat = np.radians(lat2 - lat1)
    delta_lon = np.radians(lon2 - lon1)

    # Vectorized Haversine formula
    a = np.sin(delta_lat / 2) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    # Earth radius in meters
    distance = 6371000.0 * c

    return float(distance)


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


def get_heatmap_entries(timestamp: datetime) -> Generator[FullHeatmapEntry]:
    """
    Get zoned heatmap entries for a given timestamp.
    :param timestamp: The timestamp to get the heatmap entries for.
    :return: A generator of zoned heatmap entries.
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
            current_timestamp = datetime.fromtimestamp(entry.timestamp)
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
) -> Generator[FullHeatmapEntry]:
    """
    Get a zoned heatmap for a given timestamp, latitude, longitude, and radius.
    :param timestamp: The timestamp to get the heatmap for.
    :param latitude: The latitude of the center of the zone.
    :param longitude: The longitude of the center of the zone.
    :param radius: The radius of the zone in meters.
    :return: A zoned heatmap object.
    """
    heatmap_entries = get_heatmap_entries(timestamp)
    for entry in heatmap_entries:
        if is_valid_location((latitude, longitude), radius, (entry.lat, entry.lon)):
            yield entry


def download_traces(icao: str, timestamp: datetime) -> bytes:
    """
    Download the trace for a given ICAO and timestamp.
    :param icao: The ICAO code of the aircraft.
    :param timestamp: The timestamp to download the trace for.
    :return: The path to the downloaded trace file.
    """
    date_str: Final[str] = timestamp.strftime("%Y/%m/%d")
    sub_folder: Final[str] = icao.lower()[-2:]
    filename: Final[str] = f"trace_full_{icao.lower()}.json"
    url: Final[str] = f"{ADSBEXCHANGE_HISTORICAL_DATA_URL}{date_str}/traces/{sub_folder}/{filename}"

    # Enhanced browser-like headers to avoid 403 errors
    headers: Final[dict[str, str]] = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",  # noqa: E501
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

    logger.info(f"Downloading trace for ICAO {icao} from {url}")

    try:
        # Use a session to maintain cookies and connection state
        with requests.Session() as session:
            # Set session headers
            session.headers.update(headers)

            # Make the request
            response: Final[requests.Response] = session.get(url, timeout=30)

            if response.status_code == 200:
                logger.debug(f"Successfully downloaded trace for {icao}, size: {len(response.content)} bytes")
                return response.content
            else:
                error_msg = f"Failed to download trace {url}: {response.status_code}"
                logger.error(error_msg)
                raise Exception(error_msg)
    except requests.RequestException as e:
        logger.error(f"Network error downloading trace for {icao} from {url}: {e}")
        raise


def get_traces(icao: str, timestamp: datetime) -> Generator[TraceEntry]:
    """
    Get the trace for a given ICAO and timestamp.
    :param icao: The ICAO code of the aircraft.
    :param timestamp: The timestamp to get the trace for.
    :return: A generator yielding trace entries.
    """
    logger.debug(f"Getting traces for ICAO {icao} at timestamp {timestamp}")
    data: Final[bytes] = download_traces(icao, timestamp)
    return process_traces_from_json_bytes(data)
