# py_adsb_historical_data_client

Python client to retrieve historical data from adsb databases

## Installation

```bash
pip install py-adsb-historical-data-client
```

## Usage

```python
from datetime import datetime
from py_adsb_historical_data_client.historical import download_heatmap, download_trace

# Download heatmap data for a specific timestamp
timestamp = datetime(2023, 6, 15, 14, 30)
heatmap_data = download_heatmap(timestamp)

# Download trace data for a specific aircraft
icao = "ABC123"
trace_data = download_trace(icao, timestamp)
```

## Development

### Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   uv sync
   ```

### Running Tests

The project includes comprehensive tests for all functionality.

#### Run all tests except integration tests (recommended for development):
```bash
# Using uv directly
uv run pytest tests/ -m "not integration" -v

# Using the test runner script
python run_tests.py
```

#### Run all tests including integration tests:
```bash
# Using uv directly
uv run pytest tests/ -v

# Using the test runner script
python run_tests.py --all
```

#### Run tests with coverage report:
```bash
# Using uv directly
uv run pytest tests/ --cov=src/py_adsb_historical_data_client --cov-report=html --cov-report=term

# Using the test runner script
python run_tests.py --coverage
```

### Test Structure

- **Unit Tests**: Mock external HTTP requests and test all code paths
- **Integration Tests**: Make real HTTP requests to test actual API endpoints (marked with `@pytest.mark.integration`)
- **Test Coverage**: Comprehensive coverage including edge cases, error handling, and input validation

### Available Test Commands

| Command | Description |
|---------|-------------|
| `python run_tests.py` | Run unit tests only |
| `python run_tests.py --all` | Run all tests including integration tests |
| `python run_tests.py --coverage` | Run tests with coverage report |

The test suite covers:
- ✅ Successful data downloads
- ✅ HTTP error handling (404, 500, etc.)
- ✅ Network exception handling
- ✅ Date formatting and URL construction
- ✅ ICAO code processing and case handling
- ✅ Time rounding for heatmap intervals
- ✅ Edge cases (short ICAO codes, etc.)
