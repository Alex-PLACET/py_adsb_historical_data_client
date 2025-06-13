#!/usr/bin/env python3
"""
Test runner script for the py-adsb-historical-data-client package.

Usage:
    python run_tests.py              # Run all tests except integration tests
    python run_tests.py --all        # Run all tests including integration tests
    python run_tests.py --coverage   # Run tests with coverage report
"""

import argparse
import subprocess
import sys

from src.py_adsb_historical_data_client.logger_config import get_logger, setup_logger

# Setup logging for the test runner
setup_logger("test_runner", level=20)  # INFO level
logger = get_logger("test_runner")


def run_tests(include_integration=False, coverage=False):
    """Run the test suite with specified options."""
    cmd = ["uv", "run", "pytest"]

    if not include_integration:
        cmd.extend(["-m", "not integration"])

    if coverage:
        cmd.extend(
            [
                "--cov=src/py_adsb_historical_data_client",
                "--cov-report=html",
                "--cov-report=term",
            ]
        )

    cmd.extend(["-v", "tests/"])

    logger.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd)

    if result.returncode == 0:
        logger.info("Tests completed successfully")
    else:
        logger.error(f"Tests failed with return code: {result.returncode}")

    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Run tests for py-adsb-historical-data-client")
    parser.add_argument("--all", action="store_true", help="Include integration tests")
    parser.add_argument("--coverage", action="store_true", help="Generate coverage report")

    args = parser.parse_args()

    logger.info("Starting test runner")
    if args.all:
        logger.info("Including integration tests")
    if args.coverage:
        logger.info("Generating coverage report")

    return_code = run_tests(include_integration=args.all, coverage=args.coverage)
    sys.exit(return_code)


if __name__ == "__main__":
    main()
