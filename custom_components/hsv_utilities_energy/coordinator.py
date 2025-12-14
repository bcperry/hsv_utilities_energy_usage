"""Data update coordinator for HSV Utilities Energy."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import async_add_external_statistics
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api_client import UtilityAPIClient
from .const import (
    DATA_TYPE_COST,
    DATA_TYPE_USAGE,
    DOMAIN,
    UTILITY_TYPE_ELECTRIC,
)
from .delta_storage import EnergyDataCache

_LOGGER = logging.getLogger(__name__)


class EnergyDataCoordinator(DataUpdateCoordinator):
    """Class to manage fetching energy data from HSV Utilities API."""

    def __init__(
        self,
        hass: HomeAssistant,
        username: str,
        password: str,
        service_location_number: str,
        account_number: str,
        data_path: str,
        update_interval: timedelta,
        fetch_days: int,
        utility_types: list[str],
        entry_id: str,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=update_interval,
        )
        self.username = username
        self.password = password
        self.service_location_number = service_location_number
        self.account_number = account_number
        self.data_path = data_path  # Kept for compatibility, not used
        self.fetch_days = fetch_days
        self.utility_types = utility_types
        self.entry_id = entry_id
        # In-memory cache for recent data
        self._cache = EnergyDataCache()
        self._api_client = None

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from HSV Utilities API and store in HA statistics.

        Returns a dictionary with aggregated daily usage and cost for each utility type.
        Structure:
        {
            "ELECTRIC": {
                "usage": {"today": float, "yesterday": float, "unit": "KWH"},
                "cost": {"today": float, "yesterday": float, "unit": "USD"}
            },
            "GAS": {...}
        }
        """
        try:
            # Step 1: Fetch data from API and store in cache
            await self._fetch_and_store_data()

            # Step 2: Import data to HA statistics
            await self._import_to_statistics()

            # Step 3: Return aggregated data from cache
            return self._read_aggregated_data()

        except Exception as err:
            _LOGGER.exception("Error fetching energy data: %s", err)
            raise UpdateFailed(f"Error fetching energy data: {err}") from err

    async def _fetch_and_store_data(self) -> None:
        """Fetch data from API and store in Delta Lake."""
        try:
            # Calculate time range (fetch last N days)
            end_time = datetime.now()
            start_time = end_time - timedelta(days=self.fetch_days)

            # Convert to milliseconds since epoch
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)

            _LOGGER.debug(
                "Fetching data from %s to %s (%d days)",
                start_time,
                end_time,
                self.fetch_days,
            )

            # Initialize API client
            if self._api_client is None:
                self._api_client = UtilityAPIClient(self.username, self.password)

            # Authenticate
            if not await self._api_client.authenticate():
                raise UpdateFailed("Authentication failed")

            # Fetch usage data for each utility type
            for utility_type in self.utility_types:
                await self._fetch_utility_data(utility_type, start_ms, end_ms)

        except Exception as err:
            _LOGGER.exception("Error fetching from API: %s", err)
            raise

    async def _fetch_utility_data(
        self, utility_type: str, start_ms: int, end_ms: int
    ) -> None:
        """Fetch and store data for a specific utility type."""
        try:
            # Fetch usage data
            usage_data = await self._api_client.get_usage_data(
                service_location_number=self.service_location_number,
                account_number=self.account_number,
                start_datetime=start_ms,
                end_datetime=end_ms,
                time_frame="HOURLY",
                industries=[utility_type],
                include_demand=False,
            )

            if not usage_data or "data" not in usage_data:
                _LOGGER.warning("No usage data received for %s", utility_type)
                return

            # Store usage data in cache
            self._store_data_sync(utility_type, usage_data)

        except Exception as err:
            _LOGGER.warning("Error fetching %s data: %s", utility_type, err)

    def _store_data_sync(self, utility_type: str, api_data: dict) -> None:
        """Store API data in the in-memory cache."""
        try:
            # Parse API response using SmartHub dataset structure (matches main.py)
            data_section = api_data.get("data", {})
            industry_datasets = data_section.get(utility_type, [])

            if not isinstance(industry_datasets, list):
                _LOGGER.debug("No datasets for %s in API response", utility_type)
                return

            for dataset in industry_datasets:
                data_type = dataset.get("type", DATA_TYPE_USAGE)
                # Use UNKNOWN as default to match main.py behavior
                unit_of_measure = dataset.get("unitOfMeasure", "UNKNOWN")
                series_list = dataset.get("series", [])

                for series in series_list:
                    meter_number = series.get("meterNumber", "unknown")
                    data_points = series.get("data", [])

                    if not data_points:
                        continue

                    # Determine save type (USAGE or COST)
                    save_type = (
                        DATA_TYPE_USAGE
                        if str(data_type).upper() == DATA_TYPE_USAGE
                        else DATA_TYPE_COST
                    )

                    # Store in cache
                    records_written = self._cache.save_usage_data(
                        data=data_points,
                        meter_number=meter_number,
                        service_location_number=self.service_location_number,
                        account_number=self.account_number,
                        utility_type=utility_type,
                        unit_of_measure=unit_of_measure,
                        time_frame="HOURLY",
                        data_type=save_type,
                    )
                    _LOGGER.debug(
                        "Cached %d %s %s records for meter %s",
                        records_written,
                        utility_type,
                        save_type.lower(),
                        meter_number,
                    )

        except Exception as err:
            _LOGGER.exception("Error storing %s data: %s", utility_type, err)

    async def _import_to_statistics(self) -> None:
        """Import cached data to Home Assistant statistics."""
        for utility_type in self.utility_types:
            # Import usage statistics
            await self._import_utility_statistics(
                utility_type=utility_type,
                data_type=DATA_TYPE_USAGE,
            )

            # Import cost statistics
            await self._import_utility_statistics(
                utility_type=utility_type,
                data_type=DATA_TYPE_COST,
            )

    async def _import_utility_statistics(
        self,
        utility_type: str,
        data_type: str,
    ) -> None:
        """Import statistics for a single utility type and data type."""
        hourly_data = self._cache.get_hourly_data_for_statistics(
            utility_type=utility_type,
            data_type=data_type,
        )

        if not hourly_data:
            _LOGGER.debug(
                "No %s %s data to import to statistics", utility_type, data_type
            )
            return

        # Create statistic ID (external statistics use domain:id format)
        stat_suffix = "usage" if data_type == DATA_TYPE_USAGE else "cost"
        statistic_id = f"{DOMAIN}:{utility_type.lower()}_{stat_suffix}"

        # Determine unit
        if data_type == DATA_TYPE_USAGE:
            if utility_type == UTILITY_TYPE_ELECTRIC:
                unit = UnitOfEnergy.KILO_WATT_HOUR
            else:
                # Gas/Water - get unit from cache
                records = self._cache.read_usage_data(
                    utility_type=utility_type, data_type=data_type
                )
                unit = records[0]["unit_of_measure"] if records else "CCF"
        else:
            unit = "USD"

        # Create metadata
        metadata = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name=f"{utility_type.capitalize()} {data_type.capitalize()}",
            source=DOMAIN,
            statistic_id=statistic_id,
            unit_of_measurement=unit,
        )

        # Create hourly statistics with cumulative sum
        statistics: list[StatisticData] = []
        cumulative_sum = 0.0

        for hourly in hourly_data:
            cumulative_sum += hourly["value"]
            statistics.append(
                StatisticData(
                    start=hourly["hour_start"],
                    sum=cumulative_sum,
                    state=hourly["value"],
                )
            )

        if statistics:
            _LOGGER.debug(
                "Importing %d hourly %s %s statistics",
                len(statistics),
                utility_type,
                data_type,
            )
            async_add_external_statistics(self.hass, metadata, statistics)

    def _read_aggregated_data(self) -> dict[str, Any]:
        """Read and aggregate data from cache.

        Note: The data source has a ~2 hour lag but reports at 15-minute intervals.
        We calculate usage based on the most recent available data.
        """
        data = {}

        for utility_type in self.utility_types:
            utility_data = {"usage": {}, "cost": {}}

            # Get usage data from cache
            try:
                usage_agg = self._cache.get_aggregated_data(
                    utility_type=utility_type,
                    data_type=DATA_TYPE_USAGE,
                )
                utility_data["usage"] = usage_agg
            except Exception as err:
                _LOGGER.warning(
                    "Could not fetch usage data for %s: %s", utility_type, err
                )
                unit = "KWH" if utility_type == UTILITY_TYPE_ELECTRIC else "CCF"
                utility_data["usage"] = {
                    "last_24h": 0.0,
                    "today": 0.0,
                    "yesterday": 0.0,
                    "unit": unit,
                    "last_update": None,
                    "data_lag_hours": None,
                }

            # Get cost data from cache
            try:
                cost_agg = self._cache.get_aggregated_data(
                    utility_type=utility_type,
                    data_type=DATA_TYPE_COST,
                )
                # Override unit for cost
                cost_agg["unit"] = "USD"
                utility_data["cost"] = cost_agg
            except Exception as err:
                _LOGGER.warning(
                    "Could not fetch cost data for %s: %s", utility_type, err
                )
                utility_data["cost"] = {
                    "last_24h": 0.0,
                    "today": 0.0,
                    "yesterday": 0.0,
                    "unit": "USD",
                    "last_update": None,
                }

            data[utility_type] = utility_data

        _LOGGER.debug("Aggregated energy data: %s", data)
        return data
