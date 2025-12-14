"""Data update coordinator for HSV Utilities Energy."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api_client import UtilityAPIClient
from .const import (
    DATA_TYPE_COST,
    DATA_TYPE_USAGE,
    DOMAIN,
    UTILITY_TYPE_ELECTRIC,
)

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
        # Resolve data path relative to Home Assistant config directory
        if not data_path:
            self.data_path = hass.config.path("energy_data")
        else:
            # Normalize common absolute /config paths to HA's config dir
            if data_path.startswith("/config/"):
                rel = data_path[len("/config/") :]
                self.data_path = hass.config.path(rel)
            elif data_path == "/config":
                self.data_path = hass.config.path("")
            else:
                # Treat as relative to HA config directory
                self.data_path = hass.config.path(data_path)
        self.fetch_days = fetch_days
        self.utility_types = utility_types
        self._storage = None
        self._api_client = None

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from HSV Utilities API, store in Delta Lake, then read aggregated data.

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
            # Step 1: Fetch data from API
            await self._fetch_and_store_data()

            # Step 2: Read aggregated data from Delta Lake
            from .delta_storage import EnergyDeltaStorage

            return await self.hass.async_add_executor_job(
                self._read_aggregated_data, EnergyDeltaStorage
            )

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

            # Store usage data in Delta Lake (run in executor)
            await self.hass.async_add_executor_job(
                self._store_data_sync, utility_type, usage_data
            )

        except Exception as err:
            _LOGGER.warning("Error fetching %s data: %s", utility_type, err)

    def _store_data_sync(self, utility_type: str, api_data: dict) -> None:
        """Store API data in Delta Lake (runs in executor)."""
        try:
            from .delta_storage import EnergyDeltaStorage

            if self._storage is None:
                self._storage = EnergyDeltaStorage(self.data_path)

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

                    # Pass raw unit_of_measure from API (matches main.py behavior)
                    # Unit normalization for display happens in sensor.py
                    records_written = self._storage.save_usage_data(
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
                        "Stored %d %s %s records for meter %s",
                        records_written,
                        utility_type,
                        save_type.lower(),
                        meter_number,
                    )

        except Exception as err:
            _LOGGER.exception("Error storing %s data: %s", utility_type, err)

    def _read_aggregated_data(self, storage_class) -> dict[str, Any]:
        """Read and aggregate data from Delta Lake (runs in executor).

        Note: The data source has a ~2 hour lag but reports at 15-minute intervals.
        We calculate usage based on the most recent available data.
        """
        if self._storage is None:
            self._storage = storage_class(self.data_path)

        data = {}
        now = datetime.now()
        today = now.date()
        yesterday = today - timedelta(days=1)
        # Fetch 3 days to ensure we have data despite the 2-hour lag
        three_days_ago = today - timedelta(days=3)

        for utility_type in self.utility_types:
            utility_data = {"usage": {}, "cost": {}}

            # Fetch usage data
            try:
                usage_df = self._storage.read_usage_data(
                    utility_type=utility_type,
                    data_type=DATA_TYPE_USAGE,
                    start_date=str(three_days_ago),
                    end_date=str(today),
                )

                if not usage_df.empty:
                    # Sort by datetime to get most recent
                    usage_df = usage_df.sort_values("datetime_utc")

                    # Get the most recent data timestamp
                    latest_timestamp = usage_df["datetime_utc"].max()

                    # Calculate "last 24 hours" from most recent data point
                    last_24h_start = latest_timestamp - timedelta(hours=24)
                    last_24h_df = usage_df[usage_df["datetime_utc"] > last_24h_start]
                    last_24h_total = last_24h_df["usage_value"].sum()

                    # Group by date for daily totals
                    daily_usage = (
                        usage_df.groupby("date")["usage_value"].sum().to_dict()
                    )

                    utility_data["usage"] = {
                        "last_24h": round(last_24h_total, 2),
                        "today": round(daily_usage.get(today, 0.0), 2),
                        "yesterday": round(daily_usage.get(yesterday, 0.0), 2),
                        "unit": usage_df["unit_of_measure"].iloc[0],
                        "last_update": latest_timestamp.isoformat(),
                        "data_lag_hours": round(
                            (
                                now
                                - latest_timestamp.to_pydatetime().replace(tzinfo=None)
                            ).total_seconds()
                            / 3600,
                            1,
                        ),
                    }
                else:
                    # No data available
                    unit = "kWh" if utility_type == UTILITY_TYPE_ELECTRIC else "CCF"
                    utility_data["usage"] = {
                        "last_24h": 0.0,
                        "today": 0.0,
                        "yesterday": 0.0,
                        "unit": unit,
                        "last_update": None,
                        "data_lag_hours": None,
                    }

            except Exception as err:
                _LOGGER.warning(
                    "Could not fetch usage data for %s: %s", utility_type, err
                )
                utility_data["usage"] = {
                    "last_24h": 0.0,
                    "today": 0.0,
                    "yesterday": 0.0,
                    "unit": "unknown",
                    "last_update": None,
                    "data_lag_hours": None,
                }

            # Fetch cost data
            try:
                cost_df = self._storage.read_usage_data(
                    utility_type=utility_type,
                    data_type=DATA_TYPE_COST,
                    start_date=str(three_days_ago),
                    end_date=str(today),
                )

                if not cost_df.empty:
                    # Sort by datetime to get most recent
                    cost_df = cost_df.sort_values("datetime_utc")

                    # Get the most recent data timestamp
                    latest_timestamp = cost_df["datetime_utc"].max()

                    # Calculate "last 24 hours" from most recent data point
                    last_24h_start = latest_timestamp - timedelta(hours=24)
                    last_24h_df = cost_df[cost_df["datetime_utc"] > last_24h_start]
                    last_24h_total = last_24h_df["usage_value"].sum()

                    daily_cost = cost_df.groupby("date")["usage_value"].sum().to_dict()

                    utility_data["cost"] = {
                        "last_24h": round(last_24h_total, 2),
                        "today": round(daily_cost.get(today, 0.0), 2),
                        "yesterday": round(daily_cost.get(yesterday, 0.0), 2),
                        "unit": "USD",
                        "last_update": latest_timestamp.isoformat(),
                    }
                else:
                    utility_data["cost"] = {
                        "last_24h": 0.0,
                        "today": 0.0,
                        "yesterday": 0.0,
                        "unit": "USD",
                        "last_update": None,
                    }

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
