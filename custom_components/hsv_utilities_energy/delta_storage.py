"""Storage module for energy usage data using Home Assistant's recorder.

This module provides a lightweight in-memory cache for recent data.
Historical data is stored directly in Home Assistant's statistics system.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

# HSV Utilities API returns timestamps in Central Time (America/Chicago)
# but encoded as if they were UTC. We need to interpret them correctly.
HSV_TIMEZONE = ZoneInfo("America/Chicago")


class EnergyDataCache:
    """In-memory cache for recent energy usage data.

    This replaces the Delta Lake storage with a simple in-memory structure.
    Historical data is stored directly in HA's statistics system.
    """

    def __init__(self, base_path: str = "./energy_data") -> None:
        """Initialize the data cache.

        Args:
            base_path: Ignored, kept for API compatibility with old code.
        """
        # Structure: {utility_type: {data_type: {meter_number: [records]}}}
        self._data: dict[str, dict[str, dict[str, list[dict]]]] = {}
        # Track last fetch timestamps per utility/meter
        self._last_fetch: dict[str, datetime] = {}

    def save_usage_data(
        self,
        data: list[dict],
        meter_number: str,
        service_location_number: str,
        account_number: str,
        utility_type: str,
        unit_of_measure: str,
        time_frame: str = "HOURLY",
        data_type: str = "USAGE",
    ) -> int:
        """
        Save utility usage data to in-memory cache.

        Args:
            data: List of data points with 'x' (timestamp ms) and 'y' (usage value)
            meter_number: Meter identifier
            service_location_number: Service location identifier
            account_number: Account number
            utility_type: Type of utility (ELECTRIC, GAS, WATER)
            unit_of_measure: Unit of measurement (KWH, CCF, GAL, etc.)
            time_frame: Time frame of the data (HOURLY, DAILY, etc.)
            data_type: Type of data (USAGE or COST)

        Returns:
            Number of records saved
        """
        if not data:
            return 0

        # Ensure nested structure exists
        if utility_type not in self._data:
            self._data[utility_type] = {}
        if data_type not in self._data[utility_type]:
            self._data[utility_type][data_type] = {}
        if meter_number not in self._data[utility_type][data_type]:
            self._data[utility_type][data_type][meter_number] = []

        # Convert API data to record format
        records = []
        for point in data:
            timestamp_ms = point["x"]
            usage_value = point["y"]

            # The API returns timestamps as local Central Time, but encoded as if UTC.
            # We interpret the timestamp as UTC first, then treat that wall-clock time
            # as Central Time and convert to actual UTC.
            naive_dt = datetime.utcfromtimestamp(timestamp_ms / 1000)
            # Treat this naive datetime as Central Time
            local_dt = naive_dt.replace(tzinfo=HSV_TIMEZONE)
            # Convert to proper UTC
            dt = local_dt.astimezone(timezone.utc)

            records.append(
                {
                    "timestamp_ms": timestamp_ms,
                    "datetime_utc": dt,
                    "date": dt.date(),
                    "hour": dt.hour if time_frame == "HOURLY" else None,
                    "usage_value": usage_value,
                    "unit_of_measure": unit_of_measure,
                    "utility_type": utility_type,
                    "data_type": data_type,
                    "meter_number": meter_number,
                    "service_location_number": service_location_number,
                    "account_number": account_number,
                    "time_frame": time_frame,
                }
            )

        # Get existing records
        existing = self._data[utility_type][data_type][meter_number]

        # Create a set of existing timestamps for deduplication
        existing_timestamps = {r["timestamp_ms"] for r in existing}

        # Add only new records
        new_records = [
            r for r in records if r["timestamp_ms"] not in existing_timestamps
        ]
        existing.extend(new_records)

        # Sort by timestamp
        existing.sort(key=lambda x: x["timestamp_ms"])

        # Keep only last 7 days of data in memory to prevent unbounded growth
        seven_days_ago_ms = int(
            (datetime.now(tz=timezone.utc).timestamp() - 7 * 24 * 3600) * 1000
        )
        self._data[utility_type][data_type][meter_number] = [
            r for r in existing if r["timestamp_ms"] >= seven_days_ago_ms
        ]

        # Update last fetch timestamp
        self._last_fetch[f"{utility_type}_{data_type}"] = datetime.now(tz=timezone.utc)

        return len(new_records)

    def read_usage_data(
        self,
        utility_type: str | None = None,
        data_type: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        meter_number: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Read usage data from cache.

        Args:
            utility_type: Filter by utility type (ELECTRIC, GAS, WATER)
            data_type: Filter by data type (USAGE or COST)
            start_date: Start date filter (ISO format)
            end_date: End date filter (ISO format)
            meter_number: Filter by meter number

        Returns:
            List of usage records matching filters
        """
        results = []

        # Determine which utility types to query
        utility_types = [utility_type] if utility_type else list(self._data.keys())

        for ut in utility_types:
            if ut not in self._data:
                continue

            # Determine which data types to query
            data_types = [data_type] if data_type else list(self._data[ut].keys())

            for dt in data_types:
                if dt not in self._data[ut]:
                    continue

                # Determine which meters to query
                meters = (
                    [meter_number] if meter_number else list(self._data[ut][dt].keys())
                )

                for meter in meters:
                    if meter not in self._data[ut][dt]:
                        continue

                    records = self._data[ut][dt][meter]

                    # Apply date filters
                    for record in records:
                        record_date = record["date"]

                        if start_date:
                            start_d = datetime.fromisoformat(start_date).date()
                            if record_date < start_d:
                                continue

                        if end_date:
                            end_d = datetime.fromisoformat(end_date).date()
                            if record_date > end_d:
                                continue

                        results.append(record)

        # Sort by datetime
        results.sort(key=lambda x: x["timestamp_ms"])
        return results

    def get_aggregated_data(
        self,
        utility_type: str,
        data_type: str,
    ) -> dict[str, Any]:
        """
        Get aggregated usage data for a utility type.

        Returns dict with today, yesterday, last_24h totals and metadata.
        """
        now = datetime.now(tz=timezone.utc)
        today = now.date()
        yesterday = today - timedelta(days=1)

        records = self.read_usage_data(utility_type=utility_type, data_type=data_type)

        if not records:
            return {
                "last_24h": 0.0,
                "today": 0.0,
                "yesterday": 0.0,
                "unit": "KWH" if utility_type == "ELECTRIC" else "CCF",
                "last_update": None,
                "data_lag_hours": None,
            }

        # Calculate totals
        today_total = sum(r["usage_value"] for r in records if r["date"] == today)
        yesterday_total = sum(
            r["usage_value"] for r in records if r["date"] == yesterday
        )

        # Get most recent record
        latest = records[-1] if records else None
        latest_dt = latest["datetime_utc"] if latest else None

        # Calculate last 24 hours from most recent data point
        last_24h_total = 0.0
        if latest_dt:
            cutoff = latest_dt - timedelta(hours=24)
            last_24h_total = sum(
                r["usage_value"] for r in records if r["datetime_utc"] > cutoff
            )

        # Calculate data lag
        data_lag_hours = None
        if latest_dt:
            data_lag_hours = round((now - latest_dt).total_seconds() / 3600, 1)

        return {
            "last_24h": round(last_24h_total, 2),
            "today": round(today_total, 2),
            "yesterday": round(yesterday_total, 2),
            "unit": records[0]["unit_of_measure"] if records else "UNKNOWN",
            "last_update": latest_dt.isoformat() if latest_dt else None,
            "data_lag_hours": data_lag_hours,
        }

    def get_hourly_data_for_statistics(
        self,
        utility_type: str,
        data_type: str,
    ) -> list[dict[str, Any]]:
        """
        Get hourly aggregated data suitable for HA statistics import.

        Returns list of dicts with 'hour_start' (datetime) and 'value' (float).
        """
        import logging
        _LOGGER = logging.getLogger(__name__)
        
        records = self.read_usage_data(utility_type=utility_type, data_type=data_type)

        if not records:
            _LOGGER.info("No records in cache for %s %s", utility_type, data_type)
            return []

        # Log the time range of data in cache
        first_record = records[0]
        last_record = records[-1]
        _LOGGER.info(
            "Cache has %d %s %s records from %s to %s",
            len(records),
            utility_type,
            data_type,
            first_record["datetime_utc"],
            last_record["datetime_utc"],
        )

        # Group by hour
        # HA expects timezone-aware UTC datetimes for external statistics
        hourly: dict[datetime, float] = {}
        for record in records:
            dt = record["datetime_utc"]
            # Keep timezone-aware UTC datetime
            hour_start = dt.replace(minute=0, second=0, microsecond=0)
            if hour_start not in hourly:
                hourly[hour_start] = 0.0
            hourly[hour_start] += record["usage_value"]

        # Convert to list sorted by time
        result = [
            {"hour_start": hour, "value": value}
            for hour, value in sorted(hourly.items())
        ]
        
        if result:
            _LOGGER.info(
                "Hourly aggregation: %d hours from %s to %s",
                len(result),
                result[0]["hour_start"],
                result[-1]["hour_start"],
            )

        return result


# Backwards compatibility alias
EnergyDeltaStorage = EnergyDataCache
