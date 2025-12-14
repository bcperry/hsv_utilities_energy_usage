"""The HSV Utilities Energy integration."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform, UnitOfEnergy
from homeassistant.core import HomeAssistant, ServiceCall

from .const import (
    CONF_ACCOUNT_NUMBER,
    CONF_DATA_PATH,
    CONF_FETCH_DAYS,
    CONF_PASSWORD,
    CONF_SERVICE_LOCATION,
    CONF_UPDATE_INTERVAL,
    CONF_USERNAME,
    CONF_UTILITY_TYPES,
    DEFAULT_FETCH_DAYS,
    DEFAULT_UPDATE_INTERVAL,
    DOMAIN,
)
from .coordinator import EnergyDataCoordinator
from .delta_storage import EnergyDeltaStorage

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR]

SERVICE_IMPORT_STATISTICS = "import_statistics"
ATTR_DAYS = "days"

SERVICE_IMPORT_STATISTICS_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_DAYS, default=30): cv.positive_int,
    }
)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up HSV Utilities Energy from a config entry."""
    _LOGGER.info("Setting up HSV Utilities Energy integration")

    # Get configuration
    username = entry.data.get(CONF_USERNAME)
    password = entry.data.get(CONF_PASSWORD)
    service_location_number = entry.data.get(CONF_SERVICE_LOCATION)
    account_number = entry.data.get(CONF_ACCOUNT_NUMBER)
    data_path = entry.data.get(CONF_DATA_PATH)
    update_interval_seconds = entry.data.get(
        CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL
    )
    fetch_days = entry.data.get(CONF_FETCH_DAYS, DEFAULT_FETCH_DAYS)
    utility_types = entry.data.get(CONF_UTILITY_TYPES, ["ELECTRIC", "GAS"])

    # Create coordinator
    coordinator = EnergyDataCoordinator(
        hass=hass,
        username=username,
        password=password,
        service_location_number=service_location_number,
        account_number=account_number,
        data_path=data_path,
        update_interval=timedelta(seconds=update_interval_seconds),
        fetch_days=fetch_days,
        utility_types=utility_types,
    )

    # Fetch initial data
    await coordinator.async_config_entry_first_refresh()

    # Store coordinator in hass.data
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = coordinator

    # Register service to import historical statistics
    async def async_import_statistics(call: ServiceCall) -> None:
        """Import historical data from Delta Lake into HA statistics."""
        days = call.data.get(ATTR_DAYS, 30)
        await _async_import_statistics(hass, coordinator, entry, days)

    if not hass.services.has_service(DOMAIN, SERVICE_IMPORT_STATISTICS):
        hass.services.async_register(
            DOMAIN,
            SERVICE_IMPORT_STATISTICS,
            async_import_statistics,
            schema=SERVICE_IMPORT_STATISTICS_SCHEMA,
        )

    # Forward setup to sensor platform
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Auto-import historical statistics on first setup
    # This runs after first data fetch so Delta Lake has data
    hass.async_create_background_task(
        _async_import_statistics(hass, coordinator, entry, 30),
        "hsv_utilities_energy_import_statistics",
    )

    return True


async def _async_import_statistics(
    hass: HomeAssistant,
    coordinator: EnergyDataCoordinator,
    entry: ConfigEntry,
    days: int,
) -> None:
    """Import historical statistics from Delta Lake storage."""
    _LOGGER.info("Importing %d days of historical statistics", days)

    storage = EnergyDeltaStorage(coordinator.data_path)

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    for utility_type in coordinator.utility_types:
        # Import usage statistics
        await _import_utility_statistics(
            hass=hass,
            storage=storage,
            entry=entry,
            utility_type=utility_type,
            data_type="USAGE",
            start_date=start_date,
            end_date=end_date,
        )

        # Import cost statistics
        await _import_utility_statistics(
            hass=hass,
            storage=storage,
            entry=entry,
            utility_type=utility_type,
            data_type="COST",
            start_date=start_date,
            end_date=end_date,
        )

    _LOGGER.info("Historical statistics import complete")


async def _import_utility_statistics(
    hass: HomeAssistant,
    storage: EnergyDeltaStorage,
    entry: ConfigEntry,
    utility_type: str,
    data_type: str,
    start_date,
    end_date,
) -> None:
    """Import statistics for a single utility type and data type."""

    def _read_data():
        return storage.read_usage_data(
            utility_type=utility_type,
            data_type=data_type,
            start_date=str(start_date),
            end_date=str(end_date),
        )

    # Read data in executor
    df = await hass.async_add_executor_job(_read_data)

    if df.empty:
        _LOGGER.debug("No %s %s data to import", utility_type, data_type)
        return

    # Create statistic ID (external statistics use domain:id format)
    stat_suffix = "usage" if data_type == "USAGE" else "cost"
    statistic_id = f"{DOMAIN}:{utility_type.lower()}_{stat_suffix}"

    # Determine unit
    if data_type == "USAGE":
        if utility_type == "ELECTRIC":
            unit = UnitOfEnergy.KILO_WATT_HOUR
        else:
            # Gas/Water - use the unit from data
            unit = df["unit_of_measure"].iloc[0] if not df.empty else "CCF"
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

    # Group by hour and create statistics
    # Sort by datetime
    df = df.sort_values("datetime_utc")

    # Create hourly statistics
    statistics: list[StatisticData] = []
    cumulative_sum = 0.0

    # Group by hour for hourly stats
    df["hour_start"] = df["datetime_utc"].dt.floor("h")
    hourly = df.groupby("hour_start")["usage_value"].sum()

    for hour_start, value in hourly.items():
        cumulative_sum += value
        statistics.append(
            StatisticData(
                start=hour_start.to_pydatetime(),
                sum=cumulative_sum,
                state=value,
            )
        )

    if statistics:
        _LOGGER.info(
            "Importing %d hourly %s %s statistics",
            len(statistics),
            utility_type,
            data_type,
        )
        async_add_external_statistics(hass, metadata, statistics)
    else:
        _LOGGER.debug("No statistics to import for %s %s", utility_type, data_type)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info("Unloading HSV Utilities Energy integration")

    # Unload platforms
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    # Remove coordinator from hass.data
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok
