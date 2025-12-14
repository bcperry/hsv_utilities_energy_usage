"""The HSV Utilities Energy integration."""

from __future__ import annotations

import logging
from datetime import timedelta

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
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

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR]

SERVICE_REFRESH_DATA = "refresh_data"
SERVICE_CLEAR_STATISTICS = "clear_statistics"

SERVICE_REFRESH_DATA_SCHEMA = vol.Schema({})
SERVICE_CLEAR_STATISTICS_SCHEMA = vol.Schema({})


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
        entry_id=entry.entry_id,
    )

    # Fetch initial data
    await coordinator.async_config_entry_first_refresh()

    # Store coordinator in hass.data
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = coordinator

    # Register service to manually refresh data
    async def async_refresh_data(call: ServiceCall) -> None:
        """Manually refresh energy data."""
        _LOGGER.info("Manual data refresh requested")
        await coordinator.async_request_refresh()

    # Register service to clear and rebuild statistics
    async def async_clear_statistics(call: ServiceCall) -> None:
        """Clear all statistics and rebuild from scratch."""
        _LOGGER.info("Clear statistics requested")
        await coordinator.async_clear_statistics()

    if not hass.services.has_service(DOMAIN, SERVICE_REFRESH_DATA):
        hass.services.async_register(
            DOMAIN,
            SERVICE_REFRESH_DATA,
            async_refresh_data,
            schema=SERVICE_REFRESH_DATA_SCHEMA,
        )

    if not hass.services.has_service(DOMAIN, SERVICE_CLEAR_STATISTICS):
        hass.services.async_register(
            DOMAIN,
            SERVICE_CLEAR_STATISTICS,
            async_clear_statistics,
            schema=SERVICE_CLEAR_STATISTICS_SCHEMA,
        )

    # Forward setup to sensor platform
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info("Unloading HSV Utilities Energy integration")

    # Unload platforms
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    # Remove coordinator from hass.data
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok
