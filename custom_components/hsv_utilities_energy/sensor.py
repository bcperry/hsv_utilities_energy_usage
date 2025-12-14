"""Sensor platform for HSV Utilities Energy integration."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import EnergyDataCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up HSV Utilities Energy sensors from a config entry."""
    coordinator: EnergyDataCoordinator = hass.data[DOMAIN][entry.entry_id]

    # Create sensors for each utility type
    entities: list[SensorEntity] = []

    for utility_type in coordinator.utility_types:
        # Usage sensor
        entities.append(
            EnergyUsageSensor(
                coordinator=coordinator,
                utility_type=utility_type,
                entry=entry,
            )
        )
        # Cost sensor
        entities.append(
            EnergyCostSensor(
                coordinator=coordinator,
                utility_type=utility_type,
                entry=entry,
            )
        )

    _LOGGER.info("Adding %d entities for HSV Utilities Energy", len(entities))
    async_add_entities(entities)


class EnergyUsageSensor(CoordinatorEntity[EnergyDataCoordinator], SensorEntity):
    """Sensor for energy usage."""

    def __init__(
        self,
        coordinator: EnergyDataCoordinator,
        utility_type: str,
        entry: ConfigEntry,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self.utility_type = utility_type
        self._entry = entry
        self._attr_has_entity_name = True

        # Create unique ID
        self._attr_unique_id = f"{entry.entry_id}_{utility_type.lower()}_usage"

        # Set name
        utility_name = utility_type.capitalize()
        self._attr_name = f"{utility_name} Usage"

        # Set icon based on utility type
        if utility_type == "ELECTRIC":
            self._attr_icon = "mdi:flash"
            self._attr_device_class = SensorDeviceClass.ENERGY
        elif utility_type == "GAS":
            self._attr_icon = "mdi:fire"
            # Home Assistant does not accept 'CCF' for SensorDeviceClass.GAS.
            # Avoid setting a device class to prevent unit validation errors.
            self._attr_device_class = None
        elif utility_type == "WATER":
            self._attr_icon = "mdi:water"
            self._attr_device_class = SensorDeviceClass.WATER
        else:
            self._attr_icon = "mdi:gauge"

        # State class for energy dashboard
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information for this entity."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry.entry_id)},
            name="HSV Utilities Energy",
            configuration_url="https://hsvutil.smarthub.coop",
        )

    @property
    def native_value(self) -> float | None:
        """Return the state of the sensor (last 24h of available data)."""
        if not self.coordinator.data:
            return None

        utility_data = self.coordinator.data.get(self.utility_type, {})
        usage_data = utility_data.get("usage", {})
        # Show last 24h of available data (accounts for ~2hr data lag)
        return usage_data.get("last_24h", 0.0)

    @property
    def native_unit_of_measurement(self) -> str | None:
        """Return the unit of measurement."""
        if not self.coordinator.data:
            return None

        utility_data = self.coordinator.data.get(self.utility_type, {})
        usage_data = utility_data.get("usage", {})
        unit = usage_data.get("unit")
        # Normalize units to HA expected casing
        if unit == "KWH":
            return "kWh"
        if unit == "WH":
            return "Wh"
        return unit

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return additional state attributes."""
        if not self.coordinator.data:
            return {}

        utility_data = self.coordinator.data.get(self.utility_type, {})
        usage_data = utility_data.get("usage", {})

        attrs = {
            "today": usage_data.get("today", 0.0),
            "yesterday": usage_data.get("yesterday", 0.0),
            "utility_type": self.utility_type,
        }

        last_update = usage_data.get("last_update")
        if last_update:
            attrs["last_update"] = last_update

        data_lag = usage_data.get("data_lag_hours")
        if data_lag is not None:
            attrs["data_lag_hours"] = data_lag

        return attrs


class EnergyCostSensor(CoordinatorEntity[EnergyDataCoordinator], SensorEntity):
    """Sensor for energy cost."""

    def __init__(
        self,
        coordinator: EnergyDataCoordinator,
        utility_type: str,
        entry: ConfigEntry,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self.utility_type = utility_type
        self._entry = entry
        self._attr_has_entity_name = True

        # Create unique ID
        self._attr_unique_id = f"{entry.entry_id}_{utility_type.lower()}_cost"

        # Set name
        utility_name = utility_type.capitalize()
        self._attr_name = f"{utility_name} Cost"

        # Set attributes
        self._attr_icon = "mdi:currency-usd"
        self._attr_device_class = SensorDeviceClass.MONETARY
        # Monetary sensors should use 'total' not 'total_increasing'
        self._attr_state_class = SensorStateClass.TOTAL
        self._attr_native_unit_of_measurement = "USD"

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information for this entity."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry.entry_id)},
            name="HSV Utilities Energy",
            configuration_url="https://hsvutil.smarthub.coop",
        )

    @property
    def native_value(self) -> float | None:
        """Return the state of the sensor (last 24h of available data)."""
        if not self.coordinator.data:
            return None

        utility_data = self.coordinator.data.get(self.utility_type, {})
        cost_data = utility_data.get("cost", {})
        # Show last 24h of available data (accounts for ~2hr data lag)
        return cost_data.get("last_24h", 0.0)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return additional state attributes."""
        if not self.coordinator.data:
            return {}

        utility_data = self.coordinator.data.get(self.utility_type, {})
        cost_data = utility_data.get("cost", {})

        attrs = {
            "today": cost_data.get("today", 0.0),
            "yesterday": cost_data.get("yesterday", 0.0),
            "utility_type": self.utility_type,
        }

        last_update = cost_data.get("last_update")
        if last_update:
            attrs["last_update"] = last_update

        return attrs
