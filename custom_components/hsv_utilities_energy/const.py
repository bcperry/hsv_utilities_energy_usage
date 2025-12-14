"""Constants for the HSV Utilities Energy integration."""

from datetime import timedelta
from typing import Final

# Integration domain
DOMAIN: Final = "hsv_utilities_energy"

# Configuration keys
CONF_USERNAME: Final = "username"
CONF_PASSWORD: Final = "password"
CONF_SERVICE_LOCATION: Final = "service_location_number"
CONF_ACCOUNT_NUMBER: Final = "account_number"
CONF_DATA_PATH: Final = "data_path"
CONF_UPDATE_INTERVAL: Final = "update_interval"
CONF_FETCH_DAYS: Final = "fetch_days"
CONF_UTILITY_TYPES: Final = "utility_types"

# Default values
DEFAULT_DATA_PATH: Final = "/config/energy_data"
DEFAULT_UPDATE_INTERVAL: Final = (
    900  # 15 minutes in seconds (matches data source interval)
)
DEFAULT_FETCH_DAYS: Final = 30  # Fetch last 30 days to build up history
DEFAULT_UTILITY_TYPES: Final = ["ELECTRIC", "GAS"]

# Update interval
UPDATE_INTERVAL = timedelta(seconds=DEFAULT_UPDATE_INTERVAL)

# Sensor types
SENSOR_TYPES = {
    "electric_usage": {
        "name": "Electric Usage",
        "unit": "kWh",
        "icon": "mdi:flash",
        "device_class": "energy",
        "state_class": "total_increasing",
    },
    "electric_cost": {
        "name": "Electric Cost",
        "unit": "USD",
        "icon": "mdi:currency-usd",
        "device_class": "monetary",
        "state_class": "total_increasing",
    },
    "gas_usage": {
        "name": "Gas Usage",
        "unit": "CCF",
        "icon": "mdi:fire",
        "device_class": "gas",
        "state_class": "total_increasing",
    },
    "gas_cost": {
        "name": "Gas Cost",
        "unit": "USD",
        "icon": "mdi:currency-usd",
        "device_class": "monetary",
        "state_class": "total_increasing",
    },
}

# Utility types
UTILITY_TYPE_ELECTRIC: Final = "ELECTRIC"
UTILITY_TYPE_GAS: Final = "GAS"
UTILITY_TYPE_WATER: Final = "WATER"

# Data types
DATA_TYPE_USAGE: Final = "USAGE"
DATA_TYPE_COST: Final = "COST"
