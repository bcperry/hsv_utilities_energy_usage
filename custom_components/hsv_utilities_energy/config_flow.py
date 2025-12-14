"""Config flow for HSV Utilities Energy integration."""

from __future__ import annotations

import logging
from typing import Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResult

from .const import (
    CONF_ACCOUNT_NUMBER,
    CONF_DATA_PATH,
    CONF_FETCH_DAYS,
    CONF_PASSWORD,
    CONF_SERVICE_LOCATION,
    CONF_UPDATE_INTERVAL,
    CONF_USERNAME,
    CONF_UTILITY_TYPES,
    DEFAULT_DATA_PATH,
    DEFAULT_FETCH_DAYS,
    DEFAULT_UPDATE_INTERVAL,
    DEFAULT_UTILITY_TYPES,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)


async def validate_credentials(
    hass: HomeAssistant,
    username: str,
    password: str,
    service_location: str,
    account_number: str,
) -> dict[str, str]:
    """Validate the credentials by attempting to authenticate.

    Returns dict with 'title' on success, raises ValueError on failure.
    """
    from .api_client import UtilityAPIClient

    try:
        async with UtilityAPIClient(username, password) as client:
            if not await client.authenticate():
                raise ValueError(
                    "Authentication failed. Please check your credentials."
                )

        return {"title": f"HSV Utilities ({service_location})"}

    except Exception as err:
        _LOGGER.exception("Unexpected error validating credentials")
        raise ValueError(f"Error validating credentials: {err}") from err


class HSVUtilitiesEnergyConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for HSV Utilities Energy."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                info = await validate_credentials(
                    self.hass,
                    user_input[CONF_USERNAME],
                    user_input[CONF_PASSWORD],
                    user_input[CONF_SERVICE_LOCATION],
                    user_input[CONF_ACCOUNT_NUMBER],
                )
            except ValueError as err:
                _LOGGER.warning("Validation error: %s", err)
                errors["base"] = "invalid_auth"
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected exception during validation")
                errors["base"] = "unknown"
            else:
                # Create the entry
                return self.async_create_entry(
                    title=info["title"],
                    data=user_input,
                )

        # Show form
        data_schema = vol.Schema(
            {
                vol.Required(
                    CONF_USERNAME,
                    default=user_input.get(CONF_USERNAME) if user_input else "",
                ): cv.string,
                vol.Required(
                    CONF_PASSWORD,
                    default=user_input.get(CONF_PASSWORD) if user_input else "",
                ): cv.string,
                vol.Required(
                    CONF_SERVICE_LOCATION,
                    default=user_input.get(CONF_SERVICE_LOCATION) if user_input else "",
                ): cv.string,
                vol.Required(
                    CONF_ACCOUNT_NUMBER,
                    default=user_input.get(CONF_ACCOUNT_NUMBER) if user_input else "",
                ): cv.string,
                vol.Optional(
                    CONF_DATA_PATH,
                    default=user_input.get(CONF_DATA_PATH, DEFAULT_DATA_PATH)
                    if user_input
                    else DEFAULT_DATA_PATH,
                ): cv.string,
                vol.Optional(
                    CONF_UPDATE_INTERVAL,
                    default=user_input.get(
                        CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL
                    )
                    if user_input
                    else DEFAULT_UPDATE_INTERVAL,
                ): vol.All(vol.Coerce(int), vol.Range(min=300, max=86400)),
                vol.Optional(
                    CONF_FETCH_DAYS,
                    default=user_input.get(CONF_FETCH_DAYS, DEFAULT_FETCH_DAYS)
                    if user_input
                    else DEFAULT_FETCH_DAYS,
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=30)),
                vol.Optional(
                    CONF_UTILITY_TYPES,
                    default=user_input.get(CONF_UTILITY_TYPES, DEFAULT_UTILITY_TYPES)
                    if user_input
                    else DEFAULT_UTILITY_TYPES,
                ): cv.multi_select(
                    {"ELECTRIC": "Electric", "GAS": "Gas", "WATER": "Water"}
                ),
            }
        )

        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> HSVUtilitiesEnergyOptionsFlow:
        """Get the options flow for this handler."""
        return HSVUtilitiesEnergyOptionsFlow(config_entry)


class HSVUtilitiesEnergyOptionsFlow(config_entries.OptionsFlowWithConfigEntry):
    """Handle options flow for HSV Utilities Energy."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage the options."""
        if user_input is not None:
            # Update config entry with new data
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                data={**self.config_entry.data, **user_input},
            )
            return self.async_create_entry(title="", data={})

        # Get current values
        current_interval = self.config_entry.data.get(
            CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL
        )
        current_fetch_days = self.config_entry.data.get(
            CONF_FETCH_DAYS, DEFAULT_FETCH_DAYS
        )
        current_utilities = self.config_entry.data.get(
            CONF_UTILITY_TYPES, DEFAULT_UTILITY_TYPES
        )

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_UPDATE_INTERVAL,
                        default=current_interval,
                    ): vol.All(vol.Coerce(int), vol.Range(min=300, max=86400)),
                    vol.Optional(
                        CONF_FETCH_DAYS,
                        default=current_fetch_days,
                    ): vol.All(vol.Coerce(int), vol.Range(min=1, max=30)),
                    vol.Optional(
                        CONF_UTILITY_TYPES,
                        default=current_utilities,
                    ): cv.multi_select(
                        {"ELECTRIC": "Electric", "GAS": "Gas", "WATER": "Water"}
                    ),
                }
            ),
        )
