# HSV Utilities Energy Integration

Monitor your Huntsville Utilities energy usage (electric, gas, and water) directly in Home Assistant!

## Features

- **Real-time Monitoring**: Track daily usage and costs for electric and gas utilities
- **Energy Dashboard Integration**: Automatically integrates with Home Assistant's Energy Dashboard
- **Historical Data**: Access yesterday's usage for comparison
- **Delta Lake Storage**: Efficient data storage with automatic deduplication
- **Flexible Configuration**: Choose which utility types to monitor and update intervals

## Sensors

For each enabled utility type, the integration provides:

- **Usage Sensor**: Current daily usage (kWh for electric, CCF for gas)
  - State: Today's total usage
  - Attributes: Yesterday's usage, last update time
  
- **Cost Sensor**: Current daily cost in USD
  - State: Today's total cost
  - Attributes: Yesterday's cost, last update time

## Installation

### Prerequisites

Before installing this integration, you need to set up the HSV Utilities data fetcher:

1. Clone the data fetcher repository
2. Configure your HSV Utilities credentials in `.env`
3. Run the data fetcher to populate your Delta Lake storage:
   ```bash
   python main.py
   ```

### HACS Installation (Recommended)

1. Open HACS in Home Assistant
2. Go to "Integrations"
3. Click the three dots in the top right corner
4. Select "Custom repositories"
5. Add this repository URL: `https://github.com/yourusername/hsv-utilities-energy-integration`
6. Select category: "Integration"
7. Click "Add"
8. Search for "HSV Utilities Energy"
9. Click "Download"
10. Restart Home Assistant

### Manual Installation

1. Copy the `custom_components/hsv_utilities_energy` directory to your Home Assistant's `custom_components` directory
2. Restart Home Assistant

## Configuration

1. Go to **Settings** → **Devices & Services**
2. Click **Add Integration**
3. Search for "HSV Utilities Energy"
4. Enter your configuration:
   - **Data Path**: Path to your Delta Lake data directory (default: `./energy_data`)
   - **Update Interval**: How often to check for new data in seconds (60-3600)
   - **Utility Types**: Select which utilities to monitor (Electric, Gas, Water)

## Energy Dashboard Setup

To add your sensors to the Energy Dashboard:

1. Go to **Settings** → **Dashboards** → **Energy**
2. Click **Add Consumption**
3. Select your electric usage sensor
4. Set the cost entity to your electric cost sensor
5. Repeat for gas if desired

## Troubleshooting

### "Invalid data path" error

Ensure:
- The path points to your Delta Lake directory containing the `usage` folder
- You've run the data fetcher at least once
- The `usage/_delta_log` directory exists

### Sensors show "0" or "unavailable"

- Check that data exists for today in your Delta Lake storage
- Verify the data fetcher is running regularly (via cron or automation)
- Check Home Assistant logs: **Settings** → **System** → **Logs**

### Update interval not working

- Minimum interval is 60 seconds, maximum is 3600 seconds
- Changes to update interval require reloading the integration

## Support

Report issues at: [GitHub Issues](https://github.com/yourusername/hsv-utilities-energy-integration/issues)

## License

MIT License - See LICENSE file for details
