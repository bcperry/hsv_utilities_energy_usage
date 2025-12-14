# Home Assistant Integration for HSV Utilities Energy (Unofficial)

[![GitHub Release][releases-shield]][releases]
[![GitHub Activity][commits-shield]][commits]

An unofficial HSV Utilities integration for Home Assistant, installed through [HACS](https://hacs.xyz/docs/setup/download).

This integration retrieves electricity and gas usage data from HSV Utility SmartHub and stores it in a Delta Lake data lake for historical analysis. It creates sensors in Home Assistant for real-time monitoring of your energy consumption and costs.

## Requirements

To use this integration, you'll need the following information:

- HSV Utilities SmartHub Username
- HSV Utilities SmartHub Password
- Service Location Number
- Account Number

## Disclaimer

This [Home Assistant](https://www.home-assistant.io/) integration is not affiliated, associated, nor sponsored by Huntsville Utilities or any related entities.

Any use of this integration is at the sole discretion and risk of the user integrating it into their Home Assistant installation. The user takes full responsibility for protecting their local Home Assistant installation and credentials.

This integration is provided as-is for personal energy monitoring and analysis purposes.

## Installation

1. Use [HACS](https://hacs.xyz/docs/setup/download). In HACS, go to `HACS > Integrations > 3 dots > Custom repositories` and add this GitHub repo `https://github.com/bcperry/hsv_utilities_energy_usage`. Set the category to "Integration". Now skip to step 7.
2. If not using HACS, open the directory for your Home Assistant configuration (where you find `configuration.yaml`).
3. If you do not have a `custom_components` directory there, create it.
4. In the `custom_components` directory, create a new folder called `hsv_utilities_energy`.
5. Download _all_ the files from the `custom_components/hsv_utilities_energy/` directory in this repository.
6. Place the files you downloaded in the new directory you created.
7. Restart Home Assistant.
8. [![Add Integration][add-integration-badge]][add-integration] or in the Home Assistant UI go to "Configuration" -> "Integrations", click "+" and search for "HSV Utilities Energy".

## Configuration

The configuration flow will guide you through setup:

1. **Username and Password (required)**
   - Enter your HSV Utilities SmartHub login credentials.
2. **Service Location Number (required)**
   - Your service location identifier from your utility account.
3. **Account Number (required)**
   - Your utility account number.
4. **Data Path (optional)**
   - Where to store the Delta Lake data (default: `/config/energy_data`).
5. **Update Interval (optional)**
   - How often to fetch new data in seconds (default: 900 = 15 minutes).
6. **Fetch Days (optional)**
   - Number of days of historical data to fetch (default: 30).
7. **Utility Types (optional)**
   - Select which utilities to monitor: ELECTRIC, GAS (default: both).

## Available Sensors

For each configured utility type (ELECTRIC, GAS), the integration creates two sensors:

| Sensor         | Description                              | Unit | State Class        |
| -------------- | ---------------------------------------- | ---- | ------------------ |
| Electric Usage | Last 24 hours of electricity consumption | kWh  | `total_increasing` |
| Electric Cost  | Last 24 hours of electricity cost        | USD  | `total`            |
| Gas Usage      | Last 24 hours of gas consumption         | CCF  | `total_increasing` |
| Gas Cost       | Last 24 hours of gas cost                | USD  | `total`            |

### Sensor Attributes

Each sensor includes additional attributes:

| Attribute        | Description                                       |
| ---------------- | ------------------------------------------------- |
| `today`          | Consumption/cost for today (calendar day)         |
| `yesterday`      | Consumption/cost for yesterday                    |
| `utility_type`   | ELECTRIC or GAS                                   |
| `last_update`    | Timestamp of most recent data from utility        |
| `data_lag_hours` | Hours since last data update (usage sensors only) |

> **Note:** The utility data source has approximately a 2-hour lag. The main sensor state shows the last 24 hours of _available_ data to provide consistent readings despite this lag.

## Services

### `hsv_utilities_energy.import_statistics`

Import historical data from the Delta Lake into Home Assistant's long-term statistics for use in the Energy Dashboard.

| Parameter | Description              | Default |
| --------- | ------------------------ | ------- |
| `days`    | Number of days to import | 30      |

## Data Storage

This integration stores usage data in Delta Lake format, which provides:

- **Efficient storage** with Parquet columnar format
- **Deduplication** via merge/upsert operations
- **Partitioning** by date, utility type, and data type
- **Historical queries** for analysis and reporting

Data is stored at the configured path (default: `/config/energy_data/usage/`).

---

# Energy Usage Data Retrieval & Analysis (Standalone)

In addition to the Home Assistant integration, this repository includes a standalone Python application to retrieve and analyze energy usage data from HSV Utility SmartHub. Features OAuth2 authentication, Delta Lake storage with partitioning, and comprehensive Jupyter notebook visualizations.

## Setup

1. **Install dependencies** (using uv):

   ```bash
   uv sync
   ```

2. **Configure credentials**:

   ```bash
   cp .env.example .env
   # Edit .env and add your credentials:
   # UTILITY_USERNAME=your_email@gmail.com
   # UTILITY_PASSWORD=your_password
   # SERVICE_LOCATION_NUMBER=your_service_location
   # ACCOUNT_NUMBER=your_account_number
   ```

   > **Note:** Command line arguments can override .env values for username and password.

## Usage

### Basic Data Retrieval

```bash
# Fetch last 30 days of data for all utilities (default)
uv run main.py -d 30

# Fetch only electricity data
uv run main.py -i ELECTRIC -d 7

# Fetch multiple utilities with custom time frame
uv run main.py -i ELECTRIC GAS -t DAILY -d 30

# Save to JSON without storing in Delta Lake
uv run main.py -d 7 -o output.json --no-save
```

### Command Line Options

```
-u, --username              Utility account username (overrides .env)
-p, --password              Utility account password (overrides .env)
-s, --service-location      Service location number (overrides .env)
-a, --account-number        Account number (overrides .env)
-d, --days                  Number of days to retrieve (default: 1)
-t, --time-frame            HOURLY, DAILY, or MONTHLY (default: HOURLY)
-i, --industries            WATER, GAS, ELECTRIC (default: all)
-o, --output                Output JSON file path
--no-save                   Don't save to Delta Lake
--delta-path                Delta Lake directory (default: ./energy_data)
--max-retries               Polling retries for async API (default: 10)
--retry-delay               Seconds between retries (default: 2)
```

### Data Analysis

Open the Jupyter notebook for interactive analysis:

```bash
jupyter notebook energy_analysis.ipynb
```

The notebook includes:

- Usage and cost time series visualizations
- Daily/hourly pattern analysis
- Heatmaps and peak usage identification
- Cost vs usage correlation analysis
- Custom date range filtering
- Actual cost data from API (not estimates)

## Features

- ✅ **OAuth2 Authentication** - Secure authentication with HSV Utility SmartHub
- ✅ **Multi-Utility Support** - Electricity, gas, and water usage data
- ✅ **Delta Lake Storage** - Time-series data with efficient partitioning by date, utility type, and data type
- ✅ **Usage & Cost Data** - Retrieves both actual usage (KWH, FT3, GAL) and actual cost ($) from API
- ✅ **Deduplication** - Merge/upsert logic prevents duplicate records on re-runs
- ✅ **Async Polling** - Handles API's asynchronous response mechanism with configurable retries
- ✅ **Flexible Querying** - Filter by utility type, date range, and data type (USAGE or COST)
- ✅ **Jupyter Analysis** - Interactive notebook with plotly visualizations, heatmaps, and cost analysis
- ✅ **Command Line Interface** - Full argument parsing with .env file fallback

## Project Structure

```
energy_usage/
├── main.py                  # CLI application and UtilityAPIClient
├── delta_storage.py         # Delta Lake storage handler with merge/upsert
├── energy_analysis.ipynb    # Jupyter notebook for data visualization
├── pyproject.toml           # Project dependencies and uv configuration
├── .env.example             # Template for environment variables
├── .env                     # Your credentials (not committed)
├── energy_data/             # Delta Lake tables (created on first run)
│   ├── usage/               # Usage and cost data (partitioned)
│   └── fetch_metadata/      # Fetch history metadata
└── README.md                # This file
```

## Delta Lake Schema

### Usage Table (`energy_data/usage`)

Partitioned by: `date`, `utility_type`, `data_type`

Columns:

- `timestamp_ms` - Unix timestamp in milliseconds
- `datetime_utc` - Timestamp as datetime (UTC)
- `date` - Date for partitioning
- `year`, `month`, `day`, `hour` - Time components
- `usage_value` - Usage amount (KWH, FT3, GAL) or cost ($)
- `unit_of_measure` - KWH, FT3, GAL, etc.
- `utility_type` - ELECTRIC, GAS, WATER
- `data_type` - USAGE or COST
- `meter_number` - Meter identifier
- `service_location_number` - Service location
- `account_number` - Account number
- `time_frame` - HOURLY, DAILY, MONTHLY
- `ingested_at` - When the record was saved

Unique key for merge: `(timestamp_ms, meter_number, utility_type, data_type)`

## Data Type: USAGE vs COST

The API returns two datasets for each utility:

1. **USAGE** - Actual consumption values

   - Electric: KWH (kilowatt-hours)
   - Gas: FT3 (cubic feet)
   - Water: GAL (gallons)

2. **COST** - Actual cost in dollars ($)
   - Reflects the utility's billing rates
   - May vary by time of day (time-of-use rates)

Both datasets are stored separately in Delta Lake for flexible analysis.

Example query:

```python
from delta_storage import EnergyDeltaStorage

storage = EnergyDeltaStorage()

# Get usage data
usage_df = storage.read_usage_data(
    utility_type='ELECTRIC',
    data_type='USAGE',
    start_date='2025-11-01',
    end_date='2025-11-30'
)

# Get cost data
cost_df = storage.read_usage_data(
    utility_type='ELECTRIC',
    data_type='COST',
    start_date='2025-11-01',
    end_date='2025-11-30'
)
```

## Examples

### Check Stats

```bash
uv run python -c "from delta_storage import EnergyDeltaStorage; print(EnergyDeltaStorage().get_stats())"
```

### Query Specific Date

```python
from delta_storage import EnergyDeltaStorage
from datetime import date

storage = EnergyDeltaStorage()
df = storage.read_usage_data(utility_type='ELECTRIC', data_type='USAGE')
nov28 = df[df['date'] == date(2025, 11, 28)]
print(f"Nov 28 usage: {nov28['usage_value'].sum():.2f} KWH")
```

## Security Notes

⚠️ **Never commit your `.env` file to version control**

- The `.env` file is included in `.gitignore`
- Contains sensitive credentials and account numbers
- Use `.env.example` as a template

## Dependencies

Core libraries:

- `requests` - HTTP client for API calls
- `python-dotenv` - Environment variable management
- `deltalake` - Delta Lake storage (includes pandas, pyarrow)
- `plotly`, `matplotlib`, `seaborn` - Visualization
- `scikit-learn` - Data normalization
- `jupyter`, `ipykernel` - Notebook support

Development:

- `uv` - Package manager
- `pre-commit` - Git hooks for code quality

## License

## MIT License - see [LICENSE](LICENSE) file for details.

[commits-shield]: https://img.shields.io/github/commit-activity/w/bcperry/hsv_utilities_energy_usage?style=flat-square
[commits]: https://github.com/bcperry/hsv_utilities_energy_usage/commits/master
[releases-shield]: https://img.shields.io/github/release/bcperry/hsv_utilities_energy_usage.svg?style=flat-square
[releases]: https://github.com/bcperry/hsv_utilities_energy_usage/releases
[add-integration]: https://my.home-assistant.io/redirect/config_flow_start?domain=hsv_utilities_energy
[add-integration-badge]: https://my.home-assistant.io/badges/config_flow_start.svg
