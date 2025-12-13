import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional
import requests
from dotenv import load_dotenv

from delta_storage import EnergyDeltaStorage


class UtilityAPIClient:
    """Client for interacting with HSV Utility SmartHub API."""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://hsvutil.smarthub.coop"
        self.auth_url = f"{self.base_url}/services/oauth/auth/v2"
        self.session = requests.Session()
        self.access_token: Optional[str] = None
    
    def authenticate(self) -> bool:
        """
        Authenticate with the utility provider's OAuth endpoint.
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        payload = {
            "userId": self.username,
            "password": self.password
        }
        
        try:
            print(f"Authenticating with {self.auth_url}...")
            # Try form-encoded data instead of JSON (415 error suggests JSON not accepted)
            response = self.session.post(
                self.auth_url,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            
            if response.status_code == 200:
                print("✓ Authentication successful!")
                # Store the response data - adjust based on actual API response
                self.auth_response = response.json()
                
                # Extract access token if present in response
                if isinstance(self.auth_response, dict):
                    self.access_token = self.auth_response.get("access_token") or \
                                      self.auth_response.get("accessToken")
                    
                    if self.access_token:
                        print(f"✓ Access token obtained")
                        self.session.headers.update({
                            "Authorization": f"Bearer {self.access_token}"
                        })
                
                return True
            else:
                print(f"✗ Authentication failed with status code: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Error during authentication: {e}")
            return False
    
    def get_usage_data(
        self,
        service_location_number: str,
        account_number: str,
        start_datetime: int,
        end_datetime: int,
        time_frame: str = "HOURLY",
        industries: list[str] = None,
        include_demand: bool = False,
        max_retries: int = 10,
        retry_delay: int = 2
    ) -> dict:
        """
        Retrieve energy usage data from the utility API.
        
        Args:
            service_location_number: Service location number (e.g., "5101185035")
            account_number: Account number (e.g., "490118")
            start_datetime: Start time in milliseconds since epoch
            end_datetime: End time in milliseconds since epoch
            time_frame: Time frame for data (HOURLY, DAILY, MONTHLY, etc.)
            industries: List of industries to query (WATER, GAS, ELECTRIC)
            include_demand: Whether to include demand data
            max_retries: Maximum number of polling attempts (default: 10)
            retry_delay: Seconds to wait between polling attempts (default: 2)
            
        Returns:
            dict: Usage data response from API
        """
        if industries is None:
            industries = ["WATER", "GAS", "ELECTRIC"]
        
        usage_url = f"{self.base_url}/services/secured/utility-usage/poll"
        
        payload = {
            "timeFrame": time_frame,
            "userId": self.username,
            "screen": "USAGE_EXPLORER",
            "includeDemand": include_demand,
            "serviceLocationNumber": service_location_number,
            "accountNumber": account_number,
            "industries": industries,
            "startDateTime": start_datetime,
            "endDateTime": end_datetime
        }
        
        try:
            print(f"\nFetching usage data from {usage_url}...")
            print(f"Time frame: {time_frame}")
            print(f"Industries: {', '.join(industries)}")
            
            # Initial request
            response = self.session.post(
                usage_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code != 200:
                print(f"✗ Failed to retrieve usage data. Status code: {response.status_code}")
                print(f"Response: {response.text}")
                return None
            
            data = response.json()
            
            # Check if data is pending - poll until ready
            retry_count = 0
            while data.get("status") == "PENDING" and retry_count < max_retries:
                retry_count += 1
                print(f"⏳ Data is pending... (attempt {retry_count}/{max_retries})")
                time.sleep(retry_delay)
                
                # Poll again with same payload
                response = self.session.post(
                    usage_url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    data = response.json()
                else:
                    print(f"✗ Polling failed. Status code: {response.status_code}")
                    return None
            
            # Check final status
            if data.get("status") == "PENDING":
                print(f"✗ Data still pending after {max_retries} attempts. Try again later or increase retry limit.")
                return None
            elif data.get("status") == "COMPLETE" or "data" in data or len(data.keys()) > 1:
                print("✓ Usage data retrieved successfully!")
                return data
            else:
                print(f"✓ Response received with status: {data.get('status', 'unknown')}")
                return data
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Error retrieving usage data: {e}")
            return None


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Retrieve energy usage data from your utility provider"
    )
    parser.add_argument(
        "-u", "--username",
        help="Utility account username/email"
    )
    parser.add_argument(
        "-p", "--password",
        help="Utility account password"
    )
    parser.add_argument(
        "-s", "--service-location",
        help="Service location number"
    )
    parser.add_argument(
        "-a", "--account-number",
        help="Account number"
    )
    parser.add_argument(
        "-d", "--days",
        type=int,
        default=1,
        help="Number of days of historical data to retrieve (default: 1)"
    )
    parser.add_argument(
        "-t", "--time-frame",
        choices=["HOURLY", "DAILY", "MONTHLY"],
        default="HOURLY",
        help="Time frame for data aggregation (default: HOURLY)"
    )
    parser.add_argument(
        "-i", "--industries",
        nargs="+",
        choices=["WATER", "GAS", "ELECTRIC"],
        default=["WATER", "GAS", "ELECTRIC"],
        help="Industries to query (default: all)"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output file path for JSON data (optional)"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save data to Delta Lake (only output JSON)"
    )
    parser.add_argument(
        "--delta-path",
        default="./energy_data",
        help="Path to Delta Lake storage directory (default: ./energy_data)"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=10,
        help="Maximum number of polling attempts for pending data (default: 10)"
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=2,
        help="Seconds to wait between polling attempts (default: 2)"
    )
    
    return parser.parse_args()


def get_credentials(args) -> tuple[Optional[str], Optional[str]]:
    """
    Get credentials from command line arguments or .env file.
    Command line arguments take precedence over .env file.
    
    Returns:
        tuple: (username, password) or (None, None) if not found
    """
    
    # Priority 1: Command line arguments
    if args.username and args.password:
        print("Using credentials from command line arguments")
        return args.username, args.password
    
    # Priority 2: Environment variables from .env file
    load_dotenv()
    env_username = os.getenv("UTILITY_USERNAME")
    env_password = os.getenv("UTILITY_PASSWORD")
    
    if env_username and env_password:
        print("Using credentials from .env file")
        return env_username, env_password
    
    # No credentials found
    return None, None


def main():
    """Main entry point for the energy usage application."""
    print("=== Energy Usage Data Retrieval ===\n")
    
    # Parse arguments
    args = parse_arguments()
    
    # Get credentials
    username, password = get_credentials(args)
    
    if not username or not password:
        print("✗ Error: No credentials provided!")
        print("\nPlease provide credentials either by:")
        print("  1. Command line: python main.py -u your_email@gmail.com -p your_password")
        print("  2. Create a .env file (see .env.example)")
        sys.exit(1)
    
    # Get account details from args or env
    service_location = args.service_location or os.getenv("SERVICE_LOCATION_NUMBER")
    account_number = args.account_number or os.getenv("ACCOUNT_NUMBER")
    
    if not service_location or not account_number:
        print("✗ Error: Service location number and account number required!")
        print("\nProvide via command line or add to .env file:")
        print("  SERVICE_LOCATION_NUMBER=your_service_location")
        print("  ACCOUNT_NUMBER=your_account_number")
        sys.exit(1)
    
    # Create API client and authenticate
    client = UtilityAPIClient(username, password)
    
    if not client.authenticate():
        print("\n✗ Failed to authenticate. Please check your credentials.")
        sys.exit(1)
    
    # Calculate time range
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=args.days)
    
    # Convert to milliseconds since epoch
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)
    
    print(f"\nQuerying data from {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
    
    # Get usage data
    usage_data = client.get_usage_data(
        service_location_number=service_location,
        account_number=account_number,
        start_datetime=start_ms,
        end_datetime=end_ms,
        time_frame=args.time_frame,
        industries=args.industries,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay
    )
    
    if usage_data:
        print("\n=== Usage Data Retrieved ===")
        
        # Pretty print a summary
        if isinstance(usage_data, dict):
            print(f"\nResponse keys: {list(usage_data.keys())}")
        
        # Save to Delta Lake unless --no-save is specified
        if not args.no_save and isinstance(usage_data, dict) and usage_data.get('status') == 'COMPLETE':
            print("\n=== Saving to Delta Lake ===")
            storage = EnergyDeltaStorage(args.delta_path)
            
            total_records = 0
            
            # Process each industry's data
            if 'data' in usage_data:
                for industry, industry_data in usage_data['data'].items():
                    if not industry_data:
                        continue
                    
                    for dataset in industry_data:
                        # Get data type (USAGE or COST)
                        data_type = dataset.get('type', 'USAGE')
                        
                        # Extract metadata
                        unit_of_measure = dataset.get('unitOfMeasure', 'UNKNOWN')
                        series_list = dataset.get('series', [])
                        
                        for series in series_list:
                            data_points = series.get('data', [])
                            meter_number = series.get('meterNumber', 'unknown')
                            
                            if not data_points:
                                continue
                            
                            # Save based on industry type
                            if industry == 'ELECTRIC':
                                records_written = storage.save_usage_data(
                                    data=data_points,
                                    meter_number=meter_number,
                                    service_location_number=service_location,
                                    account_number=account_number,
                                    utility_type='ELECTRIC',
                                    unit_of_measure=unit_of_measure,
                                    time_frame=args.time_frame,
                                    data_type=data_type
                                )
                            elif industry == 'GAS':
                                records_written = storage.save_usage_data(
                                    data=data_points,
                                    meter_number=meter_number,
                                    service_location_number=service_location,
                                    account_number=account_number,
                                    utility_type='GAS',
                                    unit_of_measure=unit_of_measure,
                                    time_frame=args.time_frame,
                                    data_type=data_type
                                )
                            elif industry == 'WATER':
                                records_written = storage.save_usage_data(
                                    data=data_points,
                                    meter_number=meter_number,
                                    service_location_number=service_location,
                                    account_number=account_number,
                                    utility_type='WATER',
                                    unit_of_measure=unit_of_measure,
                                    time_frame=args.time_frame,
                                    data_type=data_type
                                )
                            else:
                                print(f"⚠ Unknown industry type: {industry}")
                                continue
                            
                            total_records += records_written
                            print(f"✓ Saved {records_written} {industry.lower()} {data_type.lower()} records (meter: {meter_number}, {unit_of_measure})")
            
            # Save fetch metadata for each industry queried
            if total_records > 0:
                for industry in args.industries:
                    storage.save_fetch_metadata(
                        industry=industry,
                        time_frame=args.time_frame,
                        start_datetime=start_ms,
                        end_datetime=end_ms,
                        records_written=total_records,
                        service_location_number=service_location,
                        account_number=account_number
                    )
                print(f"\n✓ Total records saved: {total_records}")
                
                # Print stats
                stats = storage.get_stats()
                print(f"\n=== Delta Lake Stats ===")
                if 'overall' in stats:
                    print(f"Total records: {stats['overall']['total_records']}")
                    print(f"Partitions: {stats['overall']['partition_count']}")
                    print(f"Date range: {stats['overall']['date_range']['min']} to {stats['overall']['date_range']['max']}")
                
                # Print per-utility stats
                for utility in ['electric', 'gas', 'water']:
                    if utility in stats:
                        u_stats = stats[utility]
                        print(f"\n{utility.upper()}:")
                        print(f"  Records: {u_stats['total_records']}")
                        print(f"  Total: {u_stats['total_usage']:.2f} {u_stats['unit']}")
                        print(f"  Average: {u_stats['avg_usage']:.2f} {u_stats['unit']}")
        
        # Save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(usage_data, f, indent=2)
            print(f"\n✓ JSON data saved to {args.output}")
        elif args.no_save:
            # Print formatted JSON to console if not saving to Delta
            print("\nData preview:")
            print(json.dumps(usage_data, indent=2)[:1000] + "..." if len(str(usage_data)) > 1000 else json.dumps(usage_data, indent=2))
    else:
        print("\n✗ Failed to retrieve usage data.")
        sys.exit(1)


if __name__ == "__main__":
    main()
