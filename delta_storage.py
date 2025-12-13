"""Delta Lake storage module for energy usage data."""

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from deltalake import DeltaTable, write_deltalake


class EnergyDeltaStorage:
    """Delta Lake storage handler for energy usage data."""
    
    def __init__(self, base_path: str = "./energy_data"):
        """
        Initialize Delta Lake storage.
        
        Args:
            base_path: Base directory for Delta tables
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        
        # Single unified usage table for all utilities
        self.usage_path = str(self.base_path / "usage")
        self.metadata_path = str(self.base_path / "fetch_metadata")
    
    def save_usage_data(
        self,
        data: list[dict],
        meter_number: str,
        service_location_number: str,
        account_number: str,
        utility_type: str,
        unit_of_measure: str,
        time_frame: str = "HOURLY",
        data_type: str = "USAGE"
    ) -> int:
        """
        Save utility usage data to Delta Lake (electricity, gas, water).
        
        Args:
            data: List of data points with 'x' (timestamp) and 'y' (usage value) values
            meter_number: Meter identifier
            service_location_number: Service location identifier
            account_number: Account number
            utility_type: Type of utility (ELECTRIC, GAS, WATER)
            unit_of_measure: Unit of measurement (KWH, CCF, GAL, etc.)
            time_frame: Time frame of the data (HOURLY, DAILY, etc.)
            data_type: Type of data (USAGE or COST)
            
        Returns:
            Number of records written
        """
        if not data:
            return 0
        
        # Convert API data to pandas DataFrame
        records = []
        for point in data:
            timestamp_ms = point['x']
            usage_value = point['y']
            
            # Convert milliseconds to datetime
            dt = pd.to_datetime(timestamp_ms, unit='ms', utc=True)
            
            records.append({
                'timestamp_ms': timestamp_ms,
                'datetime_utc': dt,
                'date': dt.date(),
                'year': dt.year,
                'month': dt.month,
                'day': dt.day,
                'hour': dt.hour if time_frame == "HOURLY" else None,
                'usage_value': usage_value,
                'unit_of_measure': unit_of_measure,
                'utility_type': utility_type,
                'data_type': data_type,
                'meter_number': meter_number,
                'service_location_number': service_location_number,
                'account_number': account_number,
                'time_frame': time_frame,
                'ingested_at': pd.Timestamp.now(tz='UTC')
            })
        
        df = pd.DataFrame(records)
        
        # Check if table exists for merge, otherwise create it
        try:
            dt = DeltaTable(self.usage_path)
            
            # Perform merge (upsert) to avoid duplicates
            # Match on timestamp_ms + meter_number as unique key
            (
                dt.merge(
                    source=df,
                    predicate="t.timestamp_ms = s.timestamp_ms AND t.meter_number = s.meter_number AND t.utility_type = s.utility_type AND t.data_type = s.data_type",
                    source_alias="s",
                    target_alias="t"
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            
        except Exception:
            # Table doesn't exist yet, create it with initial write
            write_deltalake(
                self.usage_path,
                df,
                mode="append",
                schema_mode="merge",
                partition_by=["date", "utility_type", "data_type"]
            )
        
        return len(records)
    
    def save_electricity_data(
        self,
        data: list[dict],
        meter_number: str,
        service_location_number: str,
        account_number: str,
        time_frame: str = "HOURLY"
    ) -> int:
        """
        Save electricity usage data to Delta Lake.
        Convenience wrapper for save_usage_data with ELECTRIC type.
        
        Args:
            data: List of data points with 'x' (timestamp) and 'y' (kwh) values
            meter_number: Meter identifier
            service_location_number: Service location identifier
            account_number: Account number
            time_frame: Time frame of the data (HOURLY, DAILY, etc.)
            
        Returns:
            Number of records written
        """
        return self.save_usage_data(
            data=data,
            meter_number=meter_number,
            service_location_number=service_location_number,
            account_number=account_number,
            utility_type="ELECTRIC",
            unit_of_measure="KWH",
            time_frame=time_frame
        )
    
    def save_gas_data(
        self,
        data: list[dict],
        meter_number: str,
        service_location_number: str,
        account_number: str,
        unit_of_measure: str = "CCF",
        time_frame: str = "HOURLY"
    ) -> int:
        """
        Save gas usage data to Delta Lake.
        
        Args:
            data: List of data points with 'x' (timestamp) and 'y' (usage) values
            meter_number: Meter identifier
            service_location_number: Service location identifier
            account_number: Account number
            unit_of_measure: Unit of measurement (CCF, THERM, etc.)
            time_frame: Time frame of the data (HOURLY, DAILY, etc.)
            
        Returns:
            Number of records written
        """
        return self.save_usage_data(
            data=data,
            meter_number=meter_number,
            service_location_number=service_location_number,
            account_number=account_number,
            utility_type="GAS",
            unit_of_measure=unit_of_measure,
            time_frame=time_frame
        )
    
    def save_water_data(
        self,
        data: list[dict],
        meter_number: str,
        service_location_number: str,
        account_number: str,
        unit_of_measure: str = "GAL",
        time_frame: str = "HOURLY"
    ) -> int:
        """
        Save water usage data to Delta Lake.
        
        Args:
            data: List of data points with 'x' (timestamp) and 'y' (usage) values
            meter_number: Meter identifier
            service_location_number: Service location identifier
            account_number: Account number
            unit_of_measure: Unit of measurement (GAL, CCF, etc.)
            time_frame: Time frame of the data (HOURLY, DAILY, etc.)
            
        Returns:
            Number of records written
        """
        return self.save_usage_data(
            data=data,
            meter_number=meter_number,
            service_location_number=service_location_number,
            account_number=account_number,
            utility_type="WATER",
            unit_of_measure=unit_of_measure,
            time_frame=time_frame
        )
    
    def save_fetch_metadata(
        self,
        industry: str,
        time_frame: str,
        start_datetime: int,
        end_datetime: int,
        records_written: int,
        service_location_number: str,
        account_number: str
    ):
        """
        Save metadata about a data fetch operation.
        
        Args:
            industry: Industry type (ELECTRIC, GAS, WATER)
            time_frame: Time frame queried
            start_datetime: Start timestamp in ms
            end_datetime: End timestamp in ms
            records_written: Number of records written
            service_location_number: Service location identifier
            account_number: Account number
        """
        metadata = pd.DataFrame([{
            'fetch_timestamp': pd.Timestamp.now(tz='UTC'),
            'industry': industry,
            'time_frame': time_frame,
            'start_datetime_ms': start_datetime,
            'end_datetime_ms': end_datetime,
            'start_datetime': pd.to_datetime(start_datetime, unit='ms', utc=True),
            'end_datetime': pd.to_datetime(end_datetime, unit='ms', utc=True),
            'records_written': records_written,
            'service_location_number': service_location_number,
            'account_number': account_number
        }])
        
        write_deltalake(
            self.metadata_path,
            metadata,
            mode="append",
            schema_mode="merge"
        )
    
    def read_usage_data(
        self,
        utility_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        meter_number: Optional[str] = None,
        data_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Read usage data from Delta Lake.
        
        Args:
            utility_type: Filter by utility type (ELECTRIC, GAS, WATER)
            start_date: Start date filter (ISO format)
            end_date: End date filter (ISO format)
            meter_number: Filter by meter number
            data_type: Filter by data type (USAGE or COST)
            
        Returns:
            DataFrame with usage data
        """
        try:
            dt = DeltaTable(self.usage_path)
            
            # Build partition filters for efficient querying
            filters = []
            if utility_type:
                filters.append(("utility_type", "=", utility_type))
            if data_type:
                filters.append(("data_type", "=", data_type))
            
            # Use to_pandas with filters if available
            if filters:
                df = dt.to_pandas(filters=filters)
            else:
                df = dt.to_pandas()
            
            # Apply additional filters
            if start_date:
                start_dt = pd.to_datetime(start_date, utc=True)
                df = df[df['datetime_utc'] >= start_dt]
            if end_date:
                # Add one day to end_date to include the full day
                end_dt = pd.to_datetime(end_date, utc=True) + pd.Timedelta(days=1)
                df = df[df['datetime_utc'] < end_dt]
            if meter_number:
                df = df[df['meter_number'] == meter_number]
            
            return df.sort_values('datetime_utc')
        except Exception as e:
            print(f"Error reading Delta table: {e}")
            return pd.DataFrame()
    
    def read_electricity_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        meter_number: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Read electricity data from Delta Lake.
        Convenience wrapper for read_usage_data with ELECTRIC filter.
        
        Args:
            start_date: Start date filter (ISO format)
            end_date: End date filter (ISO format)
            meter_number: Filter by meter number
            
        Returns:
            DataFrame with electricity usage data
        """
        return self.read_usage_data(
            utility_type="ELECTRIC",
            start_date=start_date,
            end_date=end_date,
            meter_number=meter_number
        )
    
    def get_stats(self) -> dict:
        """Get statistics about stored data."""
        stats = {}
        
        try:
            # Overall usage stats
            dt = DeltaTable(self.usage_path)
            df = dt.to_pandas()
            
            stats['overall'] = {
                'total_records': len(df),
                'date_range': {
                    'min': str(df['datetime_utc'].min()) if len(df) > 0 else None,
                    'max': str(df['datetime_utc'].max()) if len(df) > 0 else None
                },
                'unique_meters': df['meter_number'].nunique(),
                'table_version': dt.version(),
                'partition_count': len(df[['date', 'utility_type']].drop_duplicates())
            }
            
            # Stats by utility type
            for utility in ['ELECTRIC', 'GAS', 'WATER']:
                utility_df = df[df['utility_type'] == utility]
                if len(utility_df) > 0:
                    stats[utility.lower()] = {
                        'total_records': len(utility_df),
                        'date_range': {
                            'min': str(utility_df['datetime_utc'].min()),
                            'max': str(utility_df['datetime_utc'].max())
                        },
                        'total_usage': float(utility_df['usage_value'].sum()),
                        'avg_usage': float(utility_df['usage_value'].mean()),
                        'unit': utility_df['unit_of_measure'].iloc[0],
                        'unique_meters': utility_df['meter_number'].nunique()
                    }
        except Exception as e:
            stats['usage'] = {'error': f'No usage data found: {e}'}
        
        try:
            # Metadata stats
            dt_meta = DeltaTable(self.metadata_path)
            df_meta = dt_meta.to_pandas()
            
            stats['fetch_history'] = {
                'total_fetches': len(df_meta),
                'last_fetch': str(df_meta['fetch_timestamp'].max()) if len(df_meta) > 0 else None,
                'total_records_fetched': int(df_meta['records_written'].sum())
            }
        except Exception:
            stats['fetch_history'] = {'error': 'No metadata found'}
        
        return stats
    
    def optimize_table(self):
        """
        Optimize the Delta table by compacting small files.
        Run this periodically after many appends.
        """
        try:
            dt = DeltaTable(self.usage_path)
            dt.optimize.compact()
            print(f"✓ Optimized usage table (version {dt.version()})")
        except Exception as e:
            print(f"Could not optimize table: {e}")
    
    def vacuum_table(self, retention_hours: int = 168):
        """
        Remove old data files (default: 7 days retention).
        
        Args:
            retention_hours: Hours of retention for old files
        """
        try:
            dt = DeltaTable(self.usage_path)
            dt.vacuum(retention_hours=retention_hours, enforce_retention_duration=False)
            print(f"✓ Vacuumed old files from usage table")
        except Exception as e:
            print(f"Could not vacuum table: {e}")
