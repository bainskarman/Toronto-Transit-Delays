import requests
import json
import csv
import os
import zipfile
import pandas as pd
from datetime import datetime, timedelta
import time
import math
import random
import re
import numpy as np
from io import BytesIO, StringIO
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Any, Optional

class TTCDataTransformer:
    def __init__(self, save_intermediate=False):
        self.gtfs_package_id = "b811ead4-6eaf-4adb-8408-d389fb5a069c"
        self.delay_package_id = "e271cdae-8788-4980-96ce-6a5c95bc6618"
        self.base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action"

        # Paths - fixed for Jupyter compatibility
        try:
            # This works when running as a script
            self.script_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            # This works in Jupyter notebooks
            self.script_dir = os.getcwd()

        self.input_data_folder = os.path.join(self.script_dir, "input_data")
        self.output_data_folder = os.path.join(self.script_dir, "assets", "data")

        # Create folders
        self.ensure_folder_exists(self.input_data_folder)
        self.ensure_folder_exists(self.output_data_folder)

        # Get current year for file filtering
        self.current_year = datetime.now().year
        self.analysis_year = 2025  # Focus on 2025 for YoY comparisons

        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TTC-Data-Transformer/1.0'
        })

        # Flag to control intermediate file saving
        self.save_intermediate = save_intermediate

        # Route mapping dictionary
        self.route_name_mapping = {}

        # Data storage
        self.delay_data = None
        self.cleaned_delay_data = None
        self.gtfs_data = None
        self.routes_in_2025 = set()  # Store routes that exist in 2025

        # Dashboard datasets
        self.dashboard_datasets = {}

    def ensure_folder_exists(self, folder_path):
        """Create folder if it doesn't exist"""
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"📁 Created folder: {folder_path}")

    def fetch_package(self, package_id):
        """Fetch package information from CKAN API"""
        url = f"{self.base_url}/package_show?id={package_id}"
        response = self.session.get(url)
        response.raise_for_status()
        data = response.json()

        if not data.get('success'):
            raise Exception(f"API request failed: {data.get('error', {}).get('message', 'Unknown error')}")

        return data['result']

    def clean_and_standardize(self, df):
        """Standardize column names and enforce a uniform schema."""
        # Create a copy to avoid modifying the original
        df = df.copy()

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Comprehensive column mapping for all possible column names
        column_mappings = {
            # Date columns
            'Date': ['Report Date', 'Date', 'Incident Date', 'Date & Time'],
            # Time columns
            'Time': ['Time', 'Incident Time'],
            # Day columns
            'Day': ['Day'],
            # Location columns
            'Location': ['Location', 'Station', 'Station Name', 'Stop', 'Stop Name'],
            # Incident columns
            'Incident': ['Incident', 'Code', 'Description'],
            # Delay columns
            'Min Delay': ['Min Delay', 'Delay', 'Delay Minutes', 'Delay_Minutes'],
            # Gap columns
            'Min Gap': ['Min Gap', 'Gap', 'Gap Minutes', 'Gap_Minutes'],
            # Route/Line columns
            'Route': ['Route', 'Route Number', 'Route No', 'Route_ID'],
            'Line': ['Line'],
            # Direction columns
            'Direction': ['Direction', 'Bound'],
            # Vehicle columns
            'Vehicle': ['Vehicle', 'Vehicle Number', 'Vehicle_No']
        }

        # Reverse mapping: from possible column names to standard names
        reverse_mapping = {}
        for standard_name, possible_names in column_mappings.items():
            for name in possible_names:
                reverse_mapping[name] = standard_name

        # Rename columns based on our mapping
        rename_dict = {}
        for col in df.columns:
            if col in reverse_mapping:
                rename_dict[col] = reverse_mapping[col]

        df = df.rename(columns=rename_dict)

        # Handle Line column specifically - it contains both route number and name
        if 'Line' in df.columns and 'Route' not in df.columns:
            def extract_route_info(line_value):
                if pd.isna(line_value):
                    return pd.Series([None, None])

                line_str = str(line_value).strip()

                # Pattern: digits followed by optional space and text (route name)
                match = re.match(r'^(\d+)(?:\s+(.+))?$', line_str)
                if match:
                    route_num = match.group(1)
                    route_name = match.group(2) if match.group(2) else ''
                    return pd.Series([route_num, route_name])
                else:
                    # Try to extract just digits
                    match = re.match(r'^(\d+)$', line_str)
                    if match:
                        return pd.Series([match.group(1), ''])
                    else:
                        return pd.Series([line_str, ''])

            df[['Route', 'Route Name']] = df['Line'].apply(extract_route_info)

        # If we have Route but not Route Name, initialize it
        if 'Route' in df.columns and 'Route Name' not in df.columns:
            df['Route Name'] = ''

        # Ensure all required columns exist
        required_columns = [
            'Date', 'Route', 'Route Name', 'Time', 'Day', 'Location',
            'Incident', 'Min Delay', 'Min Gap', 'Direction', 'Vehicle'
        ]

        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan

        # Select only the required columns in the correct order
        df = df[required_columns]

        return df

    def load_and_merge_ttc_bus_delay_data(self):
        """
        Load TTC bus delay files and merge all years (2014-2025)
        Handles multi‑sheet Excel files (pre‑2022) correctly.
        """
        print("🚌 Loading and merging TTC Bus Delay Data (2014-2025)...")

        base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
        package_id = "ttc-bus-delay-data"

        # Get package metadata
        package_url = f"{base_url}/api/3/action/package_show"
        package = self.session.get(package_url, params={"id": package_id}).json()
        resources = package["result"]["resources"]

        keyword = "ttc"
        current_year = datetime.now().year
        valid_years = set(range(2014, current_year + 1))
        year_pattern = re.compile(r"(19|20)\d{2}")

        all_dfs = []

        # Step 1: Load all files
        for res in resources:
            name = res.get("name", "")
            fmt = res.get("format", "").lower()

            if keyword not in name.lower():
                continue

            match = year_pattern.search(name)
            if not match:
                continue

            year = int(match.group(0))
            if year not in valid_years:
                continue

            expected_ext = "xlsx" if year < current_year-1 else "csv"
            if fmt != expected_ext:
                continue

            print(f"📥 Downloading {name} (year={year}, type={fmt})")
            url = res["url"]
            response = self.session.get(url)

            try:
                if fmt == "xlsx":
                    # Read all sheets and concatenate
                    excel_data = pd.read_excel(BytesIO(response.content), sheet_name=None)
                    sheet_dfs = []
                    for sheet_name, sheet_df in excel_data.items():
                        # Skip empty sheets or metadata sheets if any
                        if sheet_df.empty:
                            continue
                        # Clean each sheet separately
                        sheet_df = self.clean_and_standardize(sheet_df)
                        sheet_dfs.append(sheet_df)
                    if sheet_dfs:
                        df = pd.concat(sheet_dfs, ignore_index=True)
                    else:
                        print(f"⚠️ No data found in any sheet of {name}")
                        continue
                else:
                    df = pd.read_csv(StringIO(response.text))
                    df = self.clean_and_standardize(df)

                all_dfs.append(df)
                print(f"✅ Loaded {len(df)} records from {name}")

            except Exception as e:
                print(f"❌ Error processing {name}: {e}")
                continue

        # Step 2: Merge all datasets
        if not all_dfs:
            raise Exception("No valid delay data files found")

        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"📊 Merged {len(all_dfs)} files into {len(final_df)} total records")

        return final_df

    def download_delay_data(self):
        """Download ALL TTC Bus Delay Data"""
        print("🚌 Downloading ALL TTC Bus Delay Data (2014-2025)...")

        try:
            df = self.load_and_merge_ttc_bus_delay_data()
            self.delay_data = df

            # Save merged CSV to output folder (assets/data)
            merged_csv_path = os.path.join(self.output_data_folder, "all_delay_data_merged.csv")
            print(f"💾 Saving merged delay data to: {merged_csv_path}")
            df.to_csv(merged_csv_path, index=False)

            print(f"✅ Successfully saved {len(df)} records to assets/data folder")
            return df

        except Exception as e:
            print(f"❌ Error downloading delay data: {e}")
            import traceback
            traceback.print_exc()
            raise

    def download_gtfs_data(self):
        """Download and extract GTFS data"""
        print("🗺️ Downloading GTFS Data...")

        package_info = self.fetch_package(self.gtfs_package_id)
        print(f"📦 Package: {package_info['title']}")

        # Find the Complete GTFS resource
        gtfs_resource = None
        for resource in package_info['resources']:
            if ('complete gtfs' in resource.get('name', '').lower() or
                'completegtfs' in resource.get('name', '').lower()):
                gtfs_resource = resource
                break

        if not gtfs_resource:
            raise Exception("Complete GTFS resource not found")

        print(f"📥 Downloading GTFS ZIP from: {gtfs_resource['url']}")

        # Download GTFS ZIP to memory
        response = self.session.get(gtfs_resource['url'])
        zip_path = BytesIO(response.content)

        # Extract GTFS files
        print("🔧 Extracting GTFS files...")
        gtfs_data = self.extract_gtfs_files(zip_path)

        return gtfs_data

    def extract_gtfs_files(self, zip_path):
        """Extract required files from GTFS ZIP"""
        gtfs_data = {}
        required_files = ['routes.txt', 'trips.txt', 'shapes.txt', 'stops.txt']

        try:
            # From BytesIO
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()

            print(f"📁 Files in GTFS ZIP: {len(file_list)}")

            # Build route name mapping from routes.txt
            if 'routes.txt' in file_list:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    with zip_ref.open('routes.txt') as file:
                        routes_content = file.read().decode('utf-8')

                # Parse routes.txt to build mapping
                routes_df = pd.read_csv(StringIO(routes_content))
                if 'route_short_name' in routes_df.columns and 'route_long_name' in routes_df.columns:
                    for _, row in routes_df.iterrows():
                        route_short = str(row['route_short_name']).strip()
                        route_long = str(row['route_long_name']).strip()
                        self.route_name_mapping[route_short] = route_long
                    print(f"✅ Built route name mapping for {len(self.route_name_mapping)} routes")

                gtfs_data['routes.txt'] = routes_content

            # Extract other required files
            for filename in required_files:
                if filename in file_list:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        with zip_ref.open(filename) as file:
                            content = file.read().decode('utf-8')

                    gtfs_data[filename] = content
                    print(f"✅ Extracted: {filename}")
                else:
                    print(f"⚠️ Missing: {filename}")

            return gtfs_data

        except Exception as e:
            print(f"❌ Error extracting GTFS files: {e}")
            raise

    def clean_delay_data(self, delay_data):
        """Clean and convert delay data types"""
        print("🧹 Cleaning delay data types...")

        # Convert to DataFrame if it's not already
        if isinstance(delay_data, pd.DataFrame):
            df = delay_data.copy()
        else:
            df = pd.DataFrame(delay_data)

        # Ensure column names are properly formatted
        df.columns = [str(col).strip() for col in df.columns]

        # Handle Date column
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

        # Handle Time column and extract hour
        if 'Time' in df.columns:
            def extract_hour(time_val):
                if pd.isna(time_val):
                    return None
                time_str = str(time_val)
                # Look for HH:MM pattern
                match = re.search(r'(\d{1,2}):', time_str)
                if match:
                    try:
                        return int(match.group(1))
                    except:
                        return None
                return None

            df['Hour'] = df['Time'].apply(extract_hour)

        # Handle Min Delay column
        if 'Min Delay' in df.columns:
            df['Min Delay'] = pd.to_numeric(df['Min Delay'], errors='coerce')

        # Handle Min Gap column
        if 'Min Gap' in df.columns:
            df['Min Gap'] = pd.to_numeric(df['Min Gap'], errors='coerce')

        # Handle Vehicle column
        if 'Vehicle' in df.columns:
            df['Vehicle'] = pd.to_numeric(df['Vehicle'], errors='coerce')

        # Ensure Route column is string and extract only digits
        if 'Route' in df.columns:
            df['Route'] = df['Route'].astype(str)
            df['Route'] = df['Route'].str.extract(r'^(\d+)')[0]

        # Extract date components
        if 'Date' in df.columns:
            df['Month'] = df['Date'].dt.month_name().str[:3]
            df['Year'] = df['Date'].dt.year
            df['Weekday'] = df['Date'].dt.day_name()

        # Clean up string columns
        for col in ['Time', 'Day', 'Location', 'Incident', 'Direction', 'Route Name']:
            if col in df.columns:
                df[col] = df[col].astype(str).replace({'nan': '', 'None': '', 'NaT': ''}).str.strip()

        # STANDARDIZE ROUTE NAME CASING - FIX FOR CASE INSENSITIVE GROUPING
        if 'Route Name' in df.columns:
            # Apply proper title case to route names to standardize them
            def standardize_route_name(name):
                if pd.isna(name) or name == '':
                    return name

                # First, convert to string and strip whitespace
                name_str = str(name).strip()

                # Special handling for hyphenated names like "LAWRENCE-DONWAY"
                # Convert to proper title case while preserving hyphens
                if '-' in name_str:
                    parts = name_str.split('-')
                    standardized = '-'.join([part.strip().title() for part in parts])
                    return standardized
                else:
                    # For non-hyphenated names, just use title case
                    return name_str.title()

            df['Route Name'] = df['Route Name'].apply(standardize_route_name)

        # Apply route name mapping
        if self.route_name_mapping:
            # Also standardize the mapping values
            standardized_mapping = {}
            for route_num, route_name in self.route_name_mapping.items():
                if pd.isna(route_name) or route_name == '':
                    standardized_mapping[route_num] = route_name
                else:
                    # Apply same standardization to mapping
                    if '-' in route_name:
                        parts = route_name.split('-')
                        standardized_mapping[route_num] = '-'.join([part.strip().title() for part in parts])
                    else:
                        standardized_mapping[route_num] = route_name.strip().title()

            df['Route Name'] = df.apply(
                lambda row: standardized_mapping.get(str(row['Route']), row['Route Name'])
                if pd.isna(row['Route Name']) or row['Route Name'] == ''
                else row['Route Name'],
                axis=1
            )

        print(f"✅ Cleaned data: {len(df)} records")
        return df

    def filter_to_routes_in_2025(self, df):
        """Filter DataFrame to only include routes that have at least 1 row in 2025"""
        print("🔍 Identifying routes active in 2025...")

        # Get routes that have at least 1 record in 2025
        routes_2025 = set(df[df['Year'] == 2025]['Route'].dropna().unique())
        self.routes_in_2025 = routes_2025

        print(f"📊 Found {len(routes_2025)} routes active in 2025")

        # Filter the entire DataFrame to only include these routes (but keep all historical data)
        filtered_df = df[df['Route'].isin(routes_2025)].copy()

        print(f"📈 Filtered dataset: {len(filtered_df)} records (all years) for routes active in 2025")
        print(f"   - Original dataset: {len(df)} records")
        print(f"   - Percentage kept: {len(filtered_df)/len(df)*100:.1f}%")

        # Show sample of routes
        if routes_2025:
            sample_routes = list(routes_2025)[:10]
            print(f"   - Sample routes in 2025: {sample_routes}")

        return filtered_df

    def process_route_performance_with_trips(self):
        """
        Generate route performance data including trip variations.
        Uses the unfiltered delay dataset (all routes) and joins with trips.txt.
        Returns a list of dicts suitable for saving as CSV.
        The 'Route' column contains the route number concatenated with the trip
        short name (e.g., '100A', '100B'). If no trip short name exists, only
        the route number is used.
        Adds 'active_in_2025' column indicating if the base route is active in 2025.
        Filters out routes with fewer than 100 total delays.
        """
        print("🚏 Processing route performance with trip variations (all routes)...")

        # Use unfiltered data
        df = self.cleaned_all_delay_data if hasattr(self, 'cleaned_all_delay_data') else self.cleaned_delay_data
        df_valid = df[df['Min Delay'] > 0].copy()

        if df_valid.empty:
            print("⚠️ No valid delay data for route performance.")
            return []

        # Aggregate per route (basic stats)
        route_agg = df_valid.groupby('Route').agg(
            Delay_Count=('Min Delay', 'count'),
            Avg_Delay_Min=('Min Delay', 'mean'),
            Total_Delay_Min=('Min Delay', 'sum'),
            Unique_Vehicles=('Vehicle', 'nunique')
        ).reset_index()

        # Filter out routes with fewer than 100 delays
        original_route_count = len(route_agg)
        route_agg = route_agg[route_agg['Delay_Count'] >= 100]
        filtered_route_count = len(route_agg)
        print(f"   - Routes with ≥100 delays: {filtered_route_count} (removed {original_route_count - filtered_route_count})")

        if route_agg.empty:
            print("⚠️ No routes with at least 100 delays.")
            return []

        # Approximate Delays_Per_Day (using full date range)
        date_min = df_valid['Date'].min()
        date_max = df_valid['Date'].max()
        if pd.notna(date_min) and pd.notna(date_max):
            days_span = (date_max - date_min).days + 1
        else:
            days_span = 365  # fallback

        route_agg['Delays_Per_Day'] = (route_agg['Delay_Count'] / days_span).round(2)

        # Add route name from mapping or original data
        route_names = df_valid.groupby('Route')['Route Name'].first().to_dict()
        route_agg['route_long_name'] = route_agg['Route'].map(route_names).fillna('')

        # --- Load trips.txt and get distinct route_id + trip_short_name ---
        trips_df = None
        if self.gtfs_data and 'trips.txt' in self.gtfs_data:
            try:
                trips_content = self.gtfs_data['trips.txt']
                trips_df = pd.read_csv(StringIO(trips_content))
                # Keep only needed columns and distinct pairs
                if 'route_id' in trips_df.columns and 'trip_short_name' in trips_df.columns:
                    # Convert route_id to string for safe join
                    trips_df['route_id'] = trips_df['route_id'].astype(str)
                    trips_df['trip_short_name'] = trips_df['trip_short_name'].fillna('').astype(str)
                    distinct_pairs = trips_df[['route_id', 'trip_short_name']].drop_duplicates()
                else:
                    print("⚠️ trips.txt missing required columns (route_id, trip_short_name)")
                    distinct_pairs = pd.DataFrame(columns=['route_id', 'trip_short_name'])
            except Exception as e:
                print(f"⚠️ Could not parse trips.txt: {e}")
                distinct_pairs = pd.DataFrame(columns=['route_id', 'trip_short_name'])
        else:
            print("⚠️ trips.txt not found in GTFS data")
            distinct_pairs = pd.DataFrame(columns=['route_id', 'trip_short_name'])

        # --- Build final list by joining route aggregates with distinct pairs ---
        result = []
        for _, route_row in route_agg.iterrows():
            route_num = str(route_row['Route'])
            # Determine if base route is active in 2025
            active_in_2025 = route_num in self.routes_in_2025

            # Find all trip variations for this route from trips.txt
            route_pairs = distinct_pairs[distinct_pairs['route_id'] == route_num]
            if route_pairs.empty:
                # Route appears in delays but not in trips.txt – create one row with plain route number
                result.append({
                    'Route': route_num,  # no suffix
                    'Delay_Count': int(route_row['Delay_Count']),
                    'Avg_Delay_Min': round(route_row['Avg_Delay_Min'], 2),
                    'Total_Delay_Min': round(route_row['Total_Delay_Min'], 2),
                    'Unique_Vehicles': int(route_row['Unique_Vehicles']),
                    'Delays_Per_Day': route_row['Delays_Per_Day'],
                    'active_in_2025': active_in_2025,
                    'route_long_name': route_row['route_long_name']
                })
            else:
                for _, pair in route_pairs.iterrows():
                    trip_suffix = pair['trip_short_name'].strip()
                    # Combine route number and suffix (e.g., "100" + "A" -> "100A")
                    combined_route = route_num + trip_suffix if trip_suffix else route_num
                    result.append({
                        'Route': combined_route,
                        'Delay_Count': int(route_row['Delay_Count']),
                        'Avg_Delay_Min': round(route_row['Avg_Delay_Min'], 2),
                        'Total_Delay_Min': round(route_row['Total_Delay_Min'], 2),
                        'Unique_Vehicles': int(route_row['Unique_Vehicles']),
                        'Delays_Per_Day': route_row['Delays_Per_Day'],
                        'active_in_2025': active_in_2025,
                        'route_long_name': route_row['route_long_name']
                    })

        print(f"✅ Route performance with trip variations: {len(result)} rows")
        return result


if __name__ == "__main__":
    # --------------------------------------------------------------------
    # This standalone block generates ONLY the route_performance.csv file
    # --------------------------------------------------------------------
    print("=" * 60)
    print("🚍 TTC ROUTE PERFORMANCE GENERATOR (standalone)")
    print("=" * 60)

    transformer = TTCDataTransformer(save_intermediate=False)

    # 1. Download GTFS data (needed for trip variations)
    print("\n📥 Downloading GTFS data...")
    transformer.gtfs_data = transformer.download_gtfs_data()

    # 2. Download delay data
    print("\n📥 Downloading delay data...")
    transformer.delay_data = transformer.download_delay_data()

    # 3. Clean the delay data
    print("\n🧹 Cleaning delay data...")
    transformer.cleaned_delay_data = transformer.clean_delay_data(transformer.delay_data)

    # 4. Keep an unfiltered copy for all‑routes analysis
    transformer.cleaned_all_delay_data = transformer.cleaned_delay_data.copy()

    # 5. Identify routes active in 2025 (needed for the active_in_2025 column)
    print("\n🔍 Identifying routes active in 2025...")
    transformer.filter_to_routes_in_2025(transformer.cleaned_delay_data)

    # 6. Generate the route performance data (with trip variations)
    print("\n🚏 Generating route performance data...")
    route_perf_data = transformer.process_route_performance_with_trips()

    if not route_perf_data:
        print("❌ No route performance data generated.")
        exit(1)

    # 7. Save as CSV
    df = pd.DataFrame(route_perf_data)
    output_path = os.path.join(transformer.output_data_folder, "route_performance.csv")
    df.to_csv(output_path, index=False)
    print(f"\n✅ Route performance CSV saved to: {output_path}")
    print(f"   Total rows: {len(df)}")
    print("\n✨ Done.")