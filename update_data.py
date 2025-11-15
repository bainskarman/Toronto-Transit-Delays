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

class TTCDataTransformer:
    def __init__(self):
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
        
        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TTC-Data-Transformer/1.0'
        })

    def ensure_folder_exists(self, folder_path):
        """Create folder if it doesn't exist"""
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            ##print(f"📁 Created folder: {folder_path}")

    def fetch_package(self, package_id):
        """Fetch package information from CKAN API"""
        url = f"{self.base_url}/package_show?id={package_id}"
        response = self.session.get(url)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('success'):
            raise Exception(f"API request failed: {data.get('error', {}).get('message', 'Unknown error')}")
        
        return data['result']

    def download_file(self, url, filepath):
        """Download file with progress tracking"""
        #print(f"📥 Downloading: {os.path.basename(filepath)}")
        response = self.session.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        
        with open(filepath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
                    downloaded_size += len(chunk)
                    
                    if total_size > 0:
                        percent = (downloaded_size / total_size) * 100
                        #print(f"\r📥 Progress: {percent:.1f}% ({self.format_file_size(downloaded_size)} / {self.format_file_size(total_size)})", end="")
        
        #print("\n✅ Download completed")
        return filepath

    def format_file_size(self, size_bytes):
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0B"
        
        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names)-1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.2f} {size_names[i]}"

    def extract_year_from_filename(self, filename):
        """Extract year from filename for date filtering"""
        # Look for 4-digit years in filename
        years = re.findall(r'\b(20\d{2})\b', filename)
        if years:
            return int(years[0])
        return None

    def download_delay_data(self):
        """Download ALL TTC Bus Delay Data with year-based format handling"""
        #print("🚌 Downloading ALL TTC Bus Delay Data...")
        
        package_info = self.fetch_package(self.delay_package_id)
        #print(f"📦 Package: {package_info['title']}")
        
        # NEW: Separate resources by year and format
        excel_resources = []
        csv_resources = []
        
        for resource in package_info['resources']:
            resource_name = resource.get('name', '').lower()
            resource_format = resource.get('format', '').lower()
            
            # Extract year from filename
            file_year = self.extract_year_from_filename(resource_name)
            if not file_year:
                #print(f"⚠️ Could not extract year from: {resource_name}")
                continue
            
            # Check if it's delay data
            if 'delay' in resource_name and 'ttc' in resource_name:
                # For current year, prefer CSV files
                if file_year == self.current_year:
                    if 'csv' in resource_format:
                        csv_resources.append(resource)
                        #print(f"✅ Found CSV for current year {file_year}: {resource_name}")
                    elif 'xlsx' in resource_format or 'xls' in resource_format:
                        excel_resources.append(resource)
                        #print(f"ℹ️ Found Excel for current year {file_year}: {resource_name}")
                # For previous years (2014 to current_year-1), prefer Excel files
                elif 2014 <= file_year < self.current_year:
                    if 'xlsx' in resource_format or 'xls' in resource_format:
                        excel_resources.append(resource)
                        #print(f"✅ Found Excel for year {file_year}: {resource_name}")
                    elif 'csv' in resource_format:
                        csv_resources.append(resource)
                        #print(f"ℹ️ Found CSV for year {file_year}: {resource_name}")
        
        #print(f"\n📊 File Summary:")
        #print(f"   - Excel files (2014-{self.current_year-1}): {len(excel_resources)}")
        #print(f"   - CSV files (current year {self.current_year}): {len(csv_resources)}")
        
        if not excel_resources and not csv_resources:
            raise Exception("No delay files found in package")
        
        # Download and process each file
        all_delay_data = []
        
        # Process Excel files first (historical data)
        for i, resource in enumerate(excel_resources, 1):
            #print(f"\n📁 Processing Excel file {i}/{len(excel_resources)}: {resource['name']}")
            
            try:
                # Download Excel file
                excel_path = os.path.join(self.input_data_folder, f"delay_data_excel_{i}.xlsx")
                self.download_file(resource['url'], excel_path)
                
                # Read Excel file with multiple engine attempts
                #print(f"🔍 Reading Excel file: {resource['name']}")
                excel_data = self.read_excel_file(excel_path)
                
                if excel_data is not None:
                    # Add source file information
                    excel_data['source_file'] = resource['name']
                    excel_data['file_year'] = self.extract_year_from_filename(resource['name'])
                    
                    #print(f"✅ Loaded {len(excel_data)} records from {resource['name']}")
                    #print(f"   Columns: {list(excel_data.columns)}")
                    #print(f"   Shape: {excel_data.shape}")
                    
                    # Convert to list of dictionaries
                    file_records = excel_data.to_dict('records')
                    all_delay_data.extend(file_records)
                    
                    #print(f"📈 Total records so far: {len(all_delay_data)}")
                else:
                    print(f"❌ Failed to read Excel file: {resource['name']}")
                
            except Exception as e:
                #print(f"❌ Error processing Excel {resource['name']}: {e}")
                continue
        
        # Process CSV files (current year data)
        for i, resource in enumerate(csv_resources, 1):
            #print(f"\n📁 Processing CSV file {i}/{len(csv_resources)}: {resource['name']}")
            
            try:
                # Download CSV file
                csv_path = os.path.join(self.input_data_folder, f"delay_data_csv_{i}.csv")
                self.download_file(resource['url'], csv_path)
                
                # Read CSV file
                #print(f"🔍 Reading CSV file: {resource['name']}")
                csv_data = pd.read_csv(csv_path, encoding='utf-8', low_memory=False)
                
                # Add source file information
                csv_data['source_file'] = resource['name']
                csv_data['file_year'] = self.extract_year_from_filename(resource['name'])
                
                #print(f"✅ Loaded {len(csv_data)} records from {resource['name']}")
                #print(f"   Columns: {list(csv_data.columns)}")
                #print(f"   Shape: {csv_data.shape}")
                
                # Convert to list of dictionaries
                file_records = csv_data.to_dict('records')
                all_delay_data.extend(file_records)
                
                #print(f"📈 Total records so far: {len(all_delay_data)}")
                
            except Exception as e:
                #print(f"❌ Error processing CSV {resource['name']}: {e}")
                # Try with different encoding
                try:
                    #print("🔄 Trying with different encoding...")
                    csv_data = pd.read_csv(csv_path, encoding='latin-1', low_memory=False)
                    
                    # Add source file information
                    csv_data['source_file'] = resource['name']
                    csv_data['file_year'] = self.extract_year_from_filename(resource['name'])
                    
                    #print(f"✅ Loaded {len(csv_data)} records from {resource['name']} with latin-1 encoding")
                    
                    # Convert to list of dictionaries
                    file_records = csv_data.to_dict('records')
                    all_delay_data.extend(file_records)
                    
                except Exception as e2:
                    #print(f"❌ Failed to read CSV with alternative encoding: {e2}")
                    continue
        
        #print(f"\n🎯 Successfully processed {len(excel_resources) + len(csv_resources)} files")
        #print(f"📊 Total records merged: {len(all_delay_data)}")
        
        # Save merged raw data
        merged_data_path = os.path.join(self.input_data_folder, "all_delay_data_merged.json")
        with open(merged_data_path, 'w', encoding='utf-8') as f:
            json.dump(all_delay_data, f, indent=2, ensure_ascii=False, default=str)
        
        #print(f"💾 Saved merged raw data to: {merged_data_path}")
        
        return all_delay_data

    def read_excel_file(self, file_path):
        """Read Excel file with multiple engine attempts"""
        engines_to_try = ['openpyxl', 'xlrd']
        
        for engine in engines_to_try:
            try:
                #print(f"   Trying engine: {engine}")
                data = pd.read_excel(file_path, engine=engine)
                #print(f"   ✅ Success with engine: {engine}")
                return data
            except Exception as e:
                #print(f"   ❌ Failed with engine {engine}: {e}")
                continue
        
        # If all engines fail, try without specifying engine
        try:
            #print("   Trying without engine specification...")
            data = pd.read_excel(file_path)
            #print("   ✅ Success without engine specification")
            return data
        except Exception as e:
            #print(f"   ❌ All attempts failed: {e}")
            return None

    def download_gtfs_data(self):
        """Download and extract GTFS data"""
        #print("🗺️ Downloading GTFS Data...")
        
        package_info = self.fetch_package(self.gtfs_package_id)
        #print(f"📦 Package: {package_info['title']}")
        
        # Find the Complete GTFS resource
        gtfs_resource = None
        for resource in package_info['resources']:
            if ('complete gtfs' in resource.get('name', '').lower() or 
                'completegtfs' in resource.get('name', '').lower()):
                gtfs_resource = resource
                break
        
        if not gtfs_resource:
            raise Exception("Complete GTFS resource not found")
        
        #print(f"📥 Downloading GTFS ZIP from: {gtfs_resource['url']}")
        
        # Download GTFS ZIP
        zip_path = os.path.join(self.input_data_folder, "complete_gtfs.zip")
        self.download_file(gtfs_resource['url'], zip_path)
        
        # Extract GTFS files
        #print("🔧 Extracting GTFS files...")
        gtfs_data = self.extract_gtfs_files(zip_path)
        
        return gtfs_data

    def extract_gtfs_files(self, zip_path):
        """Extract required files from GTFS ZIP"""
        gtfs_data = {}
        required_files = ['routes.txt', 'trips.txt', 'shapes.txt', 'stops.txt']
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List files in ZIP
                file_list = zip_ref.namelist()
                #print(f"📁 Files in GTFS ZIP: {len(file_list)}")
                
                # Extract required files
                for filename in required_files:
                    if filename in file_list:
                        # Extract file content
                        with zip_ref.open(filename) as file:
                            content = file.read().decode('utf-8')
                            gtfs_data[filename] = content
                            
                        # Save individual file
                        file_path = os.path.join(self.input_data_folder, filename)
                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        
                        #print(f"✅ Extracted: {filename}")
                    else:
                        print(f"⚠️ Missing: {filename}")
            
            return gtfs_data
            
        except Exception as e:
            #print(f"❌ Error extracting GTFS files: {e}")
            raise

    def safe_min_max(self, series):
        """Safely get min and max values for a series, handling mixed data types"""
        try:
            # Try to convert to numeric first
            numeric_series = pd.to_numeric(series, errors='coerce')
            if not numeric_series.isna().all():
                valid_values = numeric_series.dropna()
                if len(valid_values) > 0:
                    return f"{valid_values.min()} to {valid_values.max()}"
            
            # Try datetime
            datetime_series = pd.to_datetime(series, errors='coerce')
            if not datetime_series.isna().all():
                valid_dates = datetime_series.dropna()
                if len(valid_dates) > 0:
                    return f"{valid_dates.min()} to {valid_dates.max()}"
            
            # For object types, show sample instead
            unique_count = series.nunique()
            sample_values = series.dropna().unique()[:3]
            return f"[Object type - {unique_count} unique values] Sample: {sample_values}"
            
        except Exception as e:
            return f"[Error: {str(e)}]"

    def clean_delay_data(self, delay_data):
        """Clean and convert delay data types with detailed debugging"""
        #print("🧹 Cleaning delay data types...")
        
        df = pd.DataFrame(delay_data)
        
        # NEW: #print the full DataFrame info before processing
        #print("\n" + "="*80)
        #print("📊 FULL DELAY DATA DATAFRAME INFO:")
        #print("="*80)
        #print(f"📈 DataFrame shape: {df.shape}")
        #print(f"📋 Columns: {list(df.columns)}")
        
        #print("\n🔍 Column details:")
        for col in df.columns:
            #print(f"   - {col}: {df[col].dtype}, {df[col].notna().sum()} non-null values")
            if df[col].dtype == 'object':
                sample_values = df[col].dropna().unique()[:3]
                #print(f"     Sample values: {sample_values}")
        
        # Check for date columns and their ranges - USE SAFE METHOD
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        #print(f"\n📅 Date-related columns: {date_columns}")
        
        for date_col in date_columns:
            if date_col in df.columns:
                range_info = self.safe_min_max(df[date_col])
                #print(f"   - {date_col}: {range_info}")
        
        # Check for route/line columns
        route_columns = [col for col in df.columns if 'route' in col.lower() or 'line' in col.lower()]
        #print(f"\n🚍 Route/Line columns: {route_columns}")
        
        for route_col in route_columns:
            if route_col in df.columns:
                print(f"   - {route_col}: {df[route_col].nunique()} unique values")
        
        # Check for delay columns
        delay_columns = [col for col in df.columns if 'delay' in col.lower()]
        #print(f"\n⏱️ Delay columns: {delay_columns}")
        
        for delay_col in delay_columns:
            if delay_col in df.columns:
                range_info = self.safe_min_max(df[delay_col])
                #print(f"   - {delay_col}: {range_info}")
        
        #print("\n" + "="*80)
        #print("🔧 STARTING DATA CLEANING...")
        #print("="*80)

        # Convert numeric columns
        if 'Min Delay' in df.columns:
            df['Min Delay'] = pd.to_numeric(df['Min Delay'], errors='coerce').fillna(0)
            #print(f"✅ Converted Min Delay to numeric: {len(df[df['Min Delay'] > 0])} valid delays")
        
        # Try alternative delay column names
        for delay_col in ['Delay', 'Delay Minutes', 'Delay_Minutes']:
            if delay_col in df.columns and 'Min Delay' not in df.columns:
                df['Min Delay'] = pd.to_numeric(df[delay_col], errors='coerce').fillna(0)
                #print(f"✅ Using '{delay_col}' as Min Delay: {len(df[df['Min Delay'] > 0])} valid delays")
                break
        
        if 'Min Gap' in df.columns:
            df['Min Gap'] = pd.to_numeric(df['Min Gap'], errors='coerce').fillna(0)
        
        if 'Vehicle' in df.columns:
            df['Vehicle'] = pd.to_numeric(df['Vehicle'], errors='coerce').fillna(0)
        
        # Convert date columns - try multiple date column names
        date_column_used = None
        for date_col in ['Date', 'Incident Date', 'Report Date', 'Date & Time']:
            if date_col in df.columns:
                df['Date'] = pd.to_datetime(df[date_col], errors='coerce')
                date_column_used = date_col
                #print(f"✅ Using '{date_col}' as Date column")
                break
        
        if date_column_used:
            date_range_info = self.safe_min_max(df['Date'])
            #print(f"📅 Date range after cleaning: {date_range_info}")
            if df['Date'].notna().any():
                years = df['Date'].dt.year.dropna().unique()
                #print(f"📅 Years in data: {sorted(years)}")
        else:
            #print("⚠️ No date column found")
            # Create a dummy date column if none exists
            df['Date'] = pd.to_datetime('2023-01-01')
        
        # Use 'Line' as Route and 'Station' as Location
        if 'Line' in df.columns:
            df['Route'] = df['Line']
            #print(f"✅ Using 'Line' column as Route: {df['Route'].nunique()} unique routes")
        elif 'Route' not in df.columns:
            # Try to find alternative route columns
            for route_col in ['Route Number', 'Route No', 'Route_ID']:
                if route_col in df.columns:
                    df['Route'] = df[route_col]
                    #print(f"✅ Using '{route_col}' as Route: {df['Route'].nunique()} unique routes")
                    break
        
        if 'Station' in df.columns:
            df['Location'] = df['Station']
            #print(f"✅ Using 'Station' column as Location: {df['Location'].nunique()} unique locations")
        elif 'Location' not in df.columns:
            # Try to find alternative location columns
            for loc_col in ['Stop', 'Stop Name', 'Station Name', 'Location Name']:
                if loc_col in df.columns:
                    df['Location'] = df[loc_col]
                    #print(f"✅ Using '{loc_col}' as Location: {df['Location'].nunique()} unique locations")
                    break
        
        # Clean route names - extract route numbers
        if 'Route' in df.columns:
            df['Route'] = df['Route'].astype(str)
            # Extract route numbers (e.g., "102 MARKHAM ROAD" -> "102")
            df['Route_Number'] = df['Route'].str.extract(r'^(\d+)')
            df['Route'] = df['Route_Number'].fillna(df['Route'])
            #print(f"✅ Extracted route numbers: {df['Route'].nunique()} unique routes")
        
        # NEW: #print final DataFrame info after cleaning
        #print("\n" + "="*80)
        #print("✅ CLEANED DATAFRAME SUMMARY:")
        #print("="*80)
        #print(f"📈 Final shape: {df.shape}")
        #print(f"📋 Final columns: {list(df.columns)}")
        
        if 'Date' in df.columns:
            date_range_info = self.safe_min_max(df['Date'])
            #print(f"📅 Final date range: {date_range_info}")
            if df['Date'].notna().any():
                year_counts = df['Date'].dt.year.value_counts().sort_index()
                #print(f"📅 Records by year:\n{year_counts}")
        
        if 'Route' in df.columns:
            print(f"🚍 Final unique routes: {df['Route'].nunique()}")
        
        if 'Min Delay' in df.columns:
            valid_delays = len(df[df['Min Delay'] > 0])
            #print(f"⏱️ Valid delays (>0 min): {valid_delays}/{len(df)} ({valid_delays/len(df)*100:.1f}%)")
        
        #print("\n" + "="*80)
        
        return df

    def process_route_performance(self, delay_data):
        """Process delay data into route performance metrics"""
        #print("📈 Processing route performance data...")
        
        # Clean and convert data types
        df = self.clean_delay_data(delay_data)
        
        # Check if we have route data
        if 'Route' not in df.columns:
            #print("❌ No 'Route' column found in delay data")
            # Try to find alternative column names
            for col in df.columns:
                if 'route' in col.lower() or 'line' in col.lower():
                    df['Route'] = df[col]
                    #print(f"✅ Using '{col}' as Route column")
                    break
        
        if 'Route' not in df.columns:
            #print("❌ No route data available")
            return []
        
        # Ensure Route column is string
        df['Route'] = df['Route'].astype(str)
        
        # Filter out routes with no valid delays
        df_valid = df[df['Min Delay'] > 0]
        
        if len(df_valid) == 0:
            #print("⚠️ No valid delays found")
            return []
        
        # Group by route and calculate metrics
        route_groups = df_valid.groupby('Route').agg({
            'Min Delay': ['count', 'mean', 'sum'],
            'Vehicle': 'nunique'
        }).round(2)
        
        # Flatten column names
        route_groups.columns = ['Delay_Count', 'Avg_Delay_Min', 'Total_Delay_Min', 'Unique_Vehicles']
        route_groups = route_groups.reset_index()
        
        # NEW: Apply filters - only routes with more than 10 delays and exclude routes 1-4
        #print("🔍 Applying filters: routes with >10 delays and excluding routes 1-4")
        route_groups = route_groups[
            (route_groups['Delay_Count'] > 10) & 
            (~route_groups['Route'].isin(['1', '2', '3', '4']))
        ]
        
        #print(f"📊 After filtering: {len(route_groups)} routes remaining")
        
        # Calculate additional metrics
        total_days = df['Date'].nunique() if 'Date' in df.columns and df['Date'].notna().any() else 30
        route_groups['Delays_Per_Day'] = (route_groups['Delay_Count'] / total_days).round(2)
        route_groups['On_Time_Percentage'] = 0  # Would need schedule data
        
        # Add route names
        route_groups['route_long_name'] = route_groups['Route'].apply(lambda x: f"Route {x}")
        
        # Convert to list of dictionaries
        route_performance = route_groups.to_dict('records')
        
        #print(f"✅ Processed {len(route_performance)} routes (filtered: >10 delays, excluding 1-4)")
        return route_performance

    def process_route_geometries(self, gtfs_data):
        """Process GTFS data into route geometries"""
        #print("🗺️ Processing route geometries...")
        
        route_geometries = {}
        
        try:
            # Parse shapes data
            if 'shapes.txt' in gtfs_data:
                shapes_path = os.path.join(self.input_data_folder, 'shapes.txt')
                trips_path = os.path.join(self.input_data_folder, 'trips.txt')
                routes_path = os.path.join(self.input_data_folder, 'routes.txt')
                
                if (os.path.exists(shapes_path) and 
                    os.path.exists(trips_path) and 
                    os.path.exists(routes_path)):
                    
                    # Read with explicit dtype to avoid mixed type warnings
                    shapes_df = pd.read_csv(shapes_path, dtype={'shape_id': str})
                    trips_df = pd.read_csv(trips_path, dtype={'route_id': str, 'shape_id': str})
                    routes_df = pd.read_csv(routes_path, dtype={'route_id': str})
                    
                    #print(f"📊 Shapes: {len(shapes_df)}, Trips: {len(trips_df)}, Routes: {len(routes_df)}")
                    
                    # Group shapes by shape_id
                    shapes_by_route = {}
                    for shape_id, group in shapes_df.groupby('shape_id'):
                        # Sort by sequence and get coordinates
                        coords = group.sort_values('shape_pt_sequence')[['shape_pt_lat', 'shape_pt_lon']].values.tolist()
                        shapes_by_route[shape_id] = coords
                    
                    # Map routes to shapes via trips
                    route_to_shape = {}
                    for _, trip in trips_df.iterrows():
                        if pd.notna(trip['route_id']) and pd.notna(trip['shape_id']):
                            route_to_shape[trip['route_id']] = trip['shape_id']
                    
                    # Create geometries for each route
                    for route_id, shape_id in route_to_shape.items():
                        if shape_id in shapes_by_route:
                            coordinates = []
                            for lat, lon in shapes_by_route[shape_id]:
                                if (isinstance(lat, (int, float)) and isinstance(lon, (int, float)) and
                                    -90 <= lat <= 90 and -180 <= lon <= 180):
                                    coordinates.append([float(lat), float(lon)])
                            
                            if coordinates:
                                route_geometries[str(route_id)] = coordinates
                    
                    #print(f"✅ Processed {len(route_geometries)} route geometries from GTFS")
                else:
                    #print("⚠️ GTFS files not found, generating sample geometries")
                    self.create_sample_geometries(route_geometries)
            else:
                #print("⚠️ No shapes.txt found, generating sample geometries")
                self.create_sample_geometries(route_geometries)
                
        except Exception as e:
            #print(f"⚠️ Error processing GTFS geometries: {e}")
            #print("🔄 Generating sample geometries instead")
            self.create_sample_geometries(route_geometries)
        
        return route_geometries

    def create_sample_geometries(self, route_geometries):
        """Create sample geometries when GTFS data is not available"""
        toronto_center = [43.6532, -79.3832]
        # Only include routes that would pass our filters (not 1-4)
        routes = ['501', '504', '505', '506', '509', '510', '511', '512', '96', '165', '102', '35']
        
        for i, route in enumerate(routes):
            coordinates = []
            point_count = 8 + i
            
            for j in range(point_count):
                angle = (j / point_count) * 3.14  # Semi-circle
                lat = toronto_center[0] + (0.01 * i) + (0.005 * math.cos(angle))
                lng = toronto_center[1] + (0.01 * i) + (0.005 * math.sin(angle))
                coordinates.append([round(lat, 6), round(lng, 6)])
            
            route_geometries[route] = coordinates
        
        #print(f"✅ Generated {len(route_geometries)} sample route geometries")

    def process_location_analysis(self, delay_data):
        """Process delay data into location analysis"""
        #print("📍 Processing location analysis...")
        
        df = self.clean_delay_data(delay_data)
        
        # Check if we have location data
        if 'Location' not in df.columns:
            #print("❌ No 'Location' column found in delay data")
            # Try to find alternative column names
            for col in df.columns:
                if 'location' in col.lower() or 'station' in col.lower() or 'stop' in col.lower():
                    df['Location'] = df[col]
                    #print(f"✅ Using '{col}' as Location column")
                    break
        
        if 'Location' not in df.columns:
            #print("❌ No location data available")
            return []
        
        # Filter out records without location
        df_with_location = df[df['Location'].notna() & (df['Location'] != '') & (df['Location'] != 'Unknown')]
        
        if len(df_with_location) == 0:
            #print("⚠️ No location data found")
            return []
        
        # Filter only records with valid delays
        df_valid = df_with_location[df_with_location['Min Delay'] > 0]
        
        if len(df_valid) == 0:
            #print("⚠️ No valid delays at locations found")
            return []
        
        # Group by location
        location_groups = df_valid.groupby('Location').agg({
            'Min Delay': ['count', 'mean'],
            'Route': 'nunique',
            'Vehicle': 'nunique'
        }).round(2)
        
        # Flatten columns
        location_groups.columns = ['total_delays', 'avg_delay_min', 'route_count', 'vehicle_count']
        location_groups = location_groups.reset_index()
        
        # Convert to list of dictionaries
        location_analysis = []
        for _, row in location_groups.iterrows():
            location_analysis.append({
                'location_id': self.sanitize_location_id(row['Location']),
                'location_name': row['Location'],
                'total_delays': int(row['total_delays']),
                'avg_delay_min': float(row['avg_delay_min']),
                'latitude': self.generate_toronto_lat(),
                'longitude': self.generate_toronto_lng(),
                'route_count': int(row['route_count']),
                'vehicle_count': int(row['vehicle_count']),
                'peak_hours': json.dumps(['07:00-09:00', '16:00-18:00'])
            })
        
        # Sort by total delays
        location_analysis.sort(key=lambda x: x['total_delays'], reverse=True)
        
        #print(f"✅ Processed {len(location_analysis)} locations")
        return location_analysis

    def sanitize_location_id(self, location_name):
        """Create a sanitized location ID"""
        return (location_name.lower()
                .replace(' ', '_')
                .replace('/', '_')
                .replace('\\', '_')
                .replace('&', 'and')
                .replace("'", '')
                .replace('"', '')
                .replace('(', '')
                .replace(')', '')
                .replace(',', '')[:50])

    def generate_toronto_lat(self):
        """Generate random Toronto latitude"""
        return round(43.65 + (random.random() - 0.5) * 0.1, 6)

    def generate_toronto_lng(self):
        """Generate random Toronto longitude"""
        return round(-79.38 + (random.random() - 0.5) * 0.1, 6)

    def process_summary_statistics(self, delay_data, route_performance, location_analysis):
        """Calculate summary statistics"""
        #print("📊 Processing summary statistics...")
        
        df = self.clean_delay_data(delay_data)
        
        total_delays = len(delay_data)
        
        # Count valid delays (Min Delay > 0)
        valid_delays = len(df[df['Min Delay'] > 0])
        avg_delay = df[df['Min Delay'] > 0]['Min Delay'].mean() if valid_delays > 0 else 0
        
        # Count unique routes and vehicles
        unique_routes = df['Route'].nunique() if 'Route' in df.columns else 0
        unique_vehicles = df['Vehicle'].nunique() if 'Vehicle' in df.columns else 0
        unique_locations = df['Location'].nunique() if 'Location' in df.columns else 0
        
        # Calculate date range for delays
        oldest_date = None
        most_recent_date = None
        if 'Date' in df.columns and df['Date'].notna().any():
            oldest_date = df['Date'].min()
            most_recent_date = df['Date'].max()
            #print(f"📅 Date range found: {oldest_date} to {most_recent_date}")
        
        # Calculate coverage percentage based on filtered routes vs total routes
        total_routes_in_data = df['Route'].nunique() if 'Route' in df.columns else 0
        displayed_routes = len(route_performance)
        
        if total_routes_in_data > 0:
            coverage_percentage = round((displayed_routes / total_routes_in_data) * 100, 1)
        else:
            coverage_percentage = 0
        
        #print(f"📈 Coverage calculation: {displayed_routes} displayed / {total_routes_in_data} total = {coverage_percentage}%")
        
        # Find most delayed route
        most_delayed_route = None
        if route_performance:
            most_delayed_route = max(route_performance, key=lambda x: x['Avg_Delay_Min'])
        
        # NEW: Calculate data period from actual data
        data_period = "Unknown"
        if oldest_date and most_recent_date:
            oldest_year = oldest_date.year
            most_recent_year = most_recent_date.year
            if oldest_year == most_recent_year:
                data_period = str(most_recent_year)
            else:
                data_period = f"{oldest_year}-{most_recent_year}"
        
        stats = {
            'total_delays': total_delays,
            'valid_delays': valid_delays,
            'avg_delay_minutes': round(avg_delay, 2),
            'unique_routes': unique_routes,
            'unique_vehicles': unique_vehicles,
            'unique_locations': unique_locations,
            'data_points': total_delays,
            'coverage_percentage': coverage_percentage,
            'time_period': data_period,
            'updated_at': datetime.now().isoformat(),
            'data_refresh_date': datetime.now().strftime('%Y-%m-%d'),
            'data_oldest_date': oldest_date.isoformat() if oldest_date else None,
            'data_most_recent_date': most_recent_date.isoformat() if most_recent_date else None,
            'data_update_date': datetime.now().strftime('%Y-%m-%d'),
            'peak_delay_hour': self.calculate_peak_hour(df),
            'most_delayed_route': f"{most_delayed_route['Route']} - {most_delayed_route['route_long_name']}" if most_delayed_route else 'Unknown',
            'displayed_routes_count': displayed_routes,
            'total_routes_count': total_routes_in_data,
            'data_quality': {
                'valid_delay_percentage': round((valid_delays / total_delays * 100), 2) if total_delays > 0 else 0,
                'route_coverage': unique_routes,
                'location_coverage': unique_locations,
                'date_range_available': oldest_date is not None and most_recent_date is not None
            }
        }
        
        #print("✅ Summary statistics calculated")
        #print(f"   - Data Period: {data_period}")
        #print(f"   - Coverage: {coverage_percentage}% ({displayed_routes}/{total_routes_in_data} routes)")
        #print(f"   - Date Range: {oldest_date} to {most_recent_date}" if oldest_date else "   - No date range available")
        
        return stats

    def calculate_peak_hour(self, df):
        """Calculate peak delay hour from data"""
        try:
            if 'Time' in df.columns:
                # Extract hour from time objects or strings
                time_series = df['Time'].dropna()
                if len(time_series) > 0:
                    # Convert to string first, then extract hour
                    time_strings = time_series.astype(str)
                    # Parse hours from various time formats
                    hours = []
                    for time_str in time_strings:
                        try:
                            if ':' in time_str:
                                hour_part = time_str.split(':')[0]
                                hour = int(hour_part)
                                hours.append(hour)
                        except:
                            continue
                    
                    if hours:
                        hour_series = pd.Series(hours)
                        peak_hour = int(hour_series.mode().iloc[0]) if not hour_series.mode().empty else 8
                        return f"{peak_hour:02d}:00"
        except Exception as e:
            print(f"⚠️ Error calculating peak hour: {e}")
        
        return "08:00"  # Fallback

    def save_processed_data(self, route_performance, route_geometries, location_analysis, summary_stats):
        """Save all processed data to output folder"""
        print("💾 Saving processed data...")
        
        # Save route performance as CSV
        route_performance_path = os.path.join(self.output_data_folder, "route_performance.csv")
        with open(route_performance_path, 'w', newline='', encoding='utf-8') as f:
            if route_performance:
                writer = csv.DictWriter(f, fieldnames=route_performance[0].keys())
                writer.writeheader()
                writer.writerows(route_performance)
        print(f"✅ Saved route_performance.csv ({len(route_performance)} routes)")
        
        # Save route geometries as JSON
        route_geometries_path = os.path.join(self.output_data_folder, "route_geometries.json")
        with open(route_geometries_path, 'w', encoding='utf-8') as f:
            json.dump(route_geometries, f, indent=2)
        print(f"✅ Saved route_geometries.json ({len(route_geometries)} routes)")
        
        # Save location analysis as CSV
        location_analysis_path = os.path.join(self.output_data_folder, "location_analysis.csv")
        with open(location_analysis_path, 'w', newline='', encoding='utf-8') as f:
            if location_analysis:
                writer = csv.DictWriter(f, fieldnames=location_analysis[0].keys())
                writer.writeheader()
                writer.writerows(location_analysis)
        print(f"✅ Saved location_analysis.csv ({len(location_analysis)} locations)")
        
        # Save summary statistics as JSON
        summary_stats_path = os.path.join(self.output_data_folder, "summary_statistics.json")
        with open(summary_stats_path, 'w', encoding='utf-8') as f:
            json.dump(summary_stats, f, indent=2, default=str)
        print("✅ Saved summary_statistics.json")
    


    def should_update_data(self):
        """Check if data needs to be updated (older than 1 hour)"""
        stats_file = os.path.join(self.output_data_folder, "summary_statistics.json")
        
        if not os.path.exists(stats_file):
            return True
        
        try:
            with open(stats_file, 'r', encoding='utf-8') as f:
                stats = json.load(f)
            
            if 'updated_at' in stats:
                last_updated = datetime.fromisoformat(stats['updated_at'].replace('Z', '+00:00'))
                one_hour_ago = datetime.now() - timedelta(hours=1)
                return last_updated < one_hour_ago
        except:
            pass
        
        return True

    def transform_data(self):
        """Main transformation function"""
        #print("🔄 Starting TTC Data Transformation...")
        #print("=" * 50)
        
        try:
            # Check if update is needed
            # if not self.should_update_data():
            #     #print("📊 Data is recent (less than 1 hour old), skipping update")
            #     return True
            
            # Step 1: Download raw data
            #print("\n📥 Downloading raw data...")
            delay_data = self.download_delay_data()
            gtfs_data = self.download_gtfs_data()
            
            #print("\n✅ Raw data downloaded successfully")
            #print("=" * 50)
            
            # Step 2: Process data
            #print("\n🔧 Processing data...")
            route_performance = self.process_route_performance(delay_data)
            route_geometries = self.process_route_geometries(gtfs_data)
            location_analysis = self.process_location_analysis(delay_data)
            summary_stats = self.process_summary_statistics(delay_data, route_performance, location_analysis)
            
            #print("\n✅ Data processing completed")
            #print("=" * 50)
            
            # Step 3: Save processed data
            #print("\n💾 Saving processed data...")
            self.save_processed_data(route_performance, route_geometries, location_analysis, summary_stats)
            
            #print("\n🎉 Transformation completed successfully!")
            #print("=" * 50)
            #print("📊 Summary:")
            #print(f"   - Routes: {len(route_performance)} (filtered: >10 delays, excluding 1-4)")
            #print(f"   - Geometries: {len(route_geometries)}")
            #print(f"   - Locations: {len(location_analysis)}")
            #print(f"   - Total Delays: {summary_stats['total_delays']}")
            #print(f"   - Valid Delays: {summary_stats['valid_delays']}")
            #print(f"   - Average Delay: {summary_stats['avg_delay_minutes']} minutes")
            #print(f"   - Coverage: {summary_stats['coverage_percentage']}%")
            #print(f"   - Data Period: {summary_stats['time_period']}")
            #print(f"   - Date Range: {summary_stats.get('data_oldest_date', 'N/A')} to {summary_stats.get('data_most_recent_date', 'N/A')}")
            #print(f"\n📁 Output folder: {self.output_data_folder}")
            
            return True
            
        except Exception as e:
            #print(f"\n💥 Transformation failed: {e}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    transformer = TTCDataTransformer()
    success = transformer.transform_data()
    
    if success:
        print("\n✨ Data update completed successfully!")
    else:
        #print("\n❌ Data update failed!")
        exit(1)