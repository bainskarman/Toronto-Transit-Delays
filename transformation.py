import requests
import pandas as pd
import json
import zipfile
import re
import os
from io import BytesIO, StringIO
from datetime import datetime
import random
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional



def clean_and_standardize(df):
    """Standardize column names and enforce a uniform schema."""
    df = df.copy()
    df.columns = df.columns.str.strip()

    column_mappings = {
        'Date': ['Report Date', 'Date', 'Incident Date', 'Date & Time'],
        'Time': ['Time', 'Incident Time'],
        'Day': ['Day'],
        'Location': ['Location', 'Station', 'Station Name', 'Stop', 'Stop Name'],
        'Incident': ['Incident', 'Code', 'Description'],
        'Min Delay': ['Min Delay', 'Delay', 'Delay Minutes', 'Delay_Minutes'],
        'Min Gap': ['Min Gap', 'Gap', 'Gap Minutes', 'Gap_Minutes'],
        'Route': ['Route', 'Route Number', 'Route No', 'Route_ID'],
        'Line': ['Line'],
        'Direction': ['Direction', 'Bound'],
        'Vehicle': ['Vehicle', 'Vehicle Number', 'Vehicle_No']
    }

    reverse_mapping = {}
    for std_name, possible in column_mappings.items():
        for name in possible:
            reverse_mapping[name] = std_name

    rename_dict = {col: reverse_mapping[col] for col in df.columns if col in reverse_mapping}
    df = df.rename(columns=rename_dict)


    if 'Line' in df.columns and 'Route' not in df.columns:
        def extract_route_info(line_val):
            if pd.isna(line_val):
                return pd.Series([None, None])
            line_str = str(line_val).strip()
            match = re.match(r'^(\d+)(?:\s+(.+))?$', line_str)
            if match:
                return pd.Series([match.group(1), match.group(2) if match.group(2) else ''])
            match_digits = re.match(r'^(\d+)$', line_str)
            if match_digits:
                return pd.Series([match_digits.group(1), ''])
            return pd.Series([line_str, ''])
        df[['Route', 'Route Name']] = df['Line'].apply(extract_route_info)

    if 'Route' in df.columns and 'Route Name' not in df.columns:
        df['Route Name'] = ''

    required_columns = [
        'Date', 'Route', 'Route Name', 'Time', 'Day', 'Location',
        'Incident', 'Min Delay', 'Min Gap', 'Direction', 'Vehicle',
        'Incident_Original'
    ]
    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    df = df[required_columns]
    return df


def _get_resource_format(res):
    """Return 'csv', 'xlsx', 'xls', or 'unknown' for a resource dict."""
    if res.get("datastore_active"):
        return "csv"
    fmt = res.get("format", "").lower()
    if fmt in ("csv", "xlsx", "xls"):
        return fmt
    # try to guess from URL extension
    url = res.get("url", "")
    if url.endswith(".csv"):
        return "csv"
    elif url.endswith((".xlsx", ".xls")):
        return "xlsx"
    return "unknown"



def load_mode_delay_data(mode_name, package_id, extra_csv=None,
                         force_csv_only=False, skip_year_filter=False):
    """Downloads all resources for a given transit mode, cleans and returns a DataFrame."""
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    current_year = datetime.now().year
    

    package_url = f"{base_url}/api/3/action/package_show"
    resp = requests.get(package_url, params={"id": package_id})
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise Exception(f"CKAN API request failed for {mode_name}")

    resources = data["result"]["resources"]
    print(f"📦 Found {len(resources)} resource(s)")

    # --- NEW: pre‑select best resource per year when year filtering is enabled ---
    # This block replaces the old in‑loop year/format filter for Bus/Streetcar/Subway.
    if not skip_year_filter:
        year_pattern = re.compile(r"(19|20)\d{2}")
        from collections import defaultdict
        year_groups = defaultdict(list)

        for res in resources:
            name = res.get("name", "")
            ym = year_pattern.search(name)
            if not ym:
                continue
            year = int(ym.group(0))
            if year < 2014:
                continue
            fmt = _get_resource_format(res)
            # respect force_csv_only
            if force_csv_only and fmt != "csv":
                continue
            year_groups[year].append((res, fmt))

        selected_resources = []
        for year, res_list in year_groups.items():
            if year <= 2024:
                # hard‑code: only XLSX/XLS
                chosen = next((r for r, f in res_list if f in ("xlsx", "xls")), None)
            else:  # year >= 2025
                # prefer CSV, otherwise XLSX/XLS
                chosen = next((r for r, f in res_list if f == "csv"), None)
                if not chosen:
                    chosen = next((r for r, f in res_list if f in ("xlsx", "xls")), None)
            if chosen:
                selected_resources.append(chosen)

        resources = selected_resources
        print(f"🎯 After year‑based selection: {len(resources)} resource(s) to download")
        
        use_best_selection = True
    else:
        use_best_selection = False
    # -------------------------------------------------------------------------

    year_pattern = re.compile(r"(19|20)\d{2}")  # still needed for logging inside the loop
    allowed_formats = {"csv"} if force_csv_only else {"csv", "xlsx", "xls"}

    mode_dfs = []

    for res in resources:
        res_name = res.get("name", "Unnamed")
        datastore_active = res.get("datastore_active", False)

        # --- Extract year for logging (not for filtering when we pre‑selected) ---
        year = None
        if not skip_year_filter:
            year_match = year_pattern.search(res_name)
            year = int(year_match.group(0)) if year_match else None

        # Determine format
        if datastore_active:
            fmt = "csv"
        else:
            fmt = res.get("format", "").lower()
            if not fmt:
                url = res.get("url", "")
                if url.endswith(".csv"):
                    fmt = "csv"
                elif url.endswith((".xlsx", ".xls")):
                    fmt = "xlsx"
                else:
                    fmt = "unknown"

        # ---- Original in‑loop filtering: only used when we did NOT pre‑select ----
        if not skip_year_filter and not use_best_selection:
            # skip if no year or too old
            if year is None or year < 2014:
                print(f"  ⏭️  {res_name}: no year or <2014, skipping")
                continue
            # apply the old format‑based year filter
            if not force_csv_only:
                if year >= current_year - 1 and fmt != "csv":
                    print(f"  ⏭️  {res_name}: skipping XLSX for latest year, only CSV kept")
                    continue
                elif year < current_year - 1 and fmt not in ("xlsx", "xls"):
                    print(f"  ⏭️  {res_name}: skipping CSV for year < {current_year}, only XLSX kept")
                    continue
        

        # Sanity check: do not download unknown formats
        if fmt not in allowed_formats:
            print(f"  ⏭️  {res_name}: format '{fmt}' not in {allowed_formats}, skipping")
            continue

        if year:
            print(f"  📄 Resource: {res_name} (year={year}, format={fmt}, datastore_active={datastore_active})")
        else:
            print(f"  📄 Resource: {res_name} (format={fmt}, datastore_active={datastore_active})")

        try:
            if datastore_active:
                dump_url = f"{base_url}/datastore/dump/{res['id']}"
                dump_resp = requests.get(dump_url)
                dump_resp.raise_for_status()
                df = pd.read_csv(StringIO(dump_resp.text))
                if "Incident" in df.columns:
                    df["Incident_Original"] = df["Incident"]
            else:
                file_url = res["url"]
                file_resp = requests.get(file_url)
                file_resp.raise_for_status()

                if fmt in ("xlsx", "xls"):
                    excel_data = pd.read_excel(BytesIO(file_resp.content), sheet_name=None)
                    sheet_dfs = []
                    for sheet_name, sheet_df in excel_data.items():
                        if not sheet_df.empty:
                            if "Incident" in sheet_df.columns:
                                sheet_df["Incident_Original"] = sheet_df["Incident"]
                            sheet_df = clean_and_standardize(sheet_df)
                            sheet_dfs.append(sheet_df)
                    if sheet_dfs:
                        df = pd.concat(sheet_dfs, ignore_index=True)
                    else:
                        print(f"    ⚠️ No data in any sheet, skipping")
                        continue
                else:  # CSV
                    df = pd.read_csv(StringIO(file_resp.text))
                    if "Incident" in df.columns:
                        df["Incident_Original"] = df["Incident"]

            # Standardize columns
            df = clean_and_standardize(df)
            df["Transit"] = mode_name
            df["Source File"] = res_name   # <-- Add this line
            mode_dfs.append(df)
            print(f"    ✅ Loaded {len(df)} records")

        except Exception as e:
            print(f"    ❌ Error: {e}")
            continue

    # Extra local file (unchanged)
    if extra_csv and os.path.isfile(extra_csv):
        print(f"\n  📄 Extra local file: {extra_csv}")
        try:
            df_extra = pd.read_csv(extra_csv)
            if "Incident" in df_extra.columns:
                df_extra["Incident_Original"] = df_extra["Incident"]
            df_extra = clean_and_standardize(df_extra)
            df_extra["Transit"] = mode_name
            mode_dfs.append(df_extra)
            print(f"    ✅ Loaded {len(df_extra)} records from extra file")
        except Exception as e:
            print(f"    ❌ Error reading extra file {extra_csv}: {e}")
    elif extra_csv:
        print(f"\n  ⏭️ Extra file {extra_csv} not found, skipping")

    if not mode_dfs:
        print(f"⚠️ No valid data loaded for {mode_name}")
        return pd.DataFrame()

    mode_combined = pd.concat(mode_dfs, ignore_index=True)
    print(f"✅ {mode_name} total records: {len(mode_combined)}")
    return mode_combined



def get_clean_ttc_delays():

    modes = [
        ("Bus", "ttc-bus-delay-data"),
        ("Streetcar", "ttc-streetcar-delay-data"),
        ("Subway", "ttc-subway-delay-data"),
        ("LRT", "ttc-lrt-delay-data")
    ]

    all_dfs = []
    for mode_name, pkg_id in modes:
        # For LRT, download all resources without year filtering (as in original)
        if mode_name == "LRT":
            df_mode = load_mode_delay_data(mode_name, pkg_id,
                                           extra_csv="TTC LRT Delays.csv",
                                           force_csv_only=False,
                                           skip_year_filter=True)
        else:
            df_mode = load_mode_delay_data(mode_name, pkg_id)
        if not df_mode.empty:
            all_dfs.append(df_mode)

    if not all_dfs:
        raise Exception("No data loaded for any mode.")

    merged_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\n📊 Total merged records (before filtering): {len(merged_df)}")

    # Drop duplicates
    initial_len = len(merged_df)
    merged_df = merged_df.drop_duplicates()
    print(f"🧹 Removed {initial_len - len(merged_df)} duplicate rows")


    cols_to_keep = [
                    'Date',
                    'Time',
                    'Day',
                    'Location',
                    'Incident',
                    'Min Delay',
                    'Min Gap',
                    'Route',
                    'Direction',
                    'Vehicle',
                    'Transit',
                  #  'Source File'
                ]
    # Ensure only columns that actually exist are selected (some may be missing in some datasets)
    cols_to_keep = [col for col in cols_to_keep if col in merged_df.columns]
    merged_df = merged_df[cols_to_keep]

    # Filter rows: Route, Date, Min Delay not null, and Min Delay > 0
    before_filter = len(merged_df)
    merged_df = merged_df.dropna(subset=['Route', 'Date', 'Min Delay'])
    merged_df = merged_df[merged_df['Min Delay'] > 0]
    print(f"🔍 Kept {len(merged_df)} rows after filtering (removed {before_filter - len(merged_df)} rows with null Route/Date/MinDelay or MinDelay <= 0)")

    return merged_df

if __name__ == "__main__":
    df = get_clean_ttc_delays()
    print("\n👀 First few rows (original columns + Transit):")
    print(df.head())
    print("\n📋 Columns returned:", list(df.columns))


df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

# 2. Extract useful components from the date
df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month
df['Weekday'] = df['Date'].dt.day_name()

# 3. Extract hour from the 'Time' column (handles "HH:MM" and "HH:MM:SS")
def extract_hour(time_val):
    if pd.isna(time_val):
        return None
    try:
        # Split by ':' and take the first part (hour)
        hour = int(str(time_val).split(':')[0])
        return hour
    except (ValueError, AttributeError):
        return None

df['Hour'] = df['Time'].apply(extract_hour)

# 4. Convert Location column to all uppercase
df['Location'] = df['Location'].str.upper()



# ----------------------------------------------------------------------
# Define the incident code mapping (from the original pipeline)
# ----------------------------------------------------------------------
def _build_code_mapping():
    """Build dictionary mapping raw incident codes to standard categories."""
    mapping = {}

    # ---- Subway codes (EU, MU, PU, SU, TU) ----
    # Equipment / Mechanical (EU)
    eu_mech = [
        'EUAC', 'EUAL', 'EUATC', 'EUBK', 'EUBO', 'EUCA', 'EUCH', 'EUCO',
        'EUDO', 'EUECD', 'EUHV', 'EULT', 'EULV', 'EUNEA', 'EUNT', 'EUO',
        'EUPI', 'EUSC', 'EUTL', 'EUTM', 'EUTR', 'EUTRD', 'EUVA', 'EUVE', 'EUYRD'
    ]
    for code in eu_mech:
        mapping[code] = 'Equipment / Mechanical'

    mapping['EUCD'] = 'General Delay / Other'          # Consequential Delay
    mapping['EUME'] = 'Operations / Human Error'       # Maintenance Error
    mapping['EUOE'] = 'Operations / Human Error'       # Rail Cars & Shops Opr. Error
    mapping['EUOPO'] = 'Infrastructure / Track / Signals'

    # Miscellaneous (MU)
    mapping['MUD']   = 'Passenger / Security'
    mapping['MUDD']  = 'External / Environment'
    mapping['MUEC']  = 'Infrastructure / Track / Signals'
    mapping['MUESA'] = 'Operations / Human Error'
    mapping['MUFM']  = 'External / Environment'
    mapping['MUFS']  = 'External / Environment'
    mapping['MUGD']  = 'General Delay / Other'
    mapping['MUI']   = 'Passenger / Security'
    mapping['MUIE']  = 'Passenger / Security'
    mapping['MUIR']  = 'Passenger / Security'
    mapping['MUIRS'] = 'Passenger / Security'
    mapping['MUIS']  = 'Passenger / Security'
    mapping['MULD']  = 'Management / Administrative'
    mapping['MUNOA'] = 'Operations / Human Error'
    mapping['MUO']   = 'General Delay / Other'
    mapping['MUODC'] = 'Infrastructure / Track / Signals'
    mapping['MUPAA'] = 'Passenger / Security'
    mapping['MUPLA'] = 'External / Environment'
    mapping['MUPLB'] = 'External / Environment'
    mapping['MUPLC'] = 'External / Environment'
    mapping['MUPR1'] = 'Passenger / Security'
    mapping['MUSAN'] = 'Cleaning / Unsanitary'
    mapping['MUSC']  = 'Equipment / Mechanical'
    mapping['MUTD']  = 'Management / Administrative'
    mapping['MUTO']  = 'General Delay / Other'
    mapping['MUWEA'] = 'External / Environment'
    mapping['MUWR']  = 'Management / Administrative'

    # Infrastructure (PU)
    pu_infra = [
        'PUATC', 'PUCBI', 'PUCSC', 'PUCSS', 'PUDCS', 'PUMEL', 'PUMO',
        'PUOPO', 'PUSAC', 'PUSBE', 'PUSCA', 'PUSCR', 'PUSEA', 'PUSI',
        'PUSIO', 'PUSIS', 'PUSLC', 'PUSO', 'PUSRA', 'PUSSW', 'PUSTC',
        'PUSTP', 'PUSTS', 'PUSWZ', 'PUSZC', 'PUTCD', 'PUTD', 'PUTIJ',
        'PUTNT', 'PUTO', 'PUTOE', 'PUTR', 'PUTS', 'PUTSC', 'PUTSM',
        'PUTTC', 'PUTTP', 'PUTWZ'
    ]
    for code in pu_infra:
        mapping[code] = 'Infrastructure / Track / Signals'

    mapping['PUMST'] = 'Passenger / Security'
    mapping['PUTDN'] = 'External / Environment'
    mapping['PUTIS'] = 'External / Environment'
    mapping['PUSNT'] = 'General Delay / Other'

    # Security (SU)
    su_sec = [
        'SUAE', 'SUAP', 'SUBT', 'SUCOL', 'SUDP', 'SUEAS', 'SUG',
        'SUO', 'SUPOL', 'SUROB', 'SUSA', 'SUSP', 'SUUT'
    ]
    for code in su_sec:
        mapping[code] = 'Passenger / Security'

    # Transportation (TU)
    mapping['TUATC'] = 'Operations / Human Error'
    mapping['TUCC']  = 'Operations / Human Error'
    mapping['TUDOE'] = 'Operations / Human Error'
    mapping['TUKEY'] = 'Operations / Human Error'
    mapping['TUML']  = 'Scheduling / Late Starts'
    mapping['TUMVS'] = 'Operations / Human Error'
    mapping['TUNIP'] = 'Operations / Human Error'
    mapping['TUNOA'] = 'Operations / Human Error'
    mapping['TUO']   = 'General Delay / Other'
    mapping['TUOPO'] = 'Operations / Human Error'
    mapping['TUOS']  = 'Operations / Human Error'
    mapping['TUS']   = 'Scheduling / Late Starts'
    mapping['TUSC']  = 'Operations / Human Error'
    mapping['TUSET'] = 'Operations / Human Error'
    mapping['TUST']  = 'External / Environment'
    mapping['TUSUP'] = 'Operations / Human Error'

    # ---- Streetcar codes (ER, MR, PR, SR, TR) ----
    # Equipment / Mechanical (ER)
    er_mech = [
        'ERAC', 'ERBO', 'ERCO', 'ERDB', 'ERDO', 'ERHV', 'ERLT', 'ERLV',
        'ERNEA', 'ERNT', 'ERO', 'ERPR', 'ERRA', 'ERTB', 'ERTC', 'ERTL',
        'ERTR', 'ERVE', 'ERWA', 'ERWS'
    ]
    for code in er_mech:
        mapping[code] = 'Equipment / Mechanical'
    mapping['ERCD'] = 'General Delay / Other'
    mapping['ERME'] = 'Operations / Human Error'

    # Miscellaneous (MR)
    mapping['MRCL']  = 'Management / Administrative'
    mapping['MRD']   = 'Passenger / Security'
    mapping['MRDD']  = 'External / Environment'
    mapping['MREC']  = 'Infrastructure / Track / Signals'
    mapping['MRESA'] = 'Operations / Human Error'
    mapping['MRFS']  = 'External / Environment'
    mapping['MRIE']  = 'Passenger / Security'
    mapping['MRLD']  = 'Management / Administrative'
    mapping['MRNOA'] = 'Operations / Human Error'
    mapping['MRO']   = 'General Delay / Other'
    mapping['MRPAA'] = 'Passenger / Security'
    mapping['MRPLA'] = 'External / Environment'
    mapping['MRPLB'] = 'External / Environment'
    mapping['MRPLC'] = 'External / Environment'
    mapping['MRPR1'] = 'Passenger / Security'
    mapping['MRSAN'] = 'Cleaning / Unsanitary'
    mapping['MRSTM'] = 'Infrastructure / Track / Signals'
    mapping['MRTO']  = 'General Delay / Other'
    mapping['MRUI']  = 'Passenger / Security'
    mapping['MRUIR'] = 'Passenger / Security'
    mapping['MRWEA'] = 'External / Environment'

    # Infrastructure (PR)
    mapping['PREL']  = 'Infrastructure / Track / Signals'
    mapping['PRO']   = 'General Delay / Other'
    mapping['PRS']   = 'Infrastructure / Track / Signals'
    mapping['PRSA']  = 'Infrastructure / Track / Signals'
    mapping['PRSL']  = 'Infrastructure / Track / Signals'
    mapping['PRSO']  = 'Infrastructure / Track / Signals'
    mapping['PRSP']  = 'Infrastructure / Track / Signals'
    mapping['PRST']  = 'Passenger / Security'
    mapping['PRSW']  = 'Infrastructure / Track / Signals'
    mapping['PRTST'] = 'Infrastructure / Track / Signals'
    mapping['PRW']   = 'Infrastructure / Track / Signals'

    # Security (SR)
    sr_sec = [
        'SRAE', 'SRAP', 'SRBT', 'SRCOL', 'SRDP', 'SREAS', 'SRO',
        'SRSA', 'SRSP', 'SRUT'
    ]
    for code in sr_sec:
        mapping[code] = 'Passenger / Security'

    # Transportation (TR)
    mapping['TRDOE'] = 'Operations / Human Error'
    mapping['TRNIP'] = 'Operations / Human Error'
    mapping['TRNOA'] = 'Operations / Human Error'
    mapping['TRO']   = 'General Delay / Other'
    mapping['TRSET'] = 'Operations / Human Error'
    mapping['TRST']  = 'External / Environment'
    mapping['TRTC']  = 'Operations / Human Error'

    # ---- LRT codes (EX, MX, PX, SX, TX) ----
    # Equipment / Mechanical (EX)
    ex_mech = [
        'EXAC', 'EXBK', 'EXBO', 'EXCB', 'EXCE', 'EXCO', 'EXDB', 'EXDO',
        'EXECD', 'EXGA', 'EXGF', 'EXHV', 'EXLT', 'EXNEA', 'EXNT', 'EXO',
        'EXOSC', 'EXSA', 'EXSE', 'EXTB', 'EXTM', 'EXTR', 'EXVC', 'EXVE',
        'EXWA', 'EXWM', 'EXWS', 'EXYRD'
    ]
    for code in ex_mech:
        mapping[code] = 'Equipment / Mechanical'
    mapping['EXADD'] = 'Equipment / Mechanical'
    mapping['EXOE']  = 'Operations / Human Error'
    mapping['EXPD']  = 'Collision / Roadblock'
    mapping['EXPI']  = 'Collision / Roadblock'

    # Miscellaneous (MX)
    mapping['MXAFR'] = 'Infrastructure / Track / Signals'
    mapping['MXCL']  = 'Management / Administrative'
    mapping['MXCSA'] = 'Operations / Human Error'
    mapping['MXD']   = 'Passenger / Security'
    mapping['MXDD']  = 'External / Environment'
    mapping['MXESA'] = 'Operations / Human Error'
    mapping['MXFM']  = 'External / Environment'
    mapping['MXFS']  = 'External / Environment'
    mapping['MXGD']  = 'General Delay / Other'
    mapping['MXI']   = 'Passenger / Security'
    mapping['MXIC']  = 'Passenger / Security'
    mapping['MXIE']  = 'Passenger / Security'
    mapping['MXIR']  = 'Passenger / Security'
    mapping['MXIRS'] = 'Passenger / Security'
    mapping['MXIS']  = 'Passenger / Security'
    mapping['MXLDC'] = 'Management / Administrative'
    mapping['MXLDT'] = 'Management / Administrative'
    mapping['MXNCA'] = 'Operations / Human Error'
    mapping['MXNOA'] = 'Operations / Human Error'
    mapping['MXO']   = 'General Delay / Other'
    mapping['MXPAA'] = 'Passenger / Security'
    mapping['MXPF']  = 'Infrastructure / Track / Signals'
    mapping['MXPLA'] = 'External / Environment'
    mapping['MXPLB'] = 'External / Environment'
    mapping['MXPLC'] = 'External / Environment'
    mapping['MXPR']  = 'External / Environment'
    mapping['MXPR1'] = 'Passenger / Security'
    mapping['MXPU']  = 'Operations / Human Error'
    mapping['MXSAN'] = 'Cleaning / Unsanitary'
    mapping['MXTD']  = 'Management / Administrative'
    mapping['MXTO']  = 'General Delay / Other'
    mapping['MXUS']  = 'Scheduling / Late Starts'
    mapping['MXWEA'] = 'External / Environment'
    mapping['MXWR']  = 'Management / Administrative'

    # Infrastructure (PX)
    mapping['PXATC'] = 'Infrastructure / Track / Signals'
    mapping['PXDCS'] = 'Infrastructure / Track / Signals'
    mapping['PXEAS'] = 'Infrastructure / Track / Signals'
    mapping['PXEME'] = 'Operations / Human Error'
    mapping['PXEO']  = 'General Delay / Other'
    mapping['PXMEL'] = 'Infrastructure / Track / Signals'
    mapping['PXMO']  = 'General Delay / Other'
    mapping['PXMST'] = 'Passenger / Security'
    mapping['PXOV']  = 'Infrastructure / Track / Signals'
    mapping['PXSAC'] = 'Infrastructure / Track / Signals'
    mapping['PXSBE'] = 'Infrastructure / Track / Signals'
    mapping['PXSCA'] = 'Infrastructure / Track / Signals'
    mapping['PXSCR'] = 'Infrastructure / Track / Signals'
    mapping['PXSI']  = 'Infrastructure / Track / Signals'
    mapping['PXSIS'] = 'Infrastructure / Track / Signals'
    mapping['PXSNT'] = 'General Delay / Other'
    mapping['PXSO']  = 'General Delay / Other'
    mapping['PXSRA'] = 'Infrastructure / Track / Signals'
    mapping['PXSTP'] = 'Infrastructure / Track / Signals'
    mapping['PXSW']  = 'Infrastructure / Track / Signals'
    mapping['PXTD']  = 'Infrastructure / Track / Signals'
    mapping['PXTDN'] = 'External / Environment'
    mapping['PXTIS'] = 'External / Environment'
    mapping['PXTR']  = 'Infrastructure / Track / Signals'
    mapping['PXTS']  = 'Infrastructure / Track / Signals'
    mapping['PXW']   = 'Infrastructure / Track / Signals'
    mapping['PXWZ']  = 'Infrastructure / Track / Signals'

    # Security (SX)
    sx_sec = [
        'SXAE', 'SXAM', 'SXAP', 'SXAX', 'SXBT', 'SXCOL', 'SXDP',
        'SXEAS', 'SXG', 'SXO', 'SXPOL', 'SXROB', 'SXSA', 'SXSP', 'SXUEG'
    ]
    for code in sx_sec:
        mapping[code] = 'Passenger / Security'

    # Transportation (TX)
    mapping['TXATC'] = 'Operations / Human Error'
    mapping['TXCC']  = 'Operations / Human Error'
    mapping['TXDOE'] = 'Operations / Human Error'
    mapping['TXLF']  = 'Scheduling / Late Starts'
    mapping['TXML']  = 'Scheduling / Late Starts'
    mapping['TXMVS'] = 'Operations / Human Error'
    mapping['TXNCA'] = 'Operations / Human Error'
    mapping['TXNIP'] = 'Operations / Human Error'
    mapping['TXNOA'] = 'Operations / Human Error'
    mapping['TXO']   = 'General Delay / Other'
    mapping['TXOI']  = 'Passenger / Security'
    mapping['TXOS']  = 'Operations / Human Error'
    mapping['TXOVS'] = 'Operations / Human Error'
    mapping['TXPD']  = 'Collision / Roadblock'
    mapping['TXPI']  = 'Collision / Roadblock'
    mapping['TXS']   = 'Scheduling / Late Starts'
    mapping['TXST']  = 'External / Environment'
    mapping['TXSUP'] = 'Operations / Human Error'
    mapping['TXSV']  = 'Operations / Human Error'

    return mapping

# Global mapping dictionary
_INCIDENT_CODE_MAP = _build_code_mapping()

# ----------------------------------------------------------------------
# Category name translation (remove slashes, use single words)
# ----------------------------------------------------------------------
CATEGORY_TRANSLATION = {
    'Equipment / Mechanical': 'Mechanical',
    'Operations / Human Error': 'Operations',
    'Infrastructure / Track / Signals': 'Infrastructure',
    'Passenger / Security': 'Passenger',
    'External / Environment': 'External',
    'Scheduling / Late Starts': 'Scheduling',
    'Collision / Roadblock': 'Collision',
    'Cleaning / Unsanitary': 'Cleaning',
    'Management / Administrative': 'Management',
    'General Delay / Other': 'General'
}

# ----------------------------------------------------------------------
# Function to map any incident value to a clean category
# ----------------------------------------------------------------------
def map_to_category(incident):
    """
    Convert an incident code or description into one of ten categories:
    Mechanical, Operations, Infrastructure, Passenger, External,
    Scheduling, Collision, Cleaning, Management, General.
    """
    if pd.isna(incident):
        return 'General'
    s = str(incident).strip()
    if not s:
        return 'General'

    # Step 1: If it looks like a pure code (all caps, 2‑6 letters), use the code map
    if re.match(r'^[A-Z]{2,6}$', s):
        old_cat = _INCIDENT_CODE_MAP.get(s)
        if old_cat:
            return CATEGORY_TRANSLATION.get(old_cat, 'General')
        else:
            return 'General'

    # Step 2: Direct matches for common plain‑text values
    direct_map = {
        'Mechanical': 'Mechanical',
        'General Delay': 'General',
        'Emergency Services': 'Passenger',
        'Investigation': 'Passenger',
        'Diversion': 'Operations',
        'Late Leaving Garage': 'Scheduling',
        'Utilized Off Route': 'Operations',
        'Vision': 'Operations',
        'Late Leaving Garage - Operator': 'Operations',
        'Late Leaving Garage - Mechanical': 'Mechanical',
        'Late Leaving Garage - Management': 'Management',
        'Late Leaving Garage - Vision': 'Operations',
        'Management': 'Management',
        'Operations - Operator': 'Operations',
        'Cleaning': 'Cleaning',
        'Security': 'Passenger',
        'Collision - TTC': 'Collision',
        'Road Blocked - NON-TTC Collision': 'Collision',
        'Road Block - Non-TTC Collision': 'Collision',
        'Roadblock by Collision - Non-TTC': 'Collision',
        'Securitty': 'Passenger',
        'Late Entering Service - Mechanical': 'Mechanical',
        'Held By': 'Passenger',
        'Late Leaving Garage - Operations': 'Operations',
        'e': 'General',
        'Late Entering Service': 'Scheduling',
        'Operations': 'Operations',
        'Cleaning - Unsanitary': 'Cleaning',
        'Cleaning - Disinfection': 'Cleaning',
        'Collision - TTC Involved': 'Collision',
        'Late': 'Scheduling',
        'Overhead': 'Infrastructure',
        'Rail/Switches': 'Infrastructure',
        'MFESA': 'Operations',
        'MFSAN': 'Cleaning',
        'MFUI': 'Passenger',
        'EFO': 'General',
        'EFP': 'General',
        'TFCNO': 'Operations',
        'SFDP': 'Passenger',
        'MFDV': 'Operations',
        'SFPOL': 'Passenger',
        'EFHVA': 'Mechanical',
        'MFUIR': 'Passenger',
        'TFO': 'General',
        'MFO': 'General',
        'MFVIS': 'Operations',
        'EFD': 'General',
        'EFB': 'General',
        'TFPD': 'Collision',
        'TFOI': 'Passenger',
        'MFTO': 'General',
        'MFSH': 'General',
        'MFPI': 'Collision',
        'EFRA': 'Mechanical',
        'MFUS': 'Scheduling',
        'SFO': 'General',
        'MFS': 'General',
        'SFAE': 'Passenger',
        'SFAP': 'Passenger',
        'MFPR': 'External',
        'SFSA': 'Passenger',
        'TFPI': 'Collision',
        'MFWEA': 'External',
        'MTO': 'General',
        'EFCAN': 'General',
        'MTVIS': 'Operations',
        'MTNOA': 'Operations',
        'MTDV': 'Operations',
        'MFFD': 'Passenger',
        'TTPD': 'Collision',
        'STO': 'General',
        'MUIS': 'Passenger',
        'PFPD': 'Collision',
        'MTUS': 'Scheduling',
        'MTSAN': 'Cleaning',
        'MUO': 'General',
        'PFO': 'General',
        'TFLF': 'Scheduling',
        'TFLL': 'Scheduling',
        'ETO': 'General',
        'Overhead - Pantograph': 'Infrastructure',
        'Late  ': 'Scheduling',
        'MTAFR': 'Infrastructure',
        'MTIE': 'Passenger',
        'MTUIR': 'Passenger',
        'ETVC': 'Mechanical',
        'MTUI': 'Passenger',
        'TTSW': 'Operations',
        'STDP': 'Passenger',
        'TTOI': 'Passenger',
        'TTO': 'General',
        'ETRA': 'Mechanical',
        'ETDB': 'Mechanical',
        'TTUS': 'Operations',
        'ETPI': 'Mechanical',
        'PTOV': 'Infrastructure',
        'MTTP': 'Infrastructure',
        'MTPU': 'Operations',
        'STAP': 'Passenger',
        'MTESA': 'Operations',
        'ETDO': 'Mechanical',
        'MTTO': 'General',
        'MTGD': 'General',
        'ETSA': 'Mechanical',
        'ETHV': 'Mechanical',
        'TTPI': 'Collision',
        'ETAC': 'Mechanical',
        'STAE': 'Passenger',
        'PTO': 'Infrastructure',
        'MTPI': 'Collision',
        'ETWS': 'Mechanical',
        'MTPOL': 'Passenger',
        'ETBO': 'Mechanical',
        'ETCE': 'Mechanical',
        'ETTR': 'Mechanical',
        'MTWEA': 'External',
        'ETWA': 'Mechanical',
        'ETSE': 'Mechanical',
        'ETLT': 'Mechanical',
        'PTSW': 'Infrastructure',
        'ETCM': 'Mechanical',
        'MTS': 'External',
        'ETVE': 'Mechanical',
        'ETNT': 'General',
        'PTSE': 'Infrastructure',
        'ETFA': 'Mechanical',
        'ETCO': 'Mechanical',
        'ETLV': 'Mechanical',
        'ETAX': 'Mechanical',
        'STSA': 'Passenger',
        'SUDP': 'Passenger',
        'ETTM': 'Mechanical',
        'ETTB': 'Mechanical',
        'MTTD': 'Management',
        'MTEC': 'Infrastructure',
        'SUO': 'Passenger',
        'TTLL': 'Scheduling',
        'STSP': 'Passenger',
        'TTLF': 'Scheduling',
        'ETDS': 'Mechanical',
        'MUPR1': 'Passenger',
        'MUSAN': 'Cleaning',
        'MUNOA': 'Operations',
        'MUTO': 'General',
        'SUEAS': 'Passenger',
        'MUIR': 'Passenger',
        'MUI': 'Passenger',
        'MUPLB': 'External',
        'SUUT': 'Passenger',
        'EUDO': 'Mechanical',
        'ERWA': 'Mechanical',
        'MUDD': 'External',
        'TUNOA': 'Operations',
        'ERDO': 'Mechanical',
        'MUSC': 'Mechanical',
        'EUPI': 'Mechanical',
        'PUTIS': 'External',
        'TUNIP': 'Operations',
        'ERTC': 'Mechanical',
        'MUPAA': 'Passenger',
        'PUSI': 'Infrastructure',
        'PUSIS': 'Infrastructure',
        'SUG': 'Passenger',
        'EUECD': 'Mechanical',
        'TUML': 'Scheduling',
        'MUD': 'Passenger',
        'EUNT': 'General',
        'EUNEA': 'General',
        'EUCH': 'Mechanical',
        'PUTTC': 'Infrastructure',
        'SUAE': 'Passenger',
        'SUAP': 'Passenger',
        'PUSO': 'Infrastructure',
        'TUO': 'General',
        'ERPR': 'Mechanical',
        'PUCSS': 'Infrastructure',
        'TUSC': 'Operations',
        'PUTIJ': 'Infrastructure',
        'PUSTC': 'Infrastructure',
        'TUST': 'External',
        'MUNCA': 'Operations',
        'ERTL': 'Mechanical',
        'EUME': 'Operations',
        'MRTO': 'General',
        'TUOS': 'Operations',
        'PUSTS': 'Infrastructure',
        'EUTRD': 'Mechanical',
        'TUMVS': 'Operations',
        'MUWEA': 'External',
        'MRWEA': 'External',
        'PUSNT': 'General',
        'SUSA': 'Passenger',
        'SUBT': 'Passenger',
        'TUSUP': 'Operations',
        'EUBK': 'Mechanical',
        'EUSC': 'Mechanical',
        'SUPOL': 'Passenger',
        'EULV': 'Mechanical',
        'PUTWZ': 'Infrastructure',
        'MRO': 'General',
        'TUDOE': 'Operations',
        'MUTD': 'Management',
        'MRNOA': 'Operations',
        'MRUI': 'Passenger',
        'ERCO': 'Mechanical',
        'ERNEA': 'General',
        'MRUIR': 'Passenger',
        'ERLV': 'Mechanical',
        'EUCA': 'Mechanical',
        'TUCC': 'Operations',
        'TUS': 'Scheduling',
        'PUCSC': 'Infrastructure',
        'EUAC': 'Mechanical',
        'EUCD': 'General',
        'EUBO': 'Mechanical',
        'MUPLA': 'External',
        'EUTR': 'Mechanical',
        'ERDB': 'Mechanical',
        'EUVE': 'Mechanical',
        'PUTDN': 'External',
        'MUWR': 'Management',
        'EUVA': 'Mechanical',
        'EUOE': 'Operations',
        'TUSET': 'Operations',
        'EUO': 'General',
        'PUTTP': 'Infrastructure',
        'TUKEY': 'Operations',
        'EUCO': 'Mechanical',
        'ERNT': 'General',
        'PRO': 'General',
        'TRO': 'General',
        'TRNOA': 'Operations',
        'MUEC': 'Infrastructure',
        'MRPLA': 'External',
        'PUTSM': 'Infrastructure',
        'ERRA': 'Mechanical',
        'PUSCA': 'Infrastructure',
        'PUSSW': 'Infrastructure',
        'SUROB': 'Passenger',
        'MRPLB': 'External',
        'SRCOL': 'Passenger',
        'SUCOL': 'Passenger',
        'PUSCR': 'Infrastructure',
        'MRPAA': 'Passenger',
        'PRW': 'Infrastructure',
        'PUTD': 'Infrastructure',
        'TRNIP': 'Operations',
        'TRTC': 'Operations',
        'EUTL': 'Mechanical',
        'ERBO': 'Mechanical',
        'ERTO': 'General',
        'PUTR': 'Infrastructure',
        'EUYRD': 'Mechanical',
        'PUTO': 'Infrastructure',
        'SRAP': 'Passenger',
        'PUTSC': 'Infrastructure',
        'SRUT': 'Passenger',
        'EULT': 'Mechanical',
        'MUESA': 'Operations',
        'EUAL': 'Mechanical',
        'SRDP': 'Passenger',
        'PUSWZ': 'Infrastructure',
        'PRS': 'Infrastructure',
        'SREAS': 'Passenger',
        'ERAC': 'Mechanical',
        'PUSRA': 'Infrastructure',
        'ERTB': 'Mechanical',
        'PUMO': 'Infrastructure',
        'SRO': 'Passenger',
        'MUFM': 'External',
        'MUPLC': 'External',
        'PUMEL': 'Infrastructure',
        'PUTS': 'Infrastructure',
        'TUTD': 'Management',
        'TRSET': 'Operations',
        'EUHV': 'Mechanical',
        'EUTM': 'Mechanical',
        'PUTOE': 'Infrastructure',
        'PUSTP': 'Infrastructure',
        'PUMST': 'Passenger',
        'MUIRS': 'Passenger',
        'ERLT': 'Mechanical',
        'PREL': 'Infrastructure',
        'SUSP': 'Passenger',
        'ERTR': 'Mechanical',
        'ERO': 'General',
        'TRST': 'External',
        'ERME': 'Operations',
        'ERHV': 'Mechanical',
        'ERVE': 'Mechanical',
        'SRBT': 'Passenger',
        'PUTNT': 'General',
        'PUTCD': 'Infrastructure',
        'TRDOE': 'Operations',
        'EUOPO': 'Infrastructure',
        'PUOPO': 'Infrastructure',
        'PUSEA': 'Infrastructure',
        'MRD': 'Passenger',
        'MUCL': 'Management',
        'PRSL': 'Infrastructure',
        'PRTST': 'Infrastructure',
        'MRDD': 'External',
        'MUIE': 'Passenger',
        'MRCL': 'Management',
        'PRSW': 'Infrastructure',
        'PRSA': 'Infrastructure',
        'MRSAN': 'Cleaning',
        'MUGD': 'General',
        'TUOPO': 'Operations',
        'MUATC': 'Infrastructure',
        'PUATC': 'Infrastructure',
        'MUFS': 'External',
        'EUATC': 'Infrastructure',
        'TUATC': 'Operations',
        'MRIE': 'Passenger',
        'PRSO': 'Infrastructure',
        'PUSZC': 'Infrastructure',
        'SRAE': 'Passenger',
        'PUEO': 'Infrastructure',
        'MRESA': 'Operations',
        'PUEWZ': 'Infrastructure',
        'MUPF': 'Infrastructure',
        'PUSAC': 'Infrastructure',
        'ERCD': 'General',
        'ERWS': 'Mechanical',
        'PUSIO': 'Infrastructure',
        'PUEME': 'Operations',
        'PRSP': 'Infrastructure',
        'TUNCA': 'Operations',
        'SUPD': 'Passenger',
        'MRPR1': 'Passenger',
        'EUTAC': 'Mechanical',
        'MRSTM': 'Infrastructure',
        'MRPLC': 'External',
        'MRFS': 'External',
        'TUUR': 'Operations',
        'MUCP': 'Infrastructure',
        'EXTM': 'Mechanical',
        'EXBK': 'Mechanical',
        'EXBO': 'Mechanical',
        'MXTO': 'General',
        'EXO': 'General',
        'TXO': 'General',
        'TXDOE': 'Operations',
        'TXOS': 'Operations',
        'MXUS': 'Scheduling',
        'PXWZ': 'Infrastructure',
        'TXNIP': 'Operations',
        'MXO': 'General',
        'EXDO': 'Mechanical',
        'EXAC': 'Mechanical',
        'PXSTP': 'Infrastructure',
        'PXSW': 'Infrastructure',
        'PXTIS': 'External',
        'MXAFR': 'Infrastructure',
        'PXSIS': 'Infrastructure',
        'EXNEA': 'General',
        'EXCE': 'Mechanical',
        'TXCC': 'Operations',
        'EXATC': 'Mechanical',
        'EXSA': 'Mechanical',
        'EXECD': 'Mechanical',
        'EXGA': 'Mechanical',
        'MXIE': 'Passenger',
        'MXPAA': 'Passenger',
        'PXTDN': 'External',
        'TXOVS': 'Operations',
        'EXYRD': 'Mechanical',
        'TXS': 'Scheduling',
        'PXTS': 'Infrastructure',
        'SXUEG': 'Passenger',
        'MXIR': 'Passenger',
        'PXDCS': 'Infrastructure',
        'EXTR': 'Mechanical',
        'TXSUP': 'Operations',
        'TXNOA': 'Operations',
        'EXWS': 'Mechanical',
        'MXNOA': 'Operations',
        'TXCL': 'Management',
        'MXWEA': 'External',
        'MXIRS': 'Passenger',
        'MXPLC': 'External',
        'EXWA': 'Mechanical',
        'MXD': 'Passenger',
    }

    if s in direct_map:
        return direct_map[s]

    # Step 3: Keyword‑based pattern matching (same logic as original)
    s_lower = s.lower()
    patterns = [
        (r'late (leaving|entering)|unable to maintain schedule|mainline storage', 'Scheduling'),
        (r'collision|road ?block', 'Collision'),
        (r'clean|unsanitary|disinfection', 'Cleaning'),
        (r'management|clerk|training|labour dispute|work refusal', 'Management'),
        (r'weather|ice|snow|fire|debris|force majeure|storm', 'External'),
        (r'mechanical|equipment|brakes|door.*faulty|hvac|propulsion', 'Mechanical'),
        (r'operations?.*operator|signal violation|overshot|overspeed|not in position|supervisory', 'Operations'),
        (r'passenger|security|assault|disorderly|bomb|alarm|unauthorized|injur', 'Passenger'),
        (r'infrastructure|track|signal|power|escalator|elevator|switch|rail|debris.*controllable', 'Infrastructure'),
    ]
    for pattern, category in patterns:
        if re.search(pattern, s_lower):
            return category

    return 'General'

df['Incident_Category'] = df['Incident'].apply(map_to_category)


def clean_route(val):
    """
    Convert route values to integer route numbers:
    - Direct replacements: YU→1, BD→2, SRT→3, SHP→985, FW→6
    - Otherwise, extract the first integer found in the string.
    - If no integer found, return None.
    """
    if pd.isna(val):
        return None
    s = str(val).strip()
    
    # Direct replacements for subway line codes
    replacement_map = {
        'YU': 1,
        'BD': 2,
        'SRT': 3,
        'SHP': 4,
        'FW': 6
    }
    if s in replacement_map:
        return replacement_map[s]
    
    # Extract first integer (including those in strings like "32.0")
    match = re.search(r'\d+', s)
    if match:
        return int(match.group())
    
    # No number found – leave as None (or you could keep original string)
    return None

# Apply to the Route column (updates in place)
df['Route'] = df['Route'].apply(clean_route)
df = df[df['Route'].notnull()]


def download_routes_gtfs():
    """Download routes.txt from the TTC GTFS package and return as DataFrame."""
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    package_id = "merged-gtfs-ttc-routes-and-schedules"

    # Get package metadata
    package_url = f"{base_url}/api/3/action/package_show"
    resp = requests.get(package_url, params={"id": package_id})
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise Exception("CKAN API request failed for GTFS package")

    # Find the ZIP resource (complete GTFS)
    zip_resource = None
    for res in data["result"]["resources"]:
        name = res.get("name", "").lower()
        fmt = res.get("format", "").lower()
        if "gtfs" in name and fmt == "zip":
            zip_resource = res
            break

    if not zip_resource:
        raise Exception("No GTFS ZIP resource found in package")

    print(f"📥 Downloading GTFS ZIP from: {zip_resource['url']}")
    zip_resp = requests.get(zip_resource['url'])
    zip_resp.raise_for_status()

    # Extract routes.txt
    with zipfile.ZipFile(BytesIO(zip_resp.content)) as zf:
        if 'routes.txt' in zf.namelist():
            with zf.open('routes.txt') as f:
                routes_df = pd.read_csv(f)
            print("✅ Extracted routes.txt")
        else:
            raise Exception("routes.txt not found in ZIP")

    return routes_df

# ----------------------------------------------------------------------
# Download routes.txt
# ----------------------------------------------------------------------
routes_df = download_routes_gtfs()
df = df.dropna(subset=['Route'])

# Convert Route to integer (if it's a float like 95.0, it becomes 95)
df['Route'] = pd.to_numeric(df['Route'], errors='coerce').astype('Int64')

# Convert route_short_name to integer as well (GTFS stores them as strings but they are numeric)
routes_df['route_short_name'] = pd.to_numeric(routes_df['route_short_name'], errors='coerce').astype('Int64')

# Drop rows where route_short_name is missing (just in case)
routes_df = routes_df.dropna(subset=['route_short_name'])


# ----------------------------------------------------------------------
# Remove any existing 'route_long_name' column to avoid merge conflicts
# ----------------------------------------------------------------------
if 'route_long_name' in df.columns:
    df = df.drop(columns=['route_long_name'])

df = df.merge(routes_df[['route_short_name', 'route_long_name']],
              left_on='Route',
              right_on='route_short_name',
              how='left')

# Drop the temporary key column
df = df.drop(columns=['route_short_name'])



df['Route'] = pd.to_numeric(df['Route'], errors='coerce').astype('Int64')

# ----------------------------------------------------------------------
# Manual route name mapping (for routes missing in GTFS or needing override)
# ----------------------------------------------------------------------
route_name_manual = {
    3: "Line 3 (Scarborough RT)",
    56: "Leaside",
    195: "Jane Rocket",
    196: "York University Express",
    199: "Finch Rocket",
    # Add others if needed
}

# ----------------------------------------------------------------------
# Fill missing route_long_name using manual map
# ----------------------------------------------------------------------
mask = df['route_long_name'].isna() & df['Route'].isin(route_name_manual.keys())
df.loc[mask, 'route_long_name'] = df.loc[mask, 'Route'].map(route_name_manual)


route_counts = df['Route'].value_counts()

# Identify routes with at least 10 records
routes_to_keep = route_counts[route_counts >= 10].index

# Filter the DataFrame
initial_rows = len(df)
df = df[df['Route'].isin(routes_to_keep)]



subway_routes = [1, 2, 3, 4]
lrt_routes = [5, 6]
streetcar_routes = [301, 304, 305, 306, 310, 312,
                    501, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512]

# Create boolean masks for rows that should be removed
remove_subway = df['Route'].isin(subway_routes) & (df['Transit'] != 'Subway')
remove_lrt = df['Route'].isin(lrt_routes) & (df['Transit'] != 'LRT')
remove_streetcar = df['Route'].isin(streetcar_routes) & (df['Transit'] != 'Streetcar')

# Combine masks – remove if any condition is true
to_remove = remove_subway | remove_lrt | remove_streetcar

# Keep only rows that are not to be removed
df = df[~to_remove].copy()
df.loc[df['Route'] == 985, 'Transit'] = 'Bus'
allowed_streetcar_routes = {
    301, 304, 305, 306, 310, 312,
    501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512,
    514, 521, 522, 500
}

# Identify rows to remove: Transit is Streetcar but Route not in allowed set
to_remove = (df['Transit'] == 'Streetcar') & (~df['Route'].isin(allowed_streetcar_routes))



# Keep only the rows that are NOT in to_remove
df = df[~to_remove].copy()

# Define allowed routes
subway_routes = {1, 2, 3, 4}
streetcar_routes = {
    301, 304, 305, 306, 310, 312,
    501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512,
    514, 521, 522, 500
}

# Masks for invalid combinations
invalid_subway = (df['Transit'] == 'Subway') & (~df['Route'].isin(subway_routes))
invalid_streetcar = (df['Transit'] == 'Streetcar') & (~df['Route'].isin(streetcar_routes))

# Combine masks – remove if either is true
to_remove = invalid_subway | invalid_streetcar

# Keep valid rows
df = df[~to_remove].copy()



def download_trips_gtfs():
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    package_id = "merged-gtfs-ttc-routes-and-schedules"
    package_url = f"{base_url}/api/3/action/package_show"
    resp = requests.get(package_url, params={"id": package_id})
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise Exception("CKAN API request failed for GTFS package")

    zip_resource = None
    for res in data["result"]["resources"]:
        name = res.get("name", "").lower()
        fmt = res.get("format", "").lower()
        if "gtfs" in name and fmt == "zip":
            zip_resource = res
            break

    if not zip_resource:
        raise Exception("No GTFS ZIP resource found")

    print(f"📥 Downloading GTFS ZIP from: {zip_resource['url']}")
    zip_resp = requests.get(zip_resource['url'])
    zip_resp.raise_for_status()

    with zipfile.ZipFile(BytesIO(zip_resp.content)) as zf:
        if 'trips.txt' in zf.namelist():
            with zf.open('trips.txt') as f:
                trips_df = pd.read_csv(f)
            print("✅ Extracted trips.txt")
        else:
            raise Exception("trips.txt not found in ZIP")
    return trips_df

# ----------------------------------------------------------------------
# 2. Load trips and build variant mapping
# ----------------------------------------------------------------------
trips_df = download_trips_gtfs()
print(f"Trips loaded: {len(trips_df)} rows")

# Clean GTFS columns
trips_df['route_id'] = trips_df['route_id'].astype(str).str.strip()
trips_df['trip_short_name'] = trips_df['trip_short_name'].fillna('').astype(str).str.strip()

# Keep only rows with non‑empty trip_short_name (these define variants)
variants_df = trips_df[trips_df['trip_short_name'] != '']

# Group by route_id and collect unique trip_short_names
variant_map = variants_df.groupby('route_id')['trip_short_name'].unique().reset_index()
variant_map['trip_short_name'] = variant_map['trip_short_name'].apply(sorted)

# Build dictionary: route_id -> list of full variant strings (e.g., "129A")
route_variants = {}
for _, row in variant_map.iterrows():
    route = row['route_id']
    short_names = row['trip_short_name']
    route_variants[route] = [f"{route}{sn}" for sn in short_names]

print(f"Variants built for {len(route_variants)} routes")

# ----------------------------------------------------------------------
# 3. Prepare df for variant expansion
# ----------------------------------------------------------------------
# First, map known special route codes to numeric strings
special_map = {
    'YU': '1',
    'BD': '2',
    'SRT': '3',
    'SHP': '985',
    'FW': '6'
}
df['Route'] = df['Route'].replace(special_map)

# Convert Route to clean string:
# - If it's a float like 129.0, convert to int then to string '129'
# - If it's already a string, just strip.
def clean_route(val):
    if pd.isna(val):
        return None
    # Convert to string, then remove trailing .0 if present
    s = str(val).strip()
    # If it ends with '.0', remove it
    if s.endswith('.0'):
        s = s[:-2]
    return s
df['OG'] = df['Route']
df['Route'] = df['Route'].apply(clean_route)

# ----------------------------------------------------------------------
# 4. Add variant list and expand
# ----------------------------------------------------------------------
# Create temporary column with list of variants (NaN if none)
df['_variants'] = df['Route'].map(route_variants)

# For rows where _variants is NaN, replace with a list containing the original Route
mask = df['_variants'].isna()
df.loc[mask, '_variants'] = df.loc[mask, 'Route'].apply(lambda x: [x])

# Now explode
df_expanded = df.explode('_variants').reset_index(drop=True)
df_expanded['Route'] = df_expanded['_variants']
df = df_expanded.drop(columns=['_variants'])

df['Incident'] =  df['Incident_Category']

df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype('Int64')

# ----------------------------------------------------------------------
# Aggregate by Route (as string), Year, Transit, Incident_Category
# ----------------------------------------------------------------------
route_agg = df.groupby(
    ['Route', 'Year', 'Transit', 'Incident_Category'],
    as_index=False,
    dropna=False
).agg(
    Delay_Count=('Min Delay', 'count'),
    Total_Delay_Min=('Min Delay', 'sum')
)

# ----------------------------------------------------------------------
# Compute active_in_2025 (route had >20 incidents in 2025)
# ----------------------------------------------------------------------
incidents_2025 = df[df['Year'] == 2025].groupby('Route').size().reset_index(name='total_2025')
active_routes = incidents_2025[incidents_2025['total_2025'] > 20]['Route'].tolist()
route_agg['active_in_2025'] = route_agg['Route'].isin(active_routes)

# ----------------------------------------------------------------------
# Add route_long_name (if you have this column)
# ----------------------------------------------------------------------
if 'route_long_name' in df.columns:
    route_names = df.dropna(subset=['route_long_name']).groupby('Route')['route_long_name'].first().reset_index()
    route_agg = route_agg.merge(route_names, on='Route', how='left')
    route_agg['route_long_name'] = route_agg['route_long_name'].fillna('Unknown')
else:
    route_agg['route_long_name'] = 'Unknown'

# ----------------------------------------------------------------------
# Filter out any rows where Route is exactly "0" (if that exists)
# ----------------------------------------------------------------------
route_agg = route_agg[route_agg['Route'] != '0']

# ==================== NEW: ADD RANK COLUMN ====================
# Extract base route (leading digits) to group variants
route_agg['Route_base'] = route_agg['Route'].str.extract(r'^(\d+)')[0]

# Sort to ensure consistent ordering (variant order within each base)
route_agg = route_agg.sort_values(
    ['Route_base', 'Year', 'Transit', 'Incident_Category', 'route_long_name', 'Route']
)

# Assign rank within each group of (base, year, transit, category, long_name)
route_agg['rank'] = route_agg.groupby(
    ['Route_base', 'Year', 'Transit', 'Incident_Category', 'route_long_name']
).cumcount() + 1

# Drop the temporary base column
route_agg = route_agg.drop(columns=['Route_base'])
# ================================================================

# ----------------------------------------------------------------------
# Save to assets/data/route_analysis.csv
# ----------------------------------------------------------------------
output_dir = os.path.join('assets', 'data')
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, 'route_analysis.csv')
route_agg.to_csv(output_path, index=False)

df2 = df.copy()

df2['Route'] = df2['OG']
df2 = df2[['Date', 'Time', 'Day', 'Location', 'Incident', 'Min Delay', 'Min Gap',
       'Route', 'Direction', 'Vehicle', 'Transit', 'Year', 'Month', 'Weekday',
       'Hour', 'route_long_name']]
df2= df2.drop_duplicates()

def generate_dashboard_files(df2, gtfs_folder=None, output_folder='assets/data'):
    """
    Generate all TTC dashboard files from an existing delay DataFrame.

    Parameters
    ----------
    df2 : pandas.DataFrame
        DataFrame containing TTC bus delay data with columns like:
        Date, Time, Day, Location, Incident, Min Delay, Min Gap, Route,
        Direction, Vehicle, Year, Month, Weekday, Hour, etc.
    gtfs_folder : str, optional
        Path to folder containing routes.txt and trips.txt. If provided,
        route names and trip variations will be enriched. Otherwise only
        the delay data is used.
    output_folder : str
        Directory where the dashboard files will be saved.
        Defaults to 'assets/data'.
    """
    # ----------------------------------------------------------------------
    # 1. Helper functions (replicated from the original class)
    # ----------------------------------------------------------------------
    def ensure_folder_exists(folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    def clean_and_standardize(df):
        """Standardize column names and enforce uniform schema."""
        df = df.copy()
        df.columns = df.columns.str.strip()

        column_mappings = {
            'Date': ['Report Date', 'Date', 'Incident Date', 'Date & Time'],
            'Time': ['Time', 'Incident Time'],
            'Day': ['Day'],
            'Location': ['Location', 'Station', 'Station Name', 'Stop', 'Stop Name'],
            'Incident': ['Incident', 'Code', 'Description'],
            'Min Delay': ['Min Delay', 'Delay', 'Delay Minutes', 'Delay_Minutes'],
            'Min Gap': ['Min Gap', 'Gap', 'Gap Minutes', 'Gap_Minutes'],
            'Route': ['Route', 'Route Number', 'Route No', 'Route_ID'],
            'Line': ['Line'],
            'Direction': ['Direction', 'Bound'],
            'Vehicle': ['Vehicle', 'Vehicle Number', 'Vehicle_No']
        }

        reverse_mapping = {}
        for std_name, poss_names in column_mappings.items():
            for name in poss_names:
                reverse_mapping[name] = std_name

        rename_dict = {col: reverse_mapping[col] for col in df.columns if col in reverse_mapping}
        df = df.rename(columns=rename_dict)

        # Handle Line column
        if 'Line' in df.columns and 'Route' not in df.columns:
            def extract_route_info(line_val):
                if pd.isna(line_val):
                    return pd.Series([None, None])
                line_str = str(line_val).strip()
                match = re.match(r'^(\d+)(?:\s+(.+))?$', line_str)
                if match:
                    return pd.Series([match.group(1), match.group(2) or ''])
                match = re.match(r'^(\d+)$', line_str)
                if match:
                    return pd.Series([match.group(1), ''])
                return pd.Series([line_str, ''])
            df[['Route', 'Route Name']] = df['Line'].apply(extract_route_info)

        if 'Route' in df.columns and 'Route Name' not in df.columns:
            df['Route Name'] = ''

        required = ['Date', 'Route', 'Route Name', 'Time', 'Day', 'Location',
                    'Incident', 'Min Delay', 'Min Gap', 'Direction', 'Vehicle']
        for col in required:
            if col not in df.columns:
                df[col] = np.nan

        return df[required]

    def clean_delay_data(delay_df):
        """Clean and convert delay data types."""
        df = delay_df.copy()
        df.columns = [str(col).strip() for col in df.columns]

        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

        if 'Time' in df.columns:
            def extract_hour(t):
                if pd.isna(t):
                    return None
                m = re.search(r'(\d{1,2}):', str(t))
                return int(m.group(1)) if m else None
            df['Hour'] = df['Time'].apply(extract_hour)

        for col in ['Min Delay', 'Min Gap', 'Vehicle']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'Route' in df.columns:
            df['Route'] = df['Route'].astype(str).str.extract(r'^(\d+)')[0]

        if 'Date' in df.columns:
            df['Month'] = df['Date'].dt.month_name().str[:3]
            df['Year'] = df['Date'].dt.year
            df['Weekday'] = df['Date'].dt.day_name()

        # Clean string columns
        for col in ['Time', 'Day', 'Location', 'Incident', 'Direction', 'Route Name']:
            if col in df.columns:
                df[col] = df[col].astype(str).replace({'nan': '', 'None': '', 'NaT': ''}).str.strip()

        # Standardize route name casing
        if 'Route Name' in df.columns:
            def std_name(name):
                if pd.isna(name) or name == '':
                    return name
                name_str = str(name).strip()
                if '-' in name_str:
                    return '-'.join(p.strip().title() for p in name_str.split('-'))
                return name_str.title()
            df['Route Name'] = df['Route Name'].apply(std_name)

        return df

    def filter_to_routes_in_2025(df):
        """Return routes that have at least one record in 2025."""
        routes_2025 = set(df[df['Year'] == 2025]['Route'].dropna().unique())
        return df[df['Route'].isin(routes_2025)].copy(), routes_2025

    # ----------------------------------------------------------------------
    # 2. Load GTFS data if folder provided
    # ----------------------------------------------------------------------
    route_name_mapping = {}
    gtfs_trip_pairs = pd.DataFrame(columns=['route_id', 'trip_short_name'])

    if gtfs_folder and os.path.isdir(gtfs_folder):
        routes_file = os.path.join(gtfs_folder, 'routes.txt')
        trips_file = os.path.join(gtfs_folder, 'trips.txt')
        if os.path.exists(routes_file):
            routes_df = pd.read_csv(routes_file)
            if 'route_short_name' in routes_df.columns and 'route_long_name' in routes_df.columns:
                for _, row in routes_df.iterrows():
                    route_short = str(row['route_short_name']).strip()
                    route_long = str(row['route_long_name']).strip()
                    route_name_mapping[route_short] = route_long
        if os.path.exists(trips_file):
            trips_df = pd.read_csv(trips_file)
            if 'route_id' in trips_df.columns and 'trip_short_name' in trips_df.columns:
                trips_df['route_id'] = trips_df['route_id'].astype(str)
                trips_df['trip_short_name'] = trips_df['trip_short_name'].fillna('').astype(str)
                gtfs_trip_pairs = trips_df[['route_id', 'trip_short_name']].drop_duplicates()
    else:
        print("GTFS folder not provided or missing – proceeding without GTFS enrichment.")

    # ----------------------------------------------------------------------
    # 3. Clean input data
    # ----------------------------------------------------------------------
    print("Cleaning input delay data...")
    df_clean = clean_delay_data(df2)

    # Apply route name mapping if available
    if route_name_mapping:
        df_clean['Route Name'] = df_clean.apply(
            lambda row: route_name_mapping.get(str(row['Route']), row['Route Name'])
            if pd.isna(row['Route Name']) or row['Route Name'] == ''
            else row['Route Name'],
            axis=1
        )

    # Keep unfiltered copy for summary and route_performance
    df_all = df_clean.copy()

    # Filter to routes active in 2025 (for most dashboards)
    df_2025, routes_in_2025 = filter_to_routes_in_2025(df_clean)
    print(f"Routes active in 2025: {len(routes_in_2025)}")

    # ----------------------------------------------------------------------
    # 4. Processing functions (adapted from class methods)
    # ----------------------------------------------------------------------

    def process_delay_distribution(df):
        dfv = df[df['Min Delay'] > 0]
        bins = [(0,5,'0-5 min'), (5,10,'5-10 min'), (10,15,'10-15 min'),
                (15,30,'15-30 min'), (30,float('inf'),'30+ min')]
        dist = []
        for lo, hi, label in bins:
            if hi == float('inf'):
                cnt = len(dfv[dfv['Min Delay'] >= lo])
            else:
                cnt = len(dfv[(dfv['Min Delay'] >= lo) & (dfv['Min Delay'] < hi)])
            dist.append({'range': label, 'count': int(cnt),
                         'percentage': round(cnt/len(dfv)*100, 1)})
        return dist

    # MODIFIED: top_delayed_routes now sorted by avg_delay (descending) instead of incident_count
    def process_top_delayed_routes(df, top_n=15):
        dfv = df[df['Min Delay'] > 0]
        cnt = dfv.groupby(['Route','Route Name']).size().reset_index(name='incident_count')
        avg = dfv.groupby('Route')['Min Delay'].mean().reset_index(name='avg_delay')
        stats = pd.merge(cnt, avg, on='Route')
        # Sort by avg_delay descending to show routes with longest delays
        stats = stats.sort_values('avg_delay', ascending=False).head(top_n)
        return [{'route_number': str(r['Route']),
                 'route_name': r['Route Name'] if pd.notna(r['Route Name']) else f"Route {r['Route']}",
                 'incident_count': int(r['incident_count']),
                 'avg_delay': round(float(r['avg_delay']), 1),
                 'active_in_2025': True} for _, r in stats.iterrows()]

    def process_weekday_hour_heatmap(df):
        dfv = df[df['Min Delay'] > 0]
        if 'Weekday' not in dfv.columns or 'Hour' not in dfv.columns:
            return []
        wd_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        data = []
        for wd in wd_order:
            for h in range(24):
                cnt = len(dfv[(dfv['Weekday']==wd) & (dfv['Hour']==h)])
                data.append({'weekday': wd, 'hour': h, 'incident_count': int(cnt), 'hour_label': f"{h:02d}:00"})
        return data

    def process_top_incident_causes(df, top_n=10):
        dfv = df[df['Min Delay'] > 0]
        if 'Incident' not in dfv.columns:
            return []
        cnts = dfv['Incident'].value_counts().head(top_n)
        total = len(dfv)
        return [{'incident_type': str(typ), 'count': int(cnt),
                 'percentage': round(cnt/total*100, 1)} for typ, cnt in cnts.items()]

    def process_hourly_frequency_delay(df):
        dfv = df[df['Min Delay'] > 0]
        if 'Hour' not in dfv.columns:
            return []
        hourly = []
        for h in range(24):
            hd = dfv[dfv['Hour']==h]
            cnt = len(hd)
            avg = hd['Min Delay'].mean() or 0
            hourly.append({'hour': h, 'hour_label': f"{h:02d}:00",
                           'incident_count': int(cnt), 'avg_delay': round(float(avg), 1)})
        return hourly

    def process_time_of_day_comparison(df):
        dfv = df[df['Min Delay'] > 0]
        if 'Hour' not in dfv.columns:
            return []
        periods = [('Morning (5AM-12PM)',5,12), ('Afternoon (12PM-5PM)',12,17),
                   ('Evening (5PM-10PM)',17,22), ('Night (10PM-5AM)',22,29)]
        res = []
        total = len(dfv)
        for name, s, e in periods:
            if s <= e:
                mask = (dfv['Hour'] >= s) & (dfv['Hour'] < e)
            else:
                mask = (dfv['Hour'] >= s) | (dfv['Hour'] < (e % 24))
            d = dfv[mask]
            cnt = len(d)
            res.append({'period': name,
                        'incident_count': int(cnt),
                        'avg_delay': round(d['Min Delay'].mean(), 1) if cnt else 0,
                        'total_delay_minutes': round(d['Min Delay'].sum(), 1) if cnt else 0,
                        'percentage_of_day': round(cnt/total*100, 1)})
        return res

    def process_monthly_trends(df):
        dfv = df[df['Min Delay'] > 0]
        if 'Month' not in dfv.columns:
            return []
        mo_order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        trends = []
        for mo in mo_order:
            md = dfv[dfv['Month']==mo]
            cnt = len(md)
            avg = md['Min Delay'].mean() or 0
            trends.append({'month': mo, 'incident_count': int(cnt), 'avg_delay': round(float(avg), 1)})
        return trends

    def process_yearly_trends(df):
        dfv = df[df['Min Delay'] > 0]
        if 'Year' not in dfv.columns:
            return []
        stats = dfv.groupby('Year')['Min Delay'].agg(['count','mean']).reset_index()
        stats.columns = ['year', 'incident_count', 'avg_delay']
        return [{'year': int(r.year), 'incident_count': int(r.incident_count),
                 'avg_delay': float(r.avg_delay)} for _, r in stats.iterrows()]

    def process_route_scatter_data(df):
        dfv = df[df['Min Delay'] > 0]
        dfv = dfv[dfv['Route'].isin(routes_in_2025)]
        stats = dfv.groupby(['Route','Route Name']).agg(
            incident_count=('Min Delay','count'),
            avg_delay=('Min Delay','mean'),
            delay_std=('Min Delay','std')
        ).reset_index()
        stats = stats[stats['incident_count'] >= 10]
        return [{'route_number': str(r['Route']),
                 'route_name': r['Route Name'] if pd.notna(r['Route Name']) else f"Route {r['Route']}",
                 'incident_count': int(r['incident_count']),
                 'avg_delay': float(r['avg_delay']),
                 'delay_std': float(r['delay_std']) if pd.notna(r['delay_std']) else 0,
                 'active_in_2025': True} for _, r in stats.iterrows()]

    def process_improving_declining_routes(df, year=2025):
        dfv = df[df['Min Delay'] > 0]
        dfv = dfv[dfv['Route'].isin(routes_in_2025)]
        curr = dfv[dfv['Year'] == year]
        prev = dfv[dfv['Year'] == year-1]
        if curr.empty or prev.empty:
            return {'improving':[], 'declining':[]}
        cur_avg = curr.groupby(['Route','Route Name'])['Min Delay'].mean().reset_index(name='current_avg')
        prev_avg = prev.groupby(['Route','Route Name'])['Min Delay'].mean().reset_index(name='previous_avg')
        merged = pd.merge(cur_avg, prev_avg, on=['Route','Route Name'], how='inner')
        merged['change'] = merged['current_avg'] - merged['previous_avg']
        merged['pct'] = (merged['change'] / merged['previous_avg'] * 100).round(1)
        merged = merged.dropna()
        improving = merged.sort_values('change').head(15)
        declining = merged.sort_values('change', ascending=False).head(15)
        return {
            'improving': [{'route_number': str(r['Route']),
                           'route_name': r['Route Name'] if pd.notna(r['Route Name']) else f"Route {r['Route']}",
                           'current_avg_delay': round(float(r['current_avg']),1),
                           'previous_avg_delay': round(float(r['previous_avg']),1),
                           'delay_change': round(float(r['change']),1),
                           'percent_change': float(r['pct']),
                           'improvement': abs(round(float(r['change']),1)),
                           'active_in_2025': True} for _, r in improving.iterrows()],
            'declining': [{'route_number': str(r['Route']),
                           'route_name': r['Route Name'] if pd.notna(r['Route Name']) else f"Route {r['Route']}",
                           'current_avg_delay': round(float(r['current_avg']),1),
                           'previous_avg_delay': round(float(r['previous_avg']),1),
                           'delay_change': round(float(r['change']),1),
                           'percent_change': float(r['pct']),
                           'active_in_2025': True} for _, r in declining.iterrows()]
        }

    def process_monthly_comparison(df, year=2025):
        dfv = df[df['Min Delay'] > 0]
        mo_order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        comp = []
        for mo in mo_order:
            cur = dfv[(dfv['Year']==year) & (dfv['Month']==mo)]
            pre = dfv[(dfv['Year']==year-1) & (dfv['Month']==mo)]
            comp.append({
                'month': mo,
                'current_year': year,
                'previous_year': year-1,
                'current_incident_count': int(len(cur)),
                'previous_incident_count': int(len(pre)),
                'current_avg_delay': round(cur['Min Delay'].mean(), 1) if len(cur) else 0,
                'previous_avg_delay': round(pre['Min Delay'].mean(), 1) if len(pre) else 0,
                'count_change': int(len(cur) - len(pre)),
                'delay_change': round(cur['Min Delay'].mean() - pre['Min Delay'].mean(), 1) if len(cur) and len(pre) else 0,
                'count_percent_change': round(((len(cur)-len(pre))/len(pre)*100) if len(pre) else 0, 1),
                'delay_percent_change': round(((cur['Min Delay'].mean()-pre['Min Delay'].mean())/pre['Min Delay'].mean()*100) if len(pre) and pre['Min Delay'].mean()>0 else 0, 1)
            })
        return comp

    def process_daily_patterns(df):
        dfv = df[df['Min Delay'] > 0]
        wd_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        weekdays = ['Monday','Tuesday','Wednesday','Thursday','Friday']
        weekends = ['Saturday','Sunday']
        patterns = {}
        for wd in wd_order:
            wd_data = dfv[dfv['Weekday']==wd]
            patterns[wd] = [{'hour': h,
                             'hour_label': f"{h:02d}:00",
                             'incident_count': int(len(wd_data[wd_data['Hour']==h])),
                             'avg_delay': round(wd_data[wd_data['Hour']==h]['Min Delay'].mean(),1) or 0}
                            for h in range(24)]
        wd_all = dfv[dfv['Weekday'].isin(weekdays)]
        we_all = dfv[dfv['Weekday'].isin(weekends)]
        wd_agg = [{'hour': h,
                   'incident_count': int(len(wd_all[wd_all['Hour']==h])),
                   'avg_delay': round(wd_all[wd_all['Hour']==h]['Min Delay'].mean(),1) or 0}
                  for h in range(24)]
        we_agg = [{'hour': h,
                   'incident_count': int(len(we_all[we_all['Hour']==h])),
                   'avg_delay': round(we_all[we_all['Hour']==h]['Min Delay'].mean(),1) or 0}
                  for h in range(24)]
        return {'by_weekday': patterns, 'weekday_aggregate': wd_agg, 'weekend_aggregate': we_agg}

    def process_top_delayed_routes_by_count(df, top_n=15):
        """Return top routes sorted by incident count (descending)."""
        dfv = df[df['Min Delay'] > 0]
        cnt = dfv.groupby(['Route','Route Name']).size().reset_index(name='incident_count')
        avg = dfv.groupby('Route')['Min Delay'].mean().reset_index(name='avg_delay')
        stats = pd.merge(cnt, avg, on='Route')
        stats = stats.sort_values('incident_count', ascending=False).head(top_n)
        return [{'route_number': str(r['Route']),
                 'route_name': r['Route Name'] if pd.notna(r['Route Name']) else f"Route {r['Route']}",
                 'incident_count': int(r['incident_count']),
                 'avg_delay': round(float(r['avg_delay']), 1),
                 'active_in_2025': True} for _, r in stats.iterrows()]
    
    def process_weekly_patterns(df):
        """
        Generate weekly summary: each weekday with incident count and average delay.
        """
        dfv = df[df['Min Delay'] > 0]
        if 'Weekday' not in dfv.columns:
            return []
        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        result = []
        for wd in weekdays:
            wd_data = dfv[dfv['Weekday'] == wd]
            cnt = len(wd_data)
            avg = wd_data['Min Delay'].mean() or 0
            result.append({
                'weekday': wd,
                'incident_count': int(cnt),
                'avg_delay': round(float(avg), 1)
            })
        return result

    # ----------------------------------------------------------------------
    # 5. Generate all datasets
    # ----------------------------------------------------------------------
    dashboard = {}

    print("Generating delay distribution...")
    dashboard['delay_distribution'] = process_delay_distribution(df_2025)

    print("Generating top delayed routes...")
    dashboard['top_delayed_routes'] = process_top_delayed_routes(df_2025)

    print("Generating weekday-hour heatmap...")
    dashboard['weekday_hour_heatmap'] = process_weekday_hour_heatmap(df_2025)

    print("Generating top incident causes...")
    dashboard['top_incident_causes'] = process_top_incident_causes(df_2025)

    dashboard['top_delayed_routes_count'] = process_top_delayed_routes_by_count(df_2025)

    print("Generating hourly frequency & delay...")
    dashboard['hourly_frequency_delay'] = process_hourly_frequency_delay(df_2025)

    print("Generating time of day comparison...")
    dashboard['time_of_day_comparison'] = process_time_of_day_comparison(df_2025)

    print("Generating monthly trends...")
    dashboard['monthly_trends'] = process_monthly_trends(df_2025)

    print("Generating weekly patterns...")
    dashboard['weekly_patterns'] = process_weekly_patterns(df_2025)

    print("Generating yearly trends...")
    dashboard['yearly_trends'] = process_yearly_trends(df_2025)

    print("Generating route scatter data...")
    dashboard['route_scatter_data'] = process_route_scatter_data(df_2025)

    print("Generating improving/declining routes...")
    yoy = process_improving_declining_routes(df_2025)
    dashboard['improving_routes'] = yoy['improving']
    dashboard['declining_routes'] = yoy['declining']

    print("Generating monthly comparison...")
    dashboard['monthly_comparison'] = process_monthly_comparison(df_2025)

    print("Generating daily patterns...")
    dashboard['daily_patterns'] = process_daily_patterns(df_2025)

    # ----------------------------------------------------------------------
    # 6. Save only the requested JSON files (no CSVs, no combined)
    # ----------------------------------------------------------------------
    ensure_folder_exists(output_folder)
    dashboard_dir = os.path.join(output_folder, 'dashboard')
    ensure_folder_exists(dashboard_dir)

    # List of required files (exactly as specified)
    required_files = [
        'delay_distribution',
        'top_delayed_routes',
        'top_delayed_routes_count',   # new file added
        'weekday_hour_heatmap',
        'top_incident_causes',
        'hourly_frequency_delay',
        'time_of_day_comparison',
        'monthly_trends',
        'yearly_trends',
        'route_scatter_data',
        'improving_routes',
        'declining_routes',
        'monthly_comparison',
        'daily_patterns',
        'weekly_patterns'
    ]
    
    for name in required_files:
        if name in dashboard:
            file_path = os.path.join(dashboard_dir, f"{name}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(dashboard[name], f, indent=2, default=str)
            print(f"Saved {name}.json")
        else:
            print(f"Warning: {name} not found in dashboard data, skipping.")

    print("\n✅ All requested dashboard files generated successfully!")
    print(f"📁 Output folder: {dashboard_dir}")

df2['Route Name'] = df2['route_long_name']   # ensure Route Name column exists
generate_dashboard_files(df2, gtfs_folder='path/to/gtfs', output_folder='assets/data')