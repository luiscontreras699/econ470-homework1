import pandas as pd
import os

data_input_path = "./data/input/"

# --- 1. Check Columns in an Enrollment File ---
enroll_file = os.path.join(data_input_path, "CPSC_Enrollment_Info_2018_01.csv")
print("=== Checking ENROLLMENT File ===")
print(f"File: {enroll_file}")
try:
    # Read just the first few rows to see the structure
    df_enroll_sample = pd.read_csv(enroll_file, nrows=5)
    print("First 5 rows:")
    print(df_enroll_sample)
    print("\nColumn names:", list(df_enroll_sample.columns))
    print("-" * 50)
except Exception as e:
    print(f"Error reading enrollment file: {e}")

# --- 2. Check Columns in a Contract Info File ---
contract_file = os.path.join(data_input_path, "CPSC_Contract_Info_2018_01.csv")
print("\n=== Checking CONTRACT INFO File ===")
print(f"File: {contract_file}")
try:
    df_contract_sample = pd.read_csv(contract_file, nrows=5)
    print("First 5 rows:")
    print(df_contract_sample)
    print("\nColumn names:", list(df_contract_sample.columns))
    print("-" * 50)
except Exception as e:
    print(f"Error reading contract file: {e}")

# --- 3. Check Columns in a Service Area File ---
service_file = os.path.join(data_input_path, "MA_Cnty_SA_2018_01.csv")
print("\n=== Checking SERVICE AREA File ===")
print(f"File: {service_file}")
try:
    df_service_sample = pd.read_csv(service_file, nrows=5)
    print("First 5 rows:")
    print(df_service_sample)
    print("\nColumn names:", list(df_service_sample.columns))
except Exception as e:
    print(f"Error reading service area file: {e}")