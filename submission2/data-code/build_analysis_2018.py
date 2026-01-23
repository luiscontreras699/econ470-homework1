import pandas as pd
import os

# ============================================
# 1. SETUP PATHS
# ============================================
data_input_path = "./data/input/"
results_path = "./submission2/results/"
os.makedirs(results_path, exist_ok=True)  # Create results folder

# ============================================
# 2. LOAD & COMBINE MONTHLY ENROLLMENT DATA
# ============================================
print("Step 1/6: Loading monthly enrollment data...")
enrollment_dfs = []

for month in range(1, 13):
    month_str = f"{month:02d}"
    file_path = os.path.join(data_input_path, f"CPSC_Enrollment_Info_2018_{month_str}.csv")
    
    df_month = pd.read_csv(file_path, low_memory=False)
    # Add month identifier for tracking
    df_month['source_month'] = month_str
    enrollment_dfs.append(df_month)

# Combine all months
df_enrollment_raw = pd.concat(enrollment_dfs, ignore_index=True)

# ============================================
# 3. LOAD & COMBINE CONTRACT INFO DATA
# ============================================
print("Step 2/6: Loading contract/plan characteristics...")
contract_dfs = []

for month in range(1, 13):
    month_str = f"{month:02d}"
    file_path = os.path.join(data_input_path, f"CPSC_Contract_Info_2018_{month_str}.csv")
    
    # TRY DIFFERENT ENCODINGS TO HANDLE SPECIAL CHARACTERS
    try:
        df_contract = pd.read_csv(file_path, low_memory=False, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df_contract = pd.read_csv(file_path, low_memory=False, encoding='windows-1252')
            print(f"  Note: Used 'windows-1252' encoding for month {month_str}")
        except:
            df_contract = pd.read_csv(file_path, low_memory=False, encoding='iso-8859-1')
            print(f"  Note: Used 'iso-8859-1' encoding for month {month_str}")
    
    # We only need key columns: Contract ID, Plan ID, Plan Type, SNP Plan, EGHP
    key_cols = ['Contract ID', 'Plan ID', 'Plan Type', 'SNP Plan', 'EGHP']
    df_contract = df_contract[key_cols].drop_duplicates()
    contract_dfs.append(df_contract)
# Combine and drop any duplicates across months
df_contract_info = pd.concat(contract_dfs, ignore_index=True).drop_duplicates()

# ============================================
# 4. MERGE ENROLLMENT WITH CONTRACT INFO
# ============================================
print("Step 3/6: Merging enrollment with plan info...")
# Standardize column names for merging
df_enrollment_raw = df_enrollment_raw.rename(columns={
    'Contract Number': 'Contract ID',
    'Plan ID': 'Plan ID'  # Already matches
})

# Perform the merge - how='left' keeps all enrollment rows with their plan info
df_enrollment_with_info = pd.merge(
    df_enrollment_raw,
    df_contract_info,
    on=['Contract ID', 'Plan ID'],
    how='left'
)

# ============================================
# 5. COLLAPSE TO YEARLY PLAN-COUNTY TOTALS
# ============================================
print("Step 4/6: Collapsing monthly data to yearly totals...")

# First, convert 'Enrollment' column to numeric, forcing errors (like '*') to NaN
df_enrollment_with_info['Enrollment'] = pd.to_numeric(df_enrollment_with_info['Enrollment'], errors='coerce')

# Now group by unique plan-county combination and sum enrollments
grouping_cols = ['Contract ID', 'Plan ID', 'State', 'County', 'Plan Type', 'SNP Plan', 'EGHP']
df_yearly = (
    df_enrollment_with_info.groupby(grouping_cols, as_index=False)
    .agg(
        tot_enroll=('Enrollment', 'sum')  # Sum yearly enrollment
    )
)

print(f"Created yearly dataset with {df_yearly.shape[0]} plan-county observations.")

# ============================================
# 6. LOAD SERVICE AREA & PERFORM INNER MERGE
# ============================================
print("Step 5/6: Loading service area data and performing inner merge...")
service_area_dfs = []

for month in range(1, 13):
    month_str = f"{month:02d}"
    file_path = os.path.join(data_input_path, f"MA_Cnty_SA_2018_{month_str}.csv")
    
    df_sa = pd.read_csv(file_path, low_memory=False)
    # We only need Contract ID and County to identify approved pairs
    df_sa = df_sa[['Contract ID', 'County']].drop_duplicates()
    service_area_dfs.append(df_sa)

# Combine and deduplicate: a contract-county pair is "approved" if it appears in any month
df_service_area = pd.concat(service_area_dfs, ignore_index=True).drop_duplicates()
df_service_area['approved'] = 1  # Add a flag

print(f"Found {df_service_area.shape[0]} approved contract-county pairs.")

# INNER MERGE: Keep only plan-counties that are approved
df_final = pd.merge(
    df_yearly,
    df_service_area,
    on=['Contract ID', 'County'],
    how='inner'  # This is the crucial INNER JOIN per instructions
)

print(f"Final dataset after inner merge: {df_final.shape[0]} observations.")
print(f"Preview:\n", df_final.head())

# ============================================
# 7. GENERATE THE THREE REQUIRED TABLES
# ============================================
print("\n" + "="*50)
print("Step 6/6: Generating analysis tables...")

# --- TABLE 1: Count of plans by Plan Type ---
table1 = (
    df_final['Plan Type'].value_counts()
    .reset_index()
    .rename(columns={'index': 'Type', 'Plan Type': 'Count'})
)
print("\n✅ TABLE 1: Plan Count by Type (All Approved)")
print(table1)

# --- TABLE 2: Apply exclusions and re-count ---
# Apply filters: exclude SNP, EGHP, and 800-series plans
# Filter 1: Exclude Special Needs Plans (SNP Plan == 'Yes')
df_filtered = df_final[df_final['SNP Plan'] != 'Yes']

# Filter 2: Exclude Employer Group Plans (EGHP == 'Yes')
df_filtered = df_filtered[df_filtered['EGHP'] != 'Yes']

# Filter 3: Exclude "800-series" plans (Plan ID starts with '8' or between 800-899)
# Convert Plan ID to string and check first character
df_filtered['Plan ID_str'] = df_filtered['Plan ID'].astype(str)
df_filtered = df_filtered[~df_filtered['Plan ID_str'].str.startswith('8')]
df_filtered = df_filtered.drop(columns=['Plan ID_str'])  # Clean up temporary column

print(f"\nFiltered dataset has {df_filtered.shape[0]} observations after exclusions.")

table2 = (
    df_filtered['Plan Type'].value_counts()
    .reset_index()
    .rename(columns={'index': 'Type', 'Plan Type': 'Count'})
)
print("\n✅ TABLE 2: Plan Count by Type (After Excluding SNP, EGHP, 800-series)")
print(table2)

# --- TABLE 3: Average enrollment by Plan Type (on filtered & approved) ---
# The df_filtered already contains only approved counties from the inner merge
table3 = (
    df_filtered.groupby('Plan Type', as_index=False)['tot_enroll']
    .mean()
    .round(2)
    .rename(columns={'Plan Type': 'Type', 'tot_enroll': 'Average Enrollment'})
)
print("\n✅ TABLE 3: Average Enrollment by Plan Type (Filtered & Approved)")
print(table3)

# ============================================
# 8. SAVE RESULTS
# ============================================
table1.to_csv(os.path.join(results_path, "table1_plan_count_all.csv"), index=False)
table2.to_csv(os.path.join(results_path, "table2_plan_count_filtered.csv"), index=False)
table3.to_csv(os.path.join(results_path, "table3_avg_enrollment.csv"), index=False)
