import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption
import re # Import the regular expression module for sanitizing filenames

# Load env variables
load_dotenv()

# ── CONFIG ──────────────────────────────────────────────
# (Your configuration remains the same)
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", "DILIGENT-DILIGENTUS1")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER", "Cognida")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "COGNIDA_RL")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "REPORTING_WH")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "PULSE_SURVEY")

SURVEY_REPORT_TABLE      = "PULSE_SURVEY.INTERMEDIATE.INT_PULSE_SURVEY_REPORT"
EMAIL_MAPPING_TABLE      = "PULSE_SURVEY.INTERMEDIATE.INT_EMP_EMAIL_MAPPING"
EMPLOYEE_HIERARCHY_TABLE = "PULSE_SURVEY.CURATED.EMPLOYEE_HIERARCHY"
SURVEY_SENTIMENT_DETAILS_TABLE = "PULSE_SURVEY.INTERMEDIATE.SURVEY_SENTIMENT_DETAILS"
PULSE_SURVEY_RECIPIENT_LIST_WEEKLY_TABLE = "PULSE_SURVEY.CURATED.PULSE_SURVEY_RECIPIENT_LIST_WEEKLY"

# ── LOAD PRIVATE KEY FROM ENV ────────────────────────────
# (Your private key loading remains the same)
private_key_str = os.getenv("SNOWFLAKE_PRIVATE_KEY")
if not private_key_str:
    raise ValueError("❌ Missing SNOWFLAKE_PRIVATE_KEY in environment variables")
private_key_bytes = private_key_str.encode("utf-8")
p_key = load_pem_private_key(private_key_bytes, password=None)
private_key = p_key.private_bytes(
    Encoding.DER,
    PrivateFormat.PKCS8,
    NoEncryption()
)

# ── CONNECT TO SNOWFLAKE ────────────────────────────────
# (Your connection remains the same)
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    account=SNOWFLAKE_ACCOUNT,
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    private_key=private_key
)

# (Your query remains the same)
query = f"""
SELECT 
    r.EMP_ID,
    r.EMP_NAME,
    r.EMP_EMAIL AS EMP_EMAIL_ENCODED,
    m.EMP_EMAIL AS EMP_EMAIL_REAL,
    h.EMPLOYEE_NAME AS HIER_EMPLOYEE_NAME,
    h.EMPLOYEE_EMAIL AS HIER_EMPLOYEE_EMAIL,
    h.MANAGER_NAME AS HIER_MANAGER_NAME,
    h.MANAGER_EMAIL AS HIER_MANAGER_EMAIL,
    r.PULSE_SURVEY_DATE,
    r.SUBMISSION_DATE,
    r.BUSINESS_UNIT,
    r.DEPT,
    r.EMP_REGION,
    r.I_FEEL_EMPOWERED_AND_ACCOUNTABLE_TO_ACHIEVE_MY_DILIGENT_GOALS,
    r.HAVE_YOU_HAD_A_1_ON_1_WITH_YOUR_MANAGER_IN_THE_LAST_ONE_OR_TWO_WEEKS,
    r.PLEASE_SHARE_WHY_YOU_CHOSE_THIS_SCORE_THIS_WEEK
FROM {SURVEY_REPORT_TABLE} r
LEFT JOIN {EMAIL_MAPPING_TABLE} m
    ON r.EMP_EMAIL = m.ENCODED_EMAIL
LEFT JOIN {EMPLOYEE_HIERARCHY_TABLE} h
    ON m.EMP_EMAIL = h.EMPLOYEE_EMAIL
INNER JOIN {PULSE_SURVEY_RECIPIENT_LIST_WEEKLY_TABLE} mm
    ON h.MANAGER_EMAIL = mm.RECIPIENT;
"""

# FETCH TO DATAFRAME ──────────────────────────────────
print("Fetching data from Snowflake...")
df = pd.read_sql(query, conn)
print("Data fetched successfully.")

# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# NEW LOGIC: GROUP BY MANAGER AND CREATE A CSV FOR EACH
# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

# Define the columns you want in your final output CSVs
final_columns = [
    'EMP_NAME',
    'PULSE_SURVEY_DATE',
    'SUBMISSION_DATE',
    'I_FEEL_EMPOWERED_AND_ACCOUNTABLE_TO_ACHIEVE_MY_DILIGENT_GOALS',
    'HAVE_YOU_HAD_A_1_ON_1_WITH_YOUR_MANAGER_IN_THE_LAST_ONE_OR_TWO_WEEKS',
    'PLEASE_SHARE_WHY_YOU_CHOSE_THIS_SCORE_THIS_WEEK'
]

# Group the DataFrame by the manager's name from the hierarchy table
# The 'HIER_MANAGER_NAME' column comes from your SQL query
manager_groups = df.groupby('HIER_MANAGER_NAME')

# Loop through each group (where each group is one manager's team)
for manager_name, manager_df in manager_groups:
    
    # It's good practice to sanitize names for filenames.
    # This replaces spaces and other non-alphanumeric characters with an underscore.
    sanitized_name = re.sub(r'[^\w-]', '_', manager_name)
    os.makedirs('output', exist_ok=True)  # Ensure the output directory exists
    # Create the dynamic filename
    filename = f"output/pulse_survey_analysis_for_{sanitized_name}.csv"

    # Select only the desired columns for the output file
    output_df = manager_df[final_columns]

    # Save the filtered DataFrame to its own CSV file, without the index column
    output_df.to_csv(filename, index=False)
    
    print(f"✅ Successfully created '{filename}'")

# Close the connection
conn.close()
print("Snowflake connection closed.")