# Subscriber Cancellations Data Pipeline

# Setup and helper functions

import pandas as pd
import sys
import json
import sqlite3
import logging

# Configure logging
logging.basicConfig(filename='script_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
version = "v1.0.2"  # Manually updated
logging.info(f"Starting script version {version}")

#function to evaluate change in the database
def db_vs_updated(base_db, updated_db):
    try:
        base_conn = sqlite3.connect(base_db)
        updated_conn = sqlite3.connect(updated_db)

        table_name = "cademycode_students"
        new_data_found = False

        # Check if the table exists in both DBs
        base_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", base_conn)
        updated_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", updated_conn)

        if table_name in base_tables['name'].values and table_name in updated_tables['name'].values:
            base_row_count = pd.read_sql_query(f"SELECT COUNT(*) FROM {table_name}", base_conn).iloc[0, 0]
            updated_row_count = pd.read_sql_query(f"SELECT COUNT(*) FROM {table_name}", updated_conn).iloc[0, 0]

            logging.info(f"{table_name} - base row count: {base_row_count}")
            logging.info(f"{table_name} - updated row count: {updated_row_count}")

            if updated_row_count > base_row_count:
                logging.info(f"{table_name} has {updated_row_count - base_row_count} new rows in the updated DB.")
                new_data_found = True
        else:
            logging.warning(f"Table '{table_name}' not found in one of the databases.")

        chosen_db = updated_db if new_data_found else base_db
        logging.info(f"Chosen database for loading: {chosen_db}")
        return chosen_db

    except Exception as e:
        logging.error(f"Error comparing databases: {e}")
        return base_db  # Fallback to base DB if there's an error

    finally:
        base_conn.close()
        updated_conn.close()

# Function to establish connection and create dataframes
def load_data(database_path):
    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = [row[0] for row in cursor.fetchall()]
        logging.info(f"Tables found: {table_names}")

        dfs = {}
        for table in table_names:
            dfs[table] = pd.read_sql_query(f"SELECT * FROM {table}", conn)

        df_students = dfs["cademycode_students"]
        df_courses = dfs["cademycode_courses"]
        df_jobs = dfs["cademycode_student_jobs"]

        logging.info(f"Rows loaded - Students: {len(df_students)}, Courses: {len(df_courses)}, Jobs: {len(df_jobs)}")

        return df_students, df_courses, df_jobs

    except Exception as e:
        logging.error(f"An error occurred while loading data: {e}")
        sys.exit("Data loading failed.")

    finally:
        if conn:
            conn.close()

# Function to split json data into columns
def split_json_column(df, json_column, new_columns):
    if json_column not in df.columns:
        raise ValueError(f"Column '{json_column}' not found in the DataFrame.")
    
    try:
        df[new_columns] = df[json_column].apply(lambda x: pd.Series(json.loads(x) if isinstance(x, str) else x))
        df.drop(columns=[json_column], inplace=True)
        logging.info(f"Column '{json_column}' split into: {new_columns}")
    except Exception as e:
        raise ValueError(f"Error parsing JSON in column '{json_column}': {e}")
    
    return df

# Function to fill missing data
def process_missing_data(df, columns_to_fill, subset_for_dropna=None, filler='0'):
    if subset_for_dropna:
        original_len = len(df)
        df = df.dropna(subset=subset_for_dropna)
        logging.info(f"Dropped {original_len - len(df)} rows due to missing values in {subset_for_dropna}")
    
    for column in columns_to_fill:
        missing_before = df[column].isna().sum()
        df.loc[:, column] = df[column].fillna(filler)
        missing_after = df[column].isna().sum()
        logging.info(f"Filled missing values in '{column}': {missing_before - missing_after} filled")

    return df

# Function to merge dataframes
def merge_dataframes(df_students, df_jobs, df_courses):
    try:
        df_students["job_id"] = pd.to_numeric(df_students["job_id"], errors='coerce').astype("Int64")
        df_students["current_career_path_id"] = pd.to_numeric(df_students["current_career_path_id"], errors='coerce').astype("Int64")

        merged_df = pd.merge(df_students, df_jobs, on="job_id", how="left")
        merged_df = pd.merge(merged_df, df_courses, left_on="current_career_path_id", right_on="career_path_id", how="left")

        merged_df['career_path_id'] = merged_df['career_path_id'].fillna(0).astype('int64')
        merged_df['career_path_name'] = merged_df['career_path_name'].fillna('No Career Path')

        duplicates = merged_df.duplicated(keep=False)
        logging.info(f"Merged DataFrame shape: {merged_df.shape}")
        logging.info(f"Duplicate rows in merged dataframe: {duplicates.sum()}")

        return merged_df, duplicates

    except KeyError as e:
        logging.error(f"KeyError during merge: {e}")
    except ValueError as e:
        logging.error(f"ValueError during type conversion in merge: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during merge: {e}")
        return None, None

# Function to export cleaned data to SQLite and CSV
# df is the input dataframe we are working on
# db_name is the name of the database file the function creates
# table_name is the name of the new df in the new file
def export_to_sqlite_and_csv(df, db_name='clean_data.db', table_name='students', csv_filename='final_output.csv'):
    try:
        conn = sqlite3.connect(db_name)
        #take the dataframe and saves as a table in db_name
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
        logging.info(f"Exported table list from '{db_name}': {tables['name'].tolist()}")

        result_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        result_df.to_csv(csv_filename, index=False)
        logging.info(f"CSV file '{csv_filename}' has been created with {len(result_df)} rows.")

    except sqlite3.Error as e:
        logging.error(f"SQLite error occurred: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during export: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

# Load the data
chosen_db = db_vs_updated("cademycode.db", "cademycode_updated.db")
df_students, df_courses, df_jobs = load_data(chosen_db)

# Split JSON column
split_json_column(df_students, 'contact_info', ['mailing_address', 'email'])

# Log missing data before processing
logging.info("Missing data before processing:")
logging.info(f"job_id: {df_students['job_id'].isna().sum()}")
logging.info(f"num_course_taken: {df_students['num_course_taken'].isna().sum()}")
logging.info(f"current_career_path_id: {df_students['current_career_path_id'].isna().sum()}")
logging.info(f"time_spent_hrs: {df_students['time_spent_hrs'].isna().sum()}")

# Handle missing data
df_students = process_missing_data(
    df_students,
    columns_to_fill=['num_course_taken', 'current_career_path_id', 'time_spent_hrs'],
    subset_for_dropna=['job_id'],
    filler='0'
)

# Fix data types
df_students['dob'] = pd.to_datetime(df_students['dob'])
df_students['num_course_taken'] = pd.to_numeric(df_students['num_course_taken'], errors='coerce').astype('Int64')
df_students['current_career_path_id'] = pd.to_numeric(df_students['current_career_path_id'], errors='coerce').astype('int64')
df_students['job_id'] = pd.to_numeric(df_students['job_id'], errors='coerce').astype('int64')
df_students['time_spent_hrs'] = df_students['time_spent_hrs'].astype('float64')

# Remove duplicates
before_dedup = len(df_students)
df_students = df_students.drop_duplicates(subset=['uuid'], keep='first')
after_dedup = len(df_students)
logging.info(f"Removed {before_dedup - after_dedup} duplicate student records")

# Clean df_jobs
df_jobs = df_jobs.drop_duplicates(keep='first')
logging.info(f"Job table cleaned. Total records: {len(df_jobs)}")

# Merge
final_df, duplicates = merge_dataframes(df_students, df_jobs, df_courses)

# Fill remaining nulls in hours_to_complete
final_df = process_missing_data(final_df, ['hours_to_complete'], filler=0)

# Export results
export_to_sqlite_and_csv(final_df, db_name='clean_data3.db', table_name='students', csv_filename='final_output3.csv')

