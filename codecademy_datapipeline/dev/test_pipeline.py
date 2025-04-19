import unittest
import pandas as pd
from pipeline_script import *

class TestPipelineFunctions(unittest.TestCase):
    def test_process_missing_data(self):
        # Sample dataframe
        data = {
            'col1': ['1', '2', None, '3'],
            'col2': [1, 3, None, 7],
            'col3': [9, 988, None, 4]
        }
        df = pd.DataFrame(data)
        print(df.dtypes)

        # Calls the function on the dataframe to fill None values
        filled_df1 = process_missing_data(df, ['col1'], filler='0')
        filled_df1 = process_missing_data(filled_df1, ['col2', 'col3'], filler=5)

        # Verify that no None values remain
        self.assertEqual(filled_df1['col1'].isnull().sum(), 0)

        # Verify that the correct filler values were applied
        self.assertEqual(filled_df1['col1'].iloc[2], '0')
        self.assertEqual(filled_df1['col2'].iloc[2], 5)
        self.assertEqual(filled_df1['col3'].iloc[2], 5)

        self.assertTrue(pd.api.types.is_numeric_dtype(filled_df1['col2']))
        self.assertTrue(pd.api.types.is_numeric_dtype(filled_df1['col3']))
        self.assertTrue(pd.api.types.is_object_dtype(filled_df1['col1']))

    def test_split_json_column(self):
        # Sample DataFrame with a JSON string
        data = {
            'json_column': ['{"name": "John", "age": 30}', '{"name": "Jane", "age": 25}']
        }
        df = pd.DataFrame(data)

        # Columns to create from the JSON column
        new_columns = ['name', 'age']

        # Call the function to split the JSON column
        result_df = split_json_column(df, 'json_column', new_columns)

        # Check that the new columns were added
        self.assertTrue(all(col in result_df.columns for col in new_columns))

        # Check if the values were split
        self.assertEqual(result_df['name'].iloc[0], 'John')
        self.assertEqual(result_df['age'].iloc[0], 30)
        self.assertEqual(result_df['name'].iloc[1], 'Jane')
        self.assertEqual(result_df['age'].iloc[1], 25)

        # Check if the original JSON column is removed
        self.assertNotIn('json_column', result_df.columns)

    def test_merge_dataframes(self):
        # Sample data for students, jobs, and courses DataFrames
        data_students = {
            'student_id': [1, 2, 3],
            'job_id': [1, 2, 3],
            'current_career_path_id': [101, 102, None]
        }
        data_jobs = {
            'job_id': [1, 2, 3],
            'job_title': ['Engineer', 'Scientist', 'Artist']
        }
        data_courses = {
            'career_path_id': [101, 102],
            'career_path_name': ['Data Science', 'Biology']
        }

        df_students = pd.DataFrame(data_students)
        df_jobs = pd.DataFrame(data_jobs)
        df_courses = pd.DataFrame(data_courses)

        # Call merge_dataframes function
        merged_df, duplicates = merge_dataframes(df_students, df_jobs, df_courses)

        # Test that merged_df is not empty
        self.assertFalse(merged_df.empty)

        # Check if the career path name is filled for students with None current_career_path_id
        self.assertEqual(merged_df['career_path_name'].iloc[2], 'No Career Path')

        # Check for duplicate rows (none in this case)
        self.assertEqual(duplicates.sum(), 0)
        
		#Check that no lines from df_students were dropped during merge
        self.assertEqual(len(merged_df), len(df_students))

if __name__ == '__main__':
    unittest.main()


