import os
import pandas as pd
from datetime import datetime


def compare_files(file1_path, file2_path,bucket_name):
    # Read files into pandas DataFrames
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    # Initialize an empty DataFrame to store output
    output_df = pd.DataFrame()

    # Iterate over rows
    # enumerate = when dealing with iterators, we also need to keep a count of iterations.
    # The itertuples() method is used to iterate over the rows of a DataFrame.
    for index, (row1, row2) in enumerate(zip(df1.itertuples(index=False), df2.itertuples(index=False))):
        # Initialize a dictionary to store differing columns
        diff_columns = {}

        # Iterate over columns
        # zip is used for multiple iterator to single
        for col_name, val1, val2 in zip(df1.columns, row1, row2):
            # Check if values are different
            if val1 != val2:
                # If different, add column to the dictionary
                diff_columns[col_name+'_DB2'] = val1
                diff_columns[col_name + '_BigQuery'] = val2
            else:
                # If same, add empty values
                diff_columns[col_name+'_DB2'] = ''
                diff_columns[col_name + '_BigQuery'] = ''

        # Append the row to the output DataFrame
        output_df = output_df._append(diff_columns, ignore_index=True)




    # Generate output file names with original file names and date-time
    output_filename_correct = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(file1_path)}"

    output_path = f'gs://{bucket_name}/Output/{output_filename_correct}'

    # Write output to CSV
    output_df.to_csv(output_path, index=False)
    df = pd.read_csv(output_path)
    pd.options.display.max_columns = len(df.columns)
    print(df)

    # print("Discrepancy"+pd.read_csv(output_path))

bucket_name = 'mybucket_hsbc'
file1_path = 'gs://mybucket_hsbc/db2_changed.csv'
file2_path = 'gs://mybucket_hsbc/BigQueryData.csv'
output_path = f'gs://mybucket_hsbc/Output/Discrepancy_Output.csv'
compare_files(file1_path,file2_path,bucket_name)
