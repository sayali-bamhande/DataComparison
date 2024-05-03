import os
from datetime import datetime

import pandas as pd
from gcsfs import GCSFileSystem


# Load the CSV files into data frames
# df1 = pd.read_csv('file1.csv')
# df2 = pd.read_csv('file2.csv')

def compareCSVwithDataFrame(file1_path, file2_path, project_name, bucket_name):
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)
    # Initialize lists to store column names and values
    column_names = []
    column_values = []
    # Extract column names and values for df1 and df2 side by side
    for col_df1, col_df2 in zip(df1.columns, df2.columns):
        column_names.append(f"{col_df1} (df1)")
        column_values.append(df1[col_df1])
        column_names.append(f"{col_df2} (df2)")
        column_values.append(df2[col_df2])
    # Transpose the column_values list
    column_values_transposed = list(zip(*column_values))
    # Write column names and values to CSV file
    # with open('discrepancies.csv', 'w') as file:
    #     file.write(','.join(column_names) + '\n')
    #     for row in column_values_transposed:
    #         file.write(','.join(map(str, row)) + '\n')
    # print("Discrepancies data saved to discrepancies.csv")
    output_filename_correct = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(file1_path)}"
    output_path = f'{bucket_name}/Output/{output_filename_correct}'

    gcs = GCSFileSystem(project=f'{project_name}')
    with gcs.open(output_path, 'w') as file:
        file.write(','.join(column_names) + '\n')
        for row in column_values_transposed:
            file.write(','.join(map(str, row)) + '\n')


bucket_name = 'mybucket_hsbc'
file1_path = 'gs://mybucket_hsbc/db2_changed.csv'
file2_path = 'gs://mybucket_hsbc/BigQueryData.csv'
project_name = 'myprojecthsbc'
compareCSVwithDataFrame(file1_path, file2_path, project_name, bucket_name)
