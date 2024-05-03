import os
from datetime import datetime

import pandas as pd


def compare_and_generate_output(csv1_path, csv2_path, bucket_name):
    # Read CSV files
    df1 = pd.read_csv(csv1_path)
    df2 = pd.read_csv(csv2_path)

    # Initialize an empty list to store rows
    rows = []

    # Iterate through each row and compare values
    for index, row in df1.iterrows():
        flag = False
        # Check if the row index exists in df2
        if index < len(df2):
            # Compare values for each column
            discrepancy_row = {}
            for column in df1.columns:
                if row[column] != df2.iloc[index][column]:
                    if pd.isna(row[column]) and pd.isna(df2.iloc[index][column]):
                        break
                    discrepancy_row[f"{column}_DB2"] = row[column]
                    discrepancy_row[f"{column}_BigQuery"] = df2.iloc[index][column]
                    flag = True
                   # rows.append(discrepancy_row)

                else:
                    discrepancy_row[f"{column}_DB2"] = row[column]
                    discrepancy_row[f"{column}_BigQuery"] = ""

            # Append the discrepancy row to rows list if discrepancies found
            if flag:
                rows.append(discrepancy_row)

        else:
            # Add remaining rows from csv1 to rows
            discrepancy_row = {f"{column}_DB2": row[column] for column in df1.columns}
            discrepancy_row.update({f"{column}_BigQuery": "" for column in df2.columns})
            rows.append(discrepancy_row)

    # Create DataFrame from the list of rows
    discrepancies_df = pd.DataFrame(rows)


    output_filename_correct = f"Output_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_path = f'gs://{bucket_name}/Output/{output_filename_correct}'

    # Write output to CSV
    discrepancies_df.to_csv(output_path, index=False)
    df = pd.read_csv(output_path)
    pd.options.display.max_columns = len(df.columns)
   # print(df)


# Example usage
# file1 = 'gs://mybucket_hsbc/db2_changed.csv'
# file2 = 'gs://mybucket_hsbc/BigQueryData.csv'
# compare_and_generate_output(file1, file2,'mybucket_hsbc')
