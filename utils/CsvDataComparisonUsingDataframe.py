import os
import pandas as pd
from datetime import datetime


def compare_and_generate_output(csv1_path, csv2_path, output_format):
    # Read CSV or Excel files
    if csv1_path.lower().endswith('.csv'):
        df1 = pd.read_csv(csv1_path)
    else:
        df1 = pd.read_excel(csv1_path)

    if csv2_path.lower().endswith('.csv'):
        df2 = pd.read_csv(csv2_path)
    else:
        df2 = pd.read_excel(csv2_path)

    # Read CSV files
    # df1 = pd.read_csv(csv1_path)
    # df2 = pd.read_csv(csv2_path)

    # Initialize an empty list to store rows
    rows = []
    report = [f"No of rows in Source file(DB2) : {len(df1)}", f"No of rows in Target file(BigQuery) : {len(df2)}"]
    # Iterate through each row and compare values
    count = 0
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
                    count = count + 1
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
    report.append(f"Discrepancy found in {count} rows")
    discrepancies_df = pd.DataFrame(rows)
    #

    # Determine the output file extension
    if output_format.lower() == 'csv':
        output_filename = f"Output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    elif output_format.lower() == 'xls':
        output_filename = f"Output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls"
    elif output_format.lower() == 'xlsx':
        output_filename = f"Output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    else:
        raise ValueError("Invalid output format. Supported formats: csv, xls, xlsx")

    output_path = f'gs://{bucket_name}/Output/{output_filename}'

    # Write output to the specified format
    if output_format.lower() == 'csv':
        discrepancies_df.to_csv(output_path, index=False)
    elif output_format.lower() == 'xls':
        discrepancies_df.to_excel(output_path, index=False)
    elif output_format.lower() == 'xlsx':
        discrepancies_df.to_excel(output_path, index=False)
    # output_filename_correct = f"Output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    # output_path = f'gs://{bucket_name}/Output/{output_filename_correct}'
    #
    # # Write output to CSV
    # discrepancies_df.to_csv(output_path, index=False)

    # df = pd.read_csv(output_path)
    #
    # pd.options.display.max_columns = len(df.columns)
    report.append(f"Reports uploaded at GCS location and path :  {output_path}")

    for line in report:
        print(line)


# Example usage
# bucket_name = 'dumybucket123'
# DB2 = 'gs://dumybucket123/DB2/file_example_XLS_100.xls'
# BigQuery = 'gs://dumybucket123/Bigquery/file_example_XLS_100error.xls'
# output_path = f'gs://dumybucket123'
# compare_and_generate_output(DB2, BigQuery,output_path,output_format='Csv')
bucket_name = 'mybucket_hsbc'
file1_path = 'gs://mybucket_hsbc/db2_changed.csv'
file2_path = 'gs://mybucket_hsbc/BigQueryData.csv'
output_path = f'gs://mybucket_hsbc/Output/Discrepancy_Output.csv'
# compare_files(file1_path,file2_path,bucket_name)
compare_and_generate_output(file1_path, file2_path, output_format='Csv')
