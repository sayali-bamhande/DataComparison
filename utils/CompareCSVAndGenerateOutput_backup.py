import os
from datetime import datetime

import pandas as pd


def compare_and_generate_output(csv1_path, csv2_path, bucket_name, output_format):
    # Read CSV files
    if csv1_path.lower().endswith('.csv'):
        df1 = pd.read_csv(csv1_path)
    else:
        df1 = pd.read_excel(csv1_path)

    if csv2_path.lower().endswith('.csv'):
        df2 = pd.read_csv(csv2_path)
    else:
        df2 = pd.read_excel(csv2_path)

    # df1 = pd.read_csv(csv1_path)
    # df2 = pd.read_csv(csv2_path)

    # Initialize an empty list to store rows
    rows = []
    report = [f"No of rows in Source file(DB2) : {len(df1)}", f"No of rows in Target file(BigQuery) : {len(df2)}"]
    # Iterate through each row and compare values
    count = 0
    row_num=0
    i = 1
    for index, row in df1.iterrows():
        temp = ''
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
                    temp = temp + f'{column} ,'
                    # report.append(f"  {i}  | {column}")
                    flag = True
                    count = count + 1
                # rows.append(discrepancy_row)

                else:
                    discrepancy_row[f"{column}_DB2"] = row[column]
                    discrepancy_row[f"{column}_BigQuery"] = ""
            if temp != '':
                report.append(f"{i} | {row[0]} --> {temp}")
                row_num = row_num + 1
            i = i + 1

            # Append the discrepancy row to rows list if discrepancies found
            if flag:
                rows.append(discrepancy_row)

        else:
            # Add remaining rows from csv1 to rows
            discrepancy_row = {f"{column}_DB2": row[column] for column in df1.columns}
            discrepancy_row.update({f"{column}_BigQuery": "" for column in df2.columns})
            rows.append(discrepancy_row)

    # Create DataFrame from the list of rows
    report.append(f"Discrepancy found in {row_num} rows")
    report.append(f"Discrepancy found at {count} places")

    discrepancies_df = pd.DataFrame(rows)

    # Determine the output file extension
    if output_format.lower() == 'csv':
        output_filename = f"Output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    elif output_format.lower() == 'xls':
        output_filename = f"Output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls"
    elif output_format.lower() == 'xlsx':
        output_filename = f"Output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    else:
        raise ValueError("Invalid output format. Supported formats: csv, xls, xlsx")

    # output_filename_correct = f"Output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    output_path = f'gs://{bucket_name}/Output/{output_filename}'

    # Write output to CSV
    discrepancies_df.to_csv(output_path, index=False)

    if output_format.lower() == 'csv':
        discrepancies_df.to_csv(output_path, index=False)
    elif output_format.lower() == 'xls':
        discrepancies_df.to_excel(output_path, index=False)
    elif output_format.lower() == 'xlsx':
        discrepancies_df.to_excel(output_path, index=False)

    df = pd.read_csv(output_path)
    pd.options.display.max_columns = len(df.columns)
    report.append(f"Reports uploaded at GCS location and path :  {output_path}")

    for line in report:
        print(line)

# print(df)


# Example usage
file1 = 'gs://mybucket_hsbc/db2_changed.csv'
file2 = 'gs://mybucket_hsbc/BigQueryData.csv'
# file1 = 'C:\\Users\\Sayali.Bamhande\\Downloads\\xl\\db2_changed.csv'
# file2 = 'C:\\Users\\Sayali.Bamhande\\Downloads\\xl\\BigQueryData.csv'
compare_and_generate_output(file1, file2, 'mybucket_hsbc', "csv")
