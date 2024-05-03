import pandas as pd

# Load the CSV files into data frames

df1 = pd.read_csv('C:\\Users\\Sayali.Bamhande\\IdeaProjects\\BigData_Test\\BigQueryData.csv')
df2 = pd.read_csv('C:\\Users\\Sayali.Bamhande\\IdeaProjects\\BigData_Test\\db2_changed.csv')

# Initialize lists to store column names and values
column_names = []
column_values = []

# Extract column names for df1
for col_df1 in df1.columns:
    column_names.append(f"{col_df1} (df1)")

    # Fill in values from df1
    column_values.append(df1[col_df1])

    # Check for discrepancies and fill in values from df2 if discrepancy is found
    for col_df2 in df2.columns:
        if col_df1 == col_df2:
            for value_df1, value_df2 in zip(df1[col_df1], df2[col_df2]):
                if value_df1 != value_df2:
                    column_names.append(f"{col_df2} (df2)")
                    column_values.append(df2[col_df2])
                    break
            else:
                # If no discrepancy is found, keep df2 values blank
                column_names.append(f"{col_df2} (df2)")
                column_values.append([''] * len(df2[col_df2]))

# Transpose the column_values list
column_values_transposed = list(zip(*column_values))

# Write column names and values to CSV file
with open('discrepancies.csv', 'w') as file:
    file.write(','.join(column_names) + '\n')
    for row in column_values_transposed:
        file.write(','.join(map(str, row)) + '\n')

print("Discrepancies data saved to discrepancies.csv")