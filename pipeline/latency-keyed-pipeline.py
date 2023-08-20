import pandas as pd
import os

# Function to read and process each file
def process_file(file_path):
    # Extract the file name without the extension
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    # Create a DataFrame with the file content and add column names
    df = pd.read_csv(file_path)

    df.columns = ['Type', 'Watermark', 'Key', 'SystemTime']    
    df['SystemTime'] = pd.to_datetime(df['SystemTime'])

    return df

# List all the CSV files in the current directory
file_list = [file for file in os.listdir('.') if file.endswith('.csv')]

# Process each file and store the resulting DataFrames in a dictionary
print('Starting')
print('Preparing dictionary of dataframes...')
dataframes = {}
for file_path in file_list:
    df = process_file(file_path)
    dataframes[file_path] = df
print('Dataframes are ready')

# Merge similar files and calculate latency
print('Calculating latency...')
latency_dataframes = {}
for start_file_path in dataframes:
    if "_start_" in start_file_path:
        end_file_path = start_file_path.replace("_start_", "_end_")
        if end_file_path in dataframes:
            start_df = dataframes[start_file_path]
            end_df = dataframes[end_file_path]

            # Group start_df by "Watermark" and "Key" and get the min "SystemTime" for each group
            grouped_start_df = start_df.groupby(["Watermark", "Key"])["SystemTime"].min().reset_index()
            grouped_start_df.rename(columns={"SystemTime": "MinSystemTime_Start"}, inplace=True)

            # Group end_df by "Watermark" and "Key" and get the max "SystemTime" for each group
            grouped_end_df = end_df.groupby(["Watermark", "Key"])["SystemTime"].max().reset_index()
            grouped_end_df.rename(columns={"SystemTime": "MaxSystemTime_End"}, inplace=True)

            # Merge start_df and grouped_end_df on "Watermark" and "Key"
            merged_df = pd.merge(grouped_start_df, grouped_end_df, on=["Watermark", "Key"])

            # Calculate the latency as the difference between MinSystemTime_Start and MaxSystemTime_End for each group
            merged_df["Latency"] = (merged_df["MaxSystemTime_End"] - merged_df["MinSystemTime_Start"]).dt.total_seconds()

            # Drop redundant columns
            merged_df.drop(columns=["MinSystemTime_Start", "MaxSystemTime_End"], inplace=True)

            latency_dataframes[start_file_path] = merged_df

print('Latency Done')

print('Saving as CSV files...')

ready_for_stat = {}

# Save each dataframe as a CSV file with the df name + "ready"
for file_path, df in latency_dataframes.items():
    # Group by Watermark and calculate MaxLatency and MinLatency
    grouped_latency_dataframes = df.groupby("Watermark").agg({
        "Latency": ["max", "min"]
    })

    # Rename columns
    grouped_latency_dataframes.columns = ["MaxLatency", "MinLatency"]

    # Reset the index and save the DataFrame in the ready_for_stat dictionary
    ready_for_stat[file_path] = grouped_latency_dataframes.reset_index(inplace=True)
    # print(grouped_latency_dataframes)
    grouped_latency_dataframes.to_csv(file_path.replace("_start_", "_").replace(".csv", "_ready.csv"), index=False, columns=["Watermark", "MaxLatency", "MinLatency"])

print('CSVs are saved')

print('Calculating statistic...')

# Function to identify outliers based on the Interquartile Range (IQR) method
def find_outliers(series):
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return series[(series < lower_bound) | (series > upper_bound)]


# Create a list to store the summary_statistics for each DataFrame
summary_list = []

# Function to calculate summary statistics and outliers for a given series
def calculate_summary(series):
    max_val = series.max()
    min_val = series.min()
    avg_val = series.mean()
    quantiles = series.quantile([0.9, 0.95, 0.99])
    outliers = find_outliers(series)
    return max_val, min_val, avg_val, quantiles[0.9], quantiles[0.95], quantiles[0.99], outliers.tolist()

# Calculate summary statistics and outliers for each DataFrame in ready_for_stat
for file_path, df in ready_for_stat.items():
    summary_statistics = {}
    
    # Calculate summary statistics and outliers for MaxLatency
    max_latency_max, min_latency_max, avg_latency_max, q90_max, q95_max, q99_max, outliers_max = calculate_summary(df["MaxLatency"])
    
    # Calculate summary statistics and outliers for MinLatency
    max_latency_min, min_latency_min, avg_latency_min, q90_min, q95_min, q99_min, outliers_min = calculate_summary(df["MinLatency"])

    # Store the summary statistics and outliers in the dictionary
    summary_statistics = {
        'Experiment': file_path.replace(".csv", "").replace("_start_", "_"),
        'MaxLatency_Max': max_latency_max,
        'MaxLatency_Min': min_latency_max,
        'MaxLatency_Avg': avg_latency_max,
        'MaxLatency_90thQuantile': q90_max,
        'MaxLatency_95thQuantile': q95_max,
        'MaxLatency_99thQuantile': q99_max,
        'MaxLatency_Outliers': outliers_max,
        'MinLatency_Max': max_latency_min,
        'MinLatency_Min': min_latency_min,
        'MinLatency_Avg': avg_latency_min,
        'MinLatency_90thQuantile': q90_min,
        'MinLatency_95thQuantile': q95_min,
        'MinLatency_99thQuantile': q99_min,
        'MinLatency_Outliers': outliers_min
    }

    # Append the summary_statistics dictionary to the summary_list
    summary_list.append(summary_statistics)

# Convert the list of dictionaries to a DataFrame
df_summary = pd.DataFrame(summary_list)

# Save the DataFrame as a CSV file
# df_summary.to_csv("summary_results.csv", index=False)

print('Summary Done')
