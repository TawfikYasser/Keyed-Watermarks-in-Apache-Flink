import pandas as pd
import os

# Function to read and process each file
def process_file(file_path):
    # Extract the file name without the extension
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    # Create a DataFrame with the file content and add column names
    df = pd.read_csv(file_path, header=None)

    df.columns = ['Type', 'Watermark', 'SystemTime']    
    df['SystemTime'] = pd.to_datetime(df['SystemTime'])

    # Sort the DataFrame by the "Time" column
    df.sort_values(by="SystemTime", inplace=True)

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
# Now you have a dictionary where each key corresponds to a file name, and the value is the sorted DataFrame
# You can access each DataFrame using its file name as the key, for example: dataframes["flikn_50global_8c_start_latency.csv"]

# Merge similar files and calculate latency
print('Calculating latency...')
latency_dataframes = {}
for start_file_path in dataframes:
    if "start_latency" in start_file_path:
        end_file_path = start_file_path.replace("start_latency", "end_latency")
        if end_file_path in dataframes:
            start_df = dataframes[start_file_path].drop(columns=["Type"])
            end_df = dataframes[end_file_path].drop(columns=["Type"])

            # Group end_df by "Watermark" and get the max "SystemTime" for each group
            grouped_end_df = end_df.groupby("Watermark")["SystemTime"].max().reset_index()
            grouped_end_df.rename(columns={"SystemTime": "MaxSystemTime_End"}, inplace=True)

            # Merge start_df and grouped_end_df on "Watermark"
            merged_df = pd.merge(start_df, grouped_end_df, on="Watermark")

            # Calculate the latency as the difference between SystemTime_Start and MaxSystemTime_End for each Watermark
            merged_df["Latency"] = (merged_df["MaxSystemTime_End"] - merged_df["SystemTime"]).dt.total_seconds()

            # Drop redundant columns
            merged_df.drop(columns=["MaxSystemTime_End"], inplace=True)

            latency_dataframes[start_file_path] = merged_df

print('Latency Done')

print('Saving as CSV files...')

# Save each dataframe as a CSV file with the df name + "ready"
for file_path, df in latency_dataframes.items():
    df.to_csv(file_path.replace("_start_","_").replace(".csv", "_ready.csv"), index=False)

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

# Dictionary to store summary statistics and outliers for each dataframe
summary_statistics = {}

# Calculate summary statistics and outliers for each dataframe
for file_path, df in latency_dataframes.items():
    max_latency = df['Latency'].max()
    min_latency = df['Latency'].min()
    avg_latency = df['Latency'].mean()
    quantiles = df['Latency'].quantile([0.9, 0.95, 0.99])

    # Find outliers using the find_outliers function
    outliers = find_outliers(df['Latency'])

    # Store the summary statistics and outliers in the dictionary
    summary_statistics[file_path] = {
        'Experiment':file_path.replace(".csv","").replace("_start_","_"),
        'Max Latency': max_latency,
        'Min Latency': min_latency,
        'Avg Latency': avg_latency,
        '90th Quantile Latency': quantiles[0.9],
        '95th Quantile Latency': quantiles[0.95],
        '99th Quantile Latency': quantiles[0.99],
        'Outliers': outliers.tolist()  # Convert outliers to a list
    }


# Save each df summary results in a CSV file if stats is not empty
if summary_statistics:
    summary_data = []
    for file_path, stats in summary_statistics.items():
        summary_data.append(stats)
    df_summary = pd.DataFrame(summary_data)
    df_summary.to_csv("summary_results.csv", index=False)

print('Statistics are DONE')
