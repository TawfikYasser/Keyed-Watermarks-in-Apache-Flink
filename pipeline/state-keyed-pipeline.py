import pandas as pd
import os

# Function to read and process each file
def process_file(file_path):
    # Extract the file name without the extension
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    print(file_name, 'is Done')

    # Create a DataFrame with the file content and add column names
    df = pd.read_csv(file_path)

    df.columns = ['Key', 'TS', 'Size']    
    df['Size'] = df['Size'].astype(int)
    df2 = df.groupby(['TS']).sum('Size')
    df2.reset_index(inplace=True)
    df2 = df2.drop(columns=['Key'])
    df2.to_csv(f'{file_name}_result.csv')
    return df2

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

print(dataframes['keyed_50median_state.csv'])
