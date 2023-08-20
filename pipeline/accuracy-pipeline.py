import pandas as pd
import os
directory = '<path-of-dir-containing-flink-job-results>'

values = {}
# Iterate over the files in the directory
for filename in os.listdir(directory):
    file_path = os.path.join(directory, filename)
    print(file_path)
    df = pd.read_csv(file_path)
    df.columns = ['W_S', 'Watermark', 'W_K', 'W_E', 'Elements']
    values[filename.split('.')[0]] = df['Elements'].sum()

# Convert the values dictionary to a DataFrame
values_df = pd.DataFrame(values.items(), columns=['Experiment', 'ProcessedElements'])

# Save the values DataFrame to a CSV file
values_df.to_csv('<output-file-name>', index=False)
