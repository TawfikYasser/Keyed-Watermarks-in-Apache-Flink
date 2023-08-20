import matplotlib.pyplot as plt
import pandas as pd
import os

dicdataset = {
    "5global":r"5% of data globally delayed",
    "20median":r"20% of the keyes around the median are delayed",
    "50global":r"50% of the data globally delayed",
    "50median":r"50% of the keyes around the median are delayed",
    "20random":r"20% of randomly chosen keyes are delayed",
    "50random":r"50% of randomly chosen keyes are delayed",
    "debs":r"DEBS Dataset",
    "ordered":r"Fully Ordered"
}

# Get a list of all CSV files in the current directory
csv_files = [file for file in os.listdir('.') if file.endswith('.csv')]

# Loop through the CSV files and plot each one as a figure
for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    csv_file = csv_file.replace('_state_result.csv.csv', '')

    # Sort the DataFrame by 'TS' column
    df = df.sort_values(by='TS')
    # Filter the DataFrame to keep rows with TS <= 700000
    if csv_file != 'debs':
        df = df[df['TS'] <= 700000]
    print(df)

    # Calculate the difference between State_Size_Flink and State_Size_Keyed
    df['State_Size_Difference'] = df['State_Size_Keyed'] - df['State_Size_Flink']

    # Create a figure for the line plot
    plt.figure(figsize=(12, 6))

    plt.plot(df['TS'], df['State_Size_Difference'], color='green', label='Above zero means Keyed state is higher, below zero means Flink state is higher')
    # plt.grid(True)
    plt.xlabel('Timestamps')
    plt.ylabel('State Size Difference')
    # plt.title(f'State Size Difference (Flink Vs. Keyed) - Dataset: {dicdataset[csv_file]}')
    plt.legend()
    # Show the plot
    # plt.show()
    # Save the figure to a file
    plt.savefig(f'{csv_file}_graph.png', bbox_inches='tight')
