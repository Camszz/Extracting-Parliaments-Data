import json
import os
import pandas as pd

def read_config(selected_config: str, config_folder: str = "../config") -> dict:
    """
    Reads and merges JSON config files based on the selected configuration.
    
    The selected_config is expected to be a string in the format 'xxx_yyy_zzz',
    and the corresponding config files will be selected and merged according to the components
    of the input string, e.g., 'base', 'base_eu', 'base_eu_votes'.
    
    Args:
        selected_config (str): The name of the config file without the json extension (in the format 'xxx_yyy_zzz').
        config_folder (str): The folder containing the JSON config files. Defaults to "config".
    
    Returns:
        dict: A dictionary containing the merged content of the relevant config files.
    """
    
    # Split the base name into parts to determine which files to load
    config_parts = selected_config.split("_")
    
    # Initialize an empty dictionary to store the merged data
    merged_data = {}
    
    # Iterate through the parts and read the corresponding JSON files
    for i in range(1, len(config_parts) + 1):
        config_file = "_".join(config_parts[:i])  # Construct the file name (e.g., "base", "base_eu", etc.)
        file_path = os.path.join(config_folder, f"{config_file}.json")
        
        # Ensure the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Config file not found: {file_path}")
        
        # Read and merge the JSON data from the file
        with open(file_path, "r") as f:
            data = json.load(f)
            merged_data.update(data)
    
    return merged_data

def split_col(col, k):
    return pd.DataFrame(col.tolist(), columns=[f'keyword_{k}', f'prob_{k}'], index=col.index)

def keywords_convert(keywords, prob=False):

    """
    Converts the KeyBERT's output format into a DataFrame.
    
    Parameters:
    - keywords: List of tuples containing keywords and their scores
    """
    keywords = pd.DataFrame(keywords)

    new_res = []
    for col in keywords.columns:
        new_res.append(split_col(keywords[col], k=col))
    
    df = pd.concat(new_res, axis=1)

    if not prob:
        selected_cols = df.columns[df.columns.str.contains('prob')]
        df = df.drop(columns=selected_cols)

    return df

def save_dataframe_to_folder(df, folder_path, file_name):
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' created.")
    
    # Construct the file path
    file_path = os.path.join(folder_path, file_name)
    
    # Save the DataFrame to the folder
    df.to_csv(file_path, index=False)
    print(f"DataFrame saved to {file_path}")

