import json
import os

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