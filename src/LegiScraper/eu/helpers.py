import pandas as pd    
import os


def get_mandate(data):

    mp_mandate = pd.json_normalize(data['hasMembership'])
    mp_eu_mandate = mp_mandate[mp_mandate['membershipClassification'] == 'def/ep-entities/EU_INSTITUTION']

    member_since = mp_eu_mandate['memberDuring.startDate'].min()
    last_membership = mp_eu_mandate['memberDuring.startDate'].max()
    
    if 'memberDuring.endDate' in mp_eu_mandate.columns:
        member_until = mp_eu_mandate['memberDuring.endDate'].dropna().max()
    else:
        member_until = None

    if member_until == None:
        return member_since, member_until
    else:
        if pd.to_datetime(member_until) < pd.to_datetime(last_membership):
            member_until = None

    return member_since, member_until


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