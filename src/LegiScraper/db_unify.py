import pandas as pd
from time import sleep
import ast
import logging

# Set up the logger
logger = logging.getLogger(__name__)

def process_MUKPsRaw(df_MUKPs_raw: pd.DataFrame):
    """
    Process the raw MUKPs DataFrame to clean and format the data.

    Parameters:
    df_MUKPs_raw (pd.DataFrame): The raw DataFrame containing MUKPs data.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """

    # We exclude the Chamber of the Lords from the dataset, as its power is highly limited.
    df_MUKPs_raw.drop(df_MUKPs_raw[df_MUKPs_raw['latestHouseMembership_house'] == 2].index, inplace=True)

    # Select relevant columns
    df_MUKPs = df_MUKPs_raw[['id', 'nameListAs', 'gender', 'latestParty_name', 'latestParty_abbreviation',
                             'latestHouseMembership_membershipStartDate', 'latestHouseMembership_membershipEndDate',
                             'latestHouseMembership_membershipStatus_statusIsActive']]

    # Rename columns for clarity
    names = {
        'latestParty_name': 'politicalGroup',
        'latestParty_abbreviation': 'politicalGroup_short',
        'latestHouseMembership_membershipStartDate': 'memberSince',
        'latestHouseMembership_membershipEndDate': 'memberUntil',
        'latestHouseMembership_membershipStatus_statusIsActive': 'isActive'
    }
    df_MUKPs = df_MUKPs.rename(names, axis=1)

    # Format name string to make it compatible with other datasets
    df_MUKPs[['lastName', 'firstName']] = df_MUKPs['nameListAs'].str.split(',', expand=True)
    df_MUKPs.drop('nameListAs', axis=1, inplace=True)

    # Fill missing values in the 'isActive' column with False
    df_MUKPs['isActive'].fillna(False, inplace=True)

    # Remove extra precision from the memberSince and memberUntil columns
    df_MUKPs['memberSince'] = pd.to_datetime(df_MUKPs['memberSince'])
    df_MUKPs['memberSince'] = df_MUKPs['memberSince'].dt.strftime("%Y-%m-%d")
    df_MUKPs['memberUntil'] = pd.to_datetime(df_MUKPs['memberUntil'])
    df_MUKPs['memberUntil'] = df_MUKPs['memberUntil'].dt.strftime("%Y-%m-%d")

    # Properly highlight Lord names, if there is still
    df_MUKPs['firstName'][df_MUKPs_raw['nameDisplayAs'].str.contains('Lord')] = 'Lord'
    df_MUKPs['parliament'] = 'UK'

    return df_MUKPs

def process_MEUPsRaw(df_MEUPs_raw: pd.DataFrame):
    """
    Process the raw MEUPs DataFrame to clean and format the data.

    Parameters:
    df_MEUPs_raw (pd.DataFrame): The raw DataFrame containing MEUPs data.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """

    df_MEUPs = df_MEUPs_raw

    # Rename columns for clarity
    name = {
        'member_since': 'memberSince',
        'member_until': 'memberUntil',
        'first_name': 'firstName',
        'last_name': 'lastName',
        'eu-parl-group': 'politicalGroup_short',
        # 'bday': 'birthday',
        'country-representation': 'countryRepresentation',
    }
    df_MEUPs = df_MEUPs.rename(columns=name)

    # Set the 'isActive' column to True
    df_MEUPs['isActive'] = True
    df_MEUPs['isActive'] = df_MEUPs['isActive'].astype(bool)

    # Add the 'parliament' column
    df_MEUPs['parliament'] = 'EP'

    # Select relevant columns
    # df_MEUPs = df_MEUPs[['parliament', 'id', 'firstName', 'lastName', 'politicalGroup_short', 'memberSince',
    #                      'memberUntil', 'isActive', 'gender', 'birthday', 'citizenship', 'countryRepresentation']]
    
    df_MEUPs = df_MEUPs[['parliament', 'id', 'firstName', 'lastName', 'politicalGroup_short', 'memberSince',
                         'memberUntil', 'isActive', 'gender', 'citizenship', 'countryRepresentation']]
    

    # Standardize gender values
    df_MEUPs['gender'][df_MEUPs['gender'] == 'MALE'] = 'M'
    df_MEUPs['gender'][df_MEUPs['gender'] == 'FEMALE'] = 'F'

    return df_MEUPs

def process_MPsRaw(dicf_MPs_raw: dict[pd.DataFrame]):
    """
    Process the raw MPs DataFrame to clean and format the data.

    Parameters:
    dicf_MPs_raw (dict[pd.DataFrame]): The raw dictionary containing DataFrames of MPs data.

    Returns:
    None
    """

    logger.info('Processing MPs')
    list_df_MPs = []
    for org, df in dicf_MPs_raw.items():
        list_df_MPs.append(globals()[f'process_M{org.upper()}PsRaw'](df))
    pd.concat(list_df_MPs).to_csv("../data/output/postprocess/MPs_full.csv", index=False)
    logger.info('MPs processed')
    return

def process_UKVotesRaw(df_UKVotes_raw: pd.DataFrame):
    """
    Process the raw UKVotes DataFrame to clean and format the data.

    Parameters:
    df_UKVotes_raw (pd.DataFrame): The raw DataFrame containing UKVotes data.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """

    df_UKVotes = df_UKVotes_raw[['DivisionId', 'Title', 'Date', 'keyword_0', 'keyword_1', 'keyword_2',
                                 'topic_1', 'topic_2', 'AyeCount', 'NoCount']]

    # Rename columns for clarity
    names = {
        'DivisionId': 'vote_id',
        'Title': 'title',
        'Date': 'timestamp',
        'AyeCount': 'ForCount',
        'NoCount': 'AgainstCount'
    }
    df_UKVotes = df_UKVotes.rename(names, axis=1)

    # Add the 'parliament' column
    df_UKVotes['parliament'] = 'UK'

    return df_UKVotes

def process_EUVotesRaw(df_EUVotes_raw: pd.DataFrame):
    """
    Process the raw EUVotes DataFrame to clean and format the data.

    Parameters:
    df_EUVotes_raw (pd.DataFrame): The raw DataFrame containing EUVotes data.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """

    names = {
        'id': 'vote_id',
        'display_title': 'title'
    }
    df_EUVotes = df_EUVotes_raw.rename(names, axis=1)

    # Add the 'parliament' column
    df_EUVotes['parliament'] = 'EU'

    return df_EUVotes

def process_VotesRaw(dicf_votes_raw: dict[pd.DataFrame]):
    """
    Process the raw Votes DataFrame to clean and format the data.

    Parameters:
    dicf_votes_raw (dict[pd.DataFrame]): The raw dictionary containing DataFrames of votes data.

    Returns:
    None
    """

    logger.info('Processing votes')
    list_df_votes = []
    for org, df in dicf_votes_raw.items():
        list_df_votes.append(globals()[f'process_{org.upper()}VotesRaw'](df))
    pd.concat(list_df_votes).to_csv("../data/output/postprocess/votes.csv", index=False)
    logger.info('Votes processed')
    return

def process_UKMemberVotesRaw(df_UKVotes_raw: pd.DataFrame):
    """
    Process the raw UKMemberVotes DataFrame to clean and format the data.

    Parameters:
    df_UKVotes_raw (pd.DataFrame): The raw DataFrame containing UKMemberVotes data.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """

    df_for = df_UKVotes_raw[['DivisionId', 'AyeTellers']]
    df_for['AyeTellers'] = df_for['AyeTellers'].dropna().apply(lambda x: ast.literal_eval(x))
    df_for = df_for.explode('AyeTellers', ignore_index=True)
    df_normalized = pd.json_normalize(df_for['AyeTellers'])
    df_for = pd.concat([df_for.drop(columns=['AyeTellers']), df_normalized], axis=1)
    df_for.dropna(subset='MemberId', inplace=True)
    df_for['MemberId'] = df_for['MemberId'].astype(int)
    df_for = df_for[['DivisionId', 'MemberId']]
    df_for.rename(columns={'MemberId': 'MP_id', 'DivisionId': 'vote_id'}, inplace=True)
    df_for['position'] = 'FOR'

    df_against = df_UKVotes_raw[['DivisionId', 'NoTellers']]
    df_against['NoTellers'] = df_against['NoTellers'].dropna().apply(lambda x: ast.literal_eval(x))
    df_against = df_against.explode('NoTellers', ignore_index=True)
    df_normalized = pd.json_normalize(df_against['NoTellers'])
    df_against = pd.concat([df_against.drop(columns=['NoTellers']), df_normalized], axis=1)
    df_against.dropna(subset='MemberId', inplace=True)
    df_against['MemberId'] = df_against['MemberId'].astype(int)
    df_against = df_against[['DivisionId', 'MemberId']]
    df_against.rename(columns={'MemberId': 'MP_id', 'DivisionId': 'vote_id'}, inplace=True)
    df_against['position'] = 'AGAINST'

    df = pd.concat([df_for, df_against])
    df['parliament'] = 'UK'

    return df

def process_EUMemberVotesRaw(df_EUVotes_raw: pd.DataFrame):
    """
    Process the raw EUMemberVotes DataFrame to clean and format the data.

    Parameters:
    df_EUVotes_raw (pd.DataFrame): The raw DataFrame containing EUMemberVotes data.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """

    df_EUVotes = df_EUVotes_raw.rename(columns={'vote-id': 'vote_id', 'member-id': 'MP_id'})
    df_EUVotes['parliament'] = 'EU'

    return df_EUVotes

def process_MemberVotesRaw(dicf_MemberVotes_raw: dict[pd.DataFrame]):
    """
    Process the raw MemberVotes DataFrame to clean and format the data.

    Parameters:
    dicf_MemberVotes_raw (dict[pd.DataFrame]): The raw dictionary containing DataFrames of MemberVotes data.

    Returns:
    None
    """

    logger.info('Processing votes')
    list_df_votes = []
    for org, df in dicf_MemberVotes_raw.items():
        list_df_votes.append(globals()[f'process_{org.upper()}MemberVotesRaw'](df))
    pd.concat(list_df_votes).to_csv("../data/output/postprocess/memberVotes.csv", index=False)
    logger.info('Votes processed')
    return
