"""
This is a boilerplate pipeline 'API_database_ingestion_pipeline'
generated using Kedro 0.19.1
"""

import pandas as pd 

from typing import Dict, List
import requests


def API_database_ingestion_pipeline(api_response: requests.models.Response,
                                    api_error_dict: Dict,
                                    db_column_names: List[str],
                                    directions_to_consider: List[str]) -> pd.DataFrame:
    """
    Main method to call all transformation steps of the data pipeline.
    API Call defined by URL in data catalog.

    :param api_response: API response 
    :param api_error_dict Dict: Dictionary with all error codes from documentation
    :return: transformed data in pandas DataFrame format
    """

    # call API and validate response
    api_response = call_API(api_response, api_error_dict)

    # get next depart and convert to relational db format
    relational_data = get_next_depart_and_convert_format(db_column_names, api_response)

    # prefilter to right directions
    output_data = prefilter_for_direction(relational_data, directions_to_consider)

    return output_data

def call_API(api_response: requests.models.Response,
             api_error_dict: Dict) -> Dict:
    """
    Check if Call was sucessfull and if not which error it is.

    :param api_response: API response 
    :param api_error_dict Dict: Dictionary with all error codes from documentation
    :return: if sucessfull call -> API response
    :raises ValueError: If response of API is not 200 -> Raise error according to return code.
    """    

    # Check if status of query was successfull
    if api_response.status_code == 200:
        print(f"API call for successful!")
        return api_response.json()

    elif api_response.status_code in api_error_dict.keys():
        raise ValueError(f"API call not successful due to Wiener Linien internal defined error! Reason {api_response.status_code}: {api_error_dict[api_response.status_code]}") 

    else:
        raise ValueError(f"API call not successful and status code not defined by to Wiener Linien! Reason {api_response.status_code}")
    

def get_next_depart_and_convert_format(db_column_names: List[str],
                                       api_response: Dict) -> pd.DataFrame:
    """
    Iterate through all stops and get the next line which is going to depart from each stop

    :params df_schema List[str]: List of strings with the column names
    :params api_response Dict: json response from the API
    :returns: dataframe with every next depature per stop
    """
    
    
    return_df = pd.DataFrame()
    timestamp = pd.json_normalize(api_response)['message.serverTime'].values[0]

    for num, stop in enumerate(api_response['data']['monitors']):
        # get stop attributes
        stop_normalized = pd.json_normalize(api_response['data']['monitors'][num])
        stop_name = stop_normalized['locationStop.properties.title'].values[0]
        rbl = stop_normalized['locationStop.properties.attributes.rbl'].values[0]

        # get lines attributes
        lines_normalized = pd.json_normalize(stop_normalized['lines'].values[0])
        lineID = lines_normalized['lineId'].values[0]
        directionID = lines_normalized['richtungsId'].values[0]

        # get realtime information
        depature_information =pd.json_normalize(lines_normalized['departures.departure'].values[0])
        time_planned = depature_information['departureTime.timePlanned'].values[0]
        time_real = depature_information['departureTime.timeReal'].values[0]

        single_entry = pd.DataFrame(columns = db_column_names,
                                    data = [[timestamp, 
                                             stop_name, 
                                             rbl, 
                                             lineID, 
                                             directionID, 
                                             time_planned, 
                                             time_real]])
        return_df = pd.concat([return_df, single_entry])

        # convert datatypes
        for col in ['API_ServerTime', 'TimePlanned', 'TimeReal']:
            return_df[col] = pd.to_datetime(return_df[col])
        
    return return_df

def prefilter_for_direction(api_response_df: pd.DataFrame,
                            directions_to_consider: List[str]) -> pd.DataFrame:
    """
    Prefilter Dataframe to only to consider wanted directions

    :param api_response_df pd.DataFrame: API response in pandas Dataframe Format
    :param directions_to_consider List[str]: List of strings for all Directions we want to consider
    :returns pd.DataFrame: Prefiltered pandas Dataframe with API response
    """

    api_response_df = api_response_df.loc[api_response_df['Direction'].astype(str).isin(directions_to_consider)]
    return api_response_df
