import pandas as pd

from util import logger

logger_obj = logger.create_logger()

def extract_data_from_DB(query, dao_object, *query_params):
    """
    function to extract data from DB
    :param query: query for which data is needed
    :param dao_object: to extract data from DB
    :param query_params: list of variable values to be passed in query
    :return: dataframe with data from DB
    """

    local_query = None

    if(len(query_params) == 0):
        local_query = query
    else:
        local_query = query % query_params

    #print(local_query)

    # Extract data
    #output_df = 0
    output_df = pd.DataFrame(dao_object.get(local_query))
    column_names = dao_object.get_column_name()
    output_df.columns = column_names

    return output_df


#def write_data_to_parquet(file_path, file_name, write_type, dataframe):
