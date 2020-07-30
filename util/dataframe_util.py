import pandas as pd

from util import logger

logger_obj = logger.create_logger()

def aggregate_df(df, group_param, aggr_param):
    """
    function to aggregate values based on group attributes
    :param df: dataframe on which operation is to be performed
    :param group_param: argument(s) on which output will be grouped
    :param aggr_param: argument that will be aggregated
    :return: aggregated dataframe
    """
    output_df = df
    if(len(aggr_param) == 0):
        logger_obj.error('Attribute to be summed is mandatory for the function')
    else:
        if (len(group_param) == 0):
            output_df = output_df.aggregate(aggr_param)
        else:
            output_df = output_df.groupby(group_param, axis=0).agg(aggr_param)

    logger_obj.info('Dataframe aggregated')

    # Reset dataframe index. Aggregation sets first column as index
    output_df = output_df.reset_index()
    return output_df

def join_df(df1, df2, merge_type, join_keys, col_names):
    """
    function to join two dataframes
    :param df1: dataframe 1
    :param df2: dataframe 2
    :param merge_type: can be left, right, inner, outer
    :param join_keys: key(s) on which to join dataframes
    :param col_names: column names of the final output dataframe
    :return: joined dataframe
    """
    merge_types = ['left', 'right', 'inner', 'outer']
    output_df = None

    #Validations
    if (df1.empty or df2.empty):
        return "Empty Dataframe"
    elif (len(join_keys) == 0):
        return "Please specify keys on which to join Dataframes"
    elif (merge_type not in merge_types):
        return "Please specify merge type"

    output_df = pd.merge(df1, df2, how=merge_type, on=join_keys, left_index=True)
    if(len(col_names) > 0):
        output_df.columns = col_names
    return output_df


def convert_df_to_json(object):
    """
    Convery an object to Json. In progress
    :param object: Object to be converted
    :return: Json format of object

    to-do: extend for other datatypes
    """

    if(isinstance(object, (pd.DataFrame)) == True):
        output = object.to_json(orient='records')
        return output

    return ('Function Convert_to_json does not work for data type: ' + str(type(object)))


def add_total_df(df, aggr_param = ['sum'], total_type = 'row'):
    """
    function to calculate total of a dataframe and append it to the end
    :param df: original dataframe
    :param aggr_param: list parameters on which total is to be calculated. Should be empty for normal total
    :param total_type: total is needed for rows or columns or both
    :return: final dataframe with total rows/columns or both
    """
    output_df = None
    total_types = ['row', 'column', 'both']
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']

    if(total_type == 'row'):
        # Total at the last row with aggregate  values of each column
        total_row = df.select_dtypes(include=numerics).aggregate(aggr_param, axis=0)
        output_df = df.append(total_row, ignore_index=True)

        for col in output_df.select_dtypes(exclude=numerics).columns:
            output_df.loc[output_df.index[-1], col] = 'Total'
        return output_df
    elif(total_type == 'column'):
        # Total at the last column with aggregate values of each row
        print('in col')
        total_column = df.select_dtypes(include=numerics).agg(aggr_param, axis=1)
        output_df = df
        output_df['Total'] = total_column
        return output_df
    else:
        # Total at last row as well as last column
        print('in both')
        total_row = df.select_dtypes(include=numerics).aggregate(aggr_param, axis=0)

        output_df = df.append(total_row, ignore_index=True)
        for col in output_df.select_dtypes(exclude=numerics).columns:
            output_df.loc[output_df.index[-1], col] = 'Total'

        total_column = output_df.select_dtypes(include=numerics).aggregate(aggr_param, axis=1)
        output_df['Total'] = total_column
        return output_df


#
#
# data = {'Product': ['Desktop','Tablet','iPhone','Laptop'],
#         'Type': ['t1', 't1', 't2', 't2'],
#         'Price': [700, 250, 800, 1200],
#         'Qty': [200, 500, 150, 600]
#         }
# df = pd.DataFrame(data, columns= ['Product', 'Type', 'Price', 'Qty'])
# # print(df)
#
#
# d = add_total_df(df, 'sum', 'both')
# print(d)





# data = {'Product': ['Desktop','Tablet','iPhone','Laptop'],
#         'Price': [700,250,800,1200]
#         }
#
# df = pd.DataFrame(data, columns= ['Product', 'Price'])
#
# print (df)
#
# o = convert_to_json(df)
#
# print(o)