from dao import data_dao
from util import util_functions, logger, dataframe_util
from config import query_config, master_config

import pandas as pd
import numpy as np


class Main:
    logger_obj = None
    input_data_df = None

    def __init__(self):
        self.logger_obj = logger.create_logger()

    def extract_data(self,start_datetime, end_datetime):

        d_dao = None

        try:
            # Initialize DataDao class object to link to DB
            #d_dao = data_dao.DataDao()

            self.logger_obj.info('Initiate Payout Level Data Extraction')
            query = query_config.get_query('payout_level_info')
            #print(query)

            #self.input_data_df = util_functions.extract_data_from_DB(query, d_dao, start_datetime, end_datetime)
            #print(self.input_data_df)

            # ******************************************************************************************
            # ******************************************************************************************
            # import data from csv as dB data cannot be accessed in local
            self.logger_obj.info('Read data from csv')
            self.input_data_df = pd.read_csv('Sample_results.csv', sep =',')
            #print(self.input_data_df)
            # ******************************************************************************************
            # ******************************************************************************************




        except Exception as Ex:
            self.logger_obj.error('Exception: ' + str(Ex))

        finally:
            self.logger_obj.info('Close DataDao object')
            if d_dao is not None:
                d_dao.close()

    def process_data(self, start_datetime, end_datetime):
        # steps
        # 1. minute bucket and TAT for each transaction
        # 2. For each minute bucket, mode & channel combo, find:
        # 2.1 # transactions
        # 2.2 # Successful transactions
        # 2.3 # Failed transactions
        # 2.4 # Uptime processing time transactions
        # 2.5 # Degrade processing time transactions
        # 2.6 # Downtime processing time transactions

        self.extract_data(start_datetime, end_datetime)
        #print(self.input_data_df[0:50].to_string())

        #Convert all relevant timestamps from string to datetime
        self.logger_obj.info('Convert all relevant timestamps from string to datetime')
        self.input_data_df['initiated_at'] = pd.to_datetime(self.input_data_df['initiated_at'], format='%Y/%m/%d %H:%M:%S')
        # errors = 'coerce' is to handle NaN values
        self.input_data_df['finished_at'] = pd.to_datetime(self.input_data_df['finished_at'],
                                                           format='%Y/%m/%d %H:%M:%S', errors='coerce')

        # Add minute_interval column to dataframe
        self.logger_obj.info('Add minute interval column to dataframe')
        #self.input_data_df['finished_date'] = self.input_data_df['finished_at'].dt.date
        #self.input_data_df['hour'] = self.input_data_df['finished_at'].apply(lambda x: x.hour)
        self.input_data_df['minute_bucket'] = self.input_data_df['initiated_at'].apply(lambda x:
                                                x.replace(minute=45, second=00) if x.minute >= 45
                                                else x.replace(minute=30, second=00) if x.minute >= 30
                                                else x.replace(minute=15, second=00) if x.minute >= 15
                                                else x.replace(minute=00, second=00))
        #print(self.input_data_df[0:50].to_string())

        # Calculate TAT in seconds for each transaction
        self.logger_obj.info('Calculate TAT in seconds for each transaction')
        self.input_data_df['tat'] = (self.input_data_df['finished_at'] - self.input_data_df['initiated_at']).dt.seconds
        #print(self.input_data_df[0:50].to_string())

        # Identify Processing TAT Status for each transaction
        self.logger_obj.info('Identify Processing TAT Status for each transaction')
        self.input_data_df['tat_status'] = self.input_data_df['tat'].apply(lambda x: 'Uptime' if x < 900
                                                                                    else 'Degrade' if x < 1800
                                                                                    else 'Downtime' if x >= 1800
                                                                                    else 'In Progress')
        #print(self.input_data_df[0:50].to_string())

        # Mark all NEFT and RTGS Transactions as NEFT/RTGS
        self.logger_obj.info('Mark all NEFT and RTGS Transactions as NEFT/RTGS')
        self.input_data_df['tat_status'] = np.where((self.input_data_df['mode'] == 'RTGS'), 'NEFT/RTGS',
                                                       self.input_data_df['tat_status'])
        self.input_data_df['tat_status'] = np.where((self.input_data_df['mode'] == 'NEFT'), 'NEFT/RTGS',
                                                       self.input_data_df['tat_status'])
        #print(self.input_data_df[0:50].to_string())

        # Add a column Freq with all 1s. Will be useful to count # transactions
        self.input_data_df['freq'] = 1

        # Create intermediate Dataframe to capture #Transactions,  # Successful and failed transactions for each bucket
        self.logger_obj.info('Calculate #transactions and #successful transactions for each interval')
        df1 = self.input_data_df.pivot_table(index=['mode', 'channel', 'minute_bucket'], columns='status',
                                             values='freq', aggfunc=np.sum, margins=True)
        df1 = df1.reset_index()
        #print(df1[0:50].to_string())
        # keep only processed transactions and total transactions column
        df1 = df1[['mode', 'channel', 'minute_bucket', 'processed', 'All']]
        #print(df1[0:50].to_string())

        # Create intermediate dataframe to capture # transactions as per processing time for each bucket
        self.logger_obj.info('Calculate uptime, degrade, in-progress and downtime transactions for each interval')
        df2 = self.input_data_df.pivot_table(index=['mode', 'channel', 'minute_bucket'], columns='tat_status',
                                             values='freq', aggfunc=np.sum)
        df2 = df2.reset_index()
        #print(df2[0:50].to_string())
        # remove 'NEFT/RTGS/Not Finished Column
        df2 = df2.drop('NEFT/RTGS', axis=1)
        #print(df2[0:50].to_string())

        # Merge df1 & df2
        aggr_df = dataframe_util.join_df(df1, df2, 'outer', ['mode', 'channel', 'minute_bucket'],
                                         ['mode', 'channel', 'minute_bucket', 'successful_tx', 'total_tx', 'degrade_tx', 'downtime_tx', 'in_progress_tx', 'uptime_tx'])
        #print(aggr_df[0:50].to_string())

        # Find all minute intervals where #transactions are less than 100
        self.logger_obj.info('Find all time intervals with less than 100 transactions')
        df3 = self.input_data_df.pivot_table(index=['minute_bucket'],
                                                   values='freq', aggfunc=np.sum)
        df3 = df3.reset_index()
        # Flag rows that have less than 100 transactions
        df3['valid'] = df3['freq'].apply(lambda x: 0 if x < 100 else 1)
        #df3 = df3[df3['freq'] < 100]
        #print(df3[0:50].to_string())

        # Flag all rows with time intervals having # transactions < 100 as invalid
        self.logger_obj.info('Flag all rows with time intervals having # transactions < 100 as invalid')
        aggr_df = dataframe_util.join_df(aggr_df, df3, 'left', ['minute_bucket'], '')
        aggr_df = aggr_df.reset_index()
        aggr_df = aggr_df.drop('freq', axis=1)
        aggr_df = aggr_df.drop('index', axis=1)
        #print(aggr_df[0:50].to_string())
        #aggr_df = aggr_df[~aggr_df.minute_bucket.isin(df3.minute_bucket)]

        # Convert all NaN to zero for easier calculations
        self.logger_obj.info('Create final output')
        aggr_df = aggr_df.fillna(0)
        print(aggr_df[0:50].to_string())

        # df = aggr_df[aggr_df['mode']=='NEFT']
        # print(df)
        # print(df[0:50].to_string())

        aggr_df.to_parquet('parquet_chk.parquet', index=False)

        df = pd.read_parquet('parquet_chk.parquet')
        print(df)





start_time = "'2020-07-01 17:00:00'"
end_time = "'2020-07-05 17:00:00'"

a = Main()
a.process_data(start_time, end_time)
