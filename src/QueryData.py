import pandas as pd
import numpy as np
from DataGetter import DataGetter

class QueryData(DataGetter):
    """
    This is a class for querying data based on proposed KPIs .
      
    Attributes:
        dg (object): object of DataGetter class.
    """

    def __init__(self,dg):
        """
        The constructor for QueryData class.
  
        Parameters:
          dg (object): object of DataGetter class.   
        """

        self.ordersDF,self.pollingDf,self.connectivity_statusDF=dg.create_datasets()

    def JoinDfs(self,leftDf:pd.DataFrame,rightDf:pd.DataFrame,leftkey:str,rightkey:str,howjoin:str)-> pd.DataFrame:
        """
        A user-defined function to execute join operation on given dataframes.

        Parameters:
            leftDf (pd.dataFrame): a dataFrame that is indicating a dataset on the left side of the join operation.
            rightDf (pd.dataFrame): a dataFrame that is indicating a dataset on the right side of the join operation.
            leftkey (string): a name of a column as a join key that belongs to a dataset on the left side of the join operation.
            rightkey (string): a name of a column as a join key that belongs to a dataset on the right side of the join operation.
            howjoin (string): a name of a type of join operation.

        Returns:
            (pd.dataFrame): a dataFrame that is a result of a merge operation on two given dataFrames.
        """
        return pd.merge(leftDf,rightDf,how=howjoin,left_on=leftkey,right_on=rightkey)

    def calculate_tot_cnt_of_polling(self,df:pd.DataFrame)  :
        """
        A user-defined function to calculate total count of polling events before and after specific period of time with respect to order creation time.

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            tot_cnt_of_poll_evnt_3minbefore (pd.dataFrame): a dataFrame that returns total count of polling event for each order 3 minutes before order creation time.
            tot_cnt_of_poll_evnt_3minafter (pd.dataFrame): a dataFrame that returns total count of polling event for each order 3 minutes after order creation time.
            tot_cnt_of_poll_evnt_OneHourBefore (pd.dataFrame): a dataFrame that returns total count of polling event for each order 1 hour before order creation time.
        """

        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 'm')
        tot_cnt_of_poll_evnt_3minbefore = df[df['diff'].between(-3 , 0)].groupby(['order_id']).size().reset_index(name='tot_cnt_of_poll_evnt_3minbefore') \
            [['order_id', 'tot_cnt_of_poll_evnt_3minbefore']]
        tot_cnt_of_poll_evnt_3minafter = df[df['diff'].between(0 , 3)].groupby(['order_id']).size().reset_index(name='tot_cnt_of_poll_evnt_3minafter') \
            [['order_id', 'tot_cnt_of_poll_evnt_3minafter']]
        tot_cnt_of_poll_evnt_OneHourBefore = df[df['diff'].between(-60 , 0)].groupby(['order_id']).size().reset_index(name='tot_cnt_of_poll_evnt_OneHourBefore') \
            [['order_id', 'tot_cnt_of_poll_evnt_OneHourBefore']]
        return tot_cnt_of_poll_evnt_3minbefore,tot_cnt_of_poll_evnt_3minafter,tot_cnt_of_poll_evnt_OneHourBefore


    def calculate_tot_cnt_of_polling_stat_code(self,df:pd.DataFrame) :
        """
        A user-defined function to calculate count of each type of polling status_code before and after specific period of time with respect to order creation time.

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            tot_cnt_of_poll_stat_code_evnt_3minbefore (pd.dataFrame): a dataFrame that returns The count of each type of polling status_code for each order 3 minutes before order creation time.
            tot_cnt_of_poll_stat_code_evnt_3minafter (pd.dataFrame): a dataFrame that returns The count of each type of polling status_code for each order 3 minutes after order creation time.
            tot_cnt_of_poll_stat_code_evnt_OneHourBefore (pd.dataFrame): a dataFrame that returns The count of each type of polling status_code for each order 1 hour before order creation time.
        """

        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 'm')
        tot_cnt_of_poll_stat_code_evnt_3minbefore = df[df['diff'].between(-3 , 0)].groupby(['order_id', 'status_code']).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_3minbefore_')
        tot_cnt_of_poll_stat_code_evnt_3minafter = df[df['diff'].between(0 , 3)].groupby(['order_id', 'status_code']).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_3minafter_')
        tot_cnt_of_poll_stat_code_evnt_OneHourBefore = df[df['diff'].between(-60 , 0)].groupby(['order_id', 'status_code']).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_OneHourBefore_')

        return tot_cnt_of_poll_stat_code_evnt_3minbefore,tot_cnt_of_poll_stat_code_evnt_3minafter,tot_cnt_of_poll_stat_code_evnt_OneHourBefore

    def calculate_tot_cnt_of_polling_error_code(self,df:pd.DataFrame) :
        """
        A user-defined function to calculate The count of each type of polling error_code and the count of responses without error
        codes before and after specific period of time with respect to order creation time.

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            tot_cnt_of_poll_error_code_evnt_3minbefore (pd.dataFrame): a dataFrame that returns The count of each type of polling error_code and the count of responses without error
                                                                       codes before and after specific period of time for each order 3 minutes before order creation time.
            tot_cnt_of_poll_error_code_evnt_3minafter (pd.dataFrame): a dataFrame that returns The count of each type of polling error_code and the count of responses without error
                                                                      codes before and after specific period of time for each order 3 minutes after order creation time.
            tot_cnt_of_poll_error_code_evnt_OneHourBefore (pd.dataFrame): a dataFrame that returns The count of each type of polling error_code and the count of responses without error
                                                                          codes before and after specific period of time for each order 1 hour before order creation time.
        """

        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 'm')
        df['error_code'] = df['error_code'].fillna('WITHOUT_ERR')
        tot_cnt_of_poll_error_code_evnt_3minbefore = df[df['diff'].between(-3 , 0)].groupby(['order_id', 'error_code'], dropna=False).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_3minbefore_')
        tot_cnt_of_poll_error_code_evnt_3minafter = df[df['diff'].between(0 , 3)].groupby(['order_id', 'error_code'], dropna=False).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_3minafter_')
        tot_cnt_of_poll_error_code_evnt_OneHourBefore = df[df['diff'].between(-60 , 0)].groupby(['order_id', 'error_code'], dropna=False).size().unstack(fill_value=0).add_prefix('tot_cnt_of_poll_stat_code_evnt_OneHourBefore_')
        return tot_cnt_of_poll_error_code_evnt_3minbefore,tot_cnt_of_poll_error_code_evnt_3minafter,tot_cnt_of_poll_error_code_evnt_OneHourBefore

    def calculate_preced_follow_poll_time(self,df:pd.DataFrame) :
        """
        A user-defined function to calculate the time of the polling event immediately preceding, and immediately following the order creation time.

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            timePollingEventPrecedDF (pd.dataFrame): a dataFrame that returns The time of the polling event immediately preceding the order creation time.
            timePollingEventFollowDF (pd.dataFrame): a dataFrame that returns The time of the polling event immediately following the order creation time.
        """
        
        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 'm')
        df['rank'] = df[df['diff'] <= 0 ].groupby('order_id')['diff'].rank(ascending=0, method='dense')
        timePollingEventPrecedDF = df[ (df['rank'] == 1)][['order_id', 'creation_time']]\
        .rename(columns={'creation_time': 'polling_event_creation_time_before_order'})

        df['rank'] = df[df['diff'] >= 0 ].groupby('order_id')['diff'].rank(ascending=1, method='dense')
        timePollingEventFollowDF = df[ (df['rank'] == 1)][['order_id', 'creation_time']]\
        .rename(columns={'creation_time': 'polling_event_creation_time_after_order'})

        return timePollingEventPrecedDF,timePollingEventFollowDF

    def calculate_most_recent_cn_stat(self,df:pd.DataFrame) :
        """
        A user-defined function to calculate the most recent connectivity status (“ONLINE” or “OFFLINE”) before an order, and at
        what time the order changed to this status. 

        Parameters:
            df (pd.dataFrame): a dataFrame that is a source for calculating requested KPIs.

        Returns:
            connectivityDF (pd.dataFrame): a dataFrame that returns the most recent connectivity status (“ONLINE” or “OFFLINE”) before an order, and at what time the order changed to this status.
        """

        df['diff'] = (df['creation_time'] - df['order_creation_time'])/np.timedelta64(1, 's')
        df['rank'] = df[df['diff'] <= 0].groupby('order_id')['diff'].rank(ascending=0, method='first').astype(int)
        connectivityDF = df[(df['diff'] <= 0) & (df['rank'] == 1)][['order_id', 'creation_time', 'status']] \
        .rename(columns={'creation_time': 'TIME_OF_MOST_RECENT_CON_STAT', 'status': 'MOST_RECENT_CON_STAT'})
        return connectivityDF
