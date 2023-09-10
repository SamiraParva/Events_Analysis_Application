import pandas as pd
import os
from os import listdir
import numpy as np
import functools as ft
from DataGetter import DataGetter
from QueryData import QueryData

def main():

    input_zip_file=os.path.abspath("../data/input/appEventProcessingDataset.tar.gz")
    input_main_data_path=os.path.abspath("../data/input")
    input_data_path=os.path.abspath("../data/input/dataset")
    output_data_path=os.path.abspath("../data/output/output.csv")
    data_getter = DataGetter(input_zip_file,input_data_path,input_main_data_path) # creating object from DataGetter class
    data_getter.unzip_data() #unzipping input file
    data_getter.create_datasets() #creating input dataFrames
    query_data=QueryData(data_getter) #creating object from QueryData class

    firstDf= query_data.JoinDfs(howjoin='right', leftDf=query_data.pollingDf, 
                                rightDf=query_data.ordersDF, leftkey='device_id', 
                                rightkey='device_id') # joining orders and polling dataFrames
    secondDf = query_data.JoinDfs(howjoin='right', leftDf=query_data.connectivity_statusDF,
                                  rightDf=query_data.ordersDF, leftkey='device_id', 
                                  rightkey='device_id') # joining orders and connectivity status dataFrames

    tot_cnt_of_poll_evnt_3minbefore,tot_cnt_of_poll_evnt_3minafter,tot_cnt_of_poll_evnt_OneHourBefore=query_data.calculate_tot_cnt_of_polling(firstDf) #returning output DFs for calculate total count of polling
    tot_cnt_of_poll_stat_code_evnt_3minbefore,tot_cnt_of_poll_stat_code_evnt_3minafter,tot_cnt_of_poll_stat_code_evnt_OneHourBefore=query_data.calculate_tot_cnt_of_polling_stat_code(firstDf) #returning output DFs for calculate total count of polling type based on status_code
    tot_cnt_of_poll_stat_code_evnt_without_err_3minbefore,tot_cnt_of_poll_stat_code_evnt_without_err_3minafter,tot_cnt_of_poll_stat_code_evnt_without_err_OneHourBefore=query_data.calculate_tot_cnt_of_polling_error_code(firstDf) #returning output DFs for calculate count of polling error_code and the count of responses without error codes.
    polling_event_creation_time_before_order,polling_event_creation_time_after_order=query_data.calculate_preced_follow_poll_time(firstDf) #returning output DFs for calculate the time of the polling event immediately preceding, and immediately following the order creation time.
    
    DFS = [query_data.ordersDF,
           tot_cnt_of_poll_evnt_3minbefore,tot_cnt_of_poll_evnt_3minafter,tot_cnt_of_poll_evnt_OneHourBefore,
           tot_cnt_of_poll_stat_code_evnt_3minbefore,tot_cnt_of_poll_stat_code_evnt_3minafter,tot_cnt_of_poll_stat_code_evnt_OneHourBefore,
           tot_cnt_of_poll_stat_code_evnt_without_err_3minbefore,tot_cnt_of_poll_stat_code_evnt_without_err_3minafter,tot_cnt_of_poll_stat_code_evnt_without_err_OneHourBefore,
           polling_event_creation_time_before_order,polling_event_creation_time_after_order,
           query_data.calculate_most_recent_cn_stat(secondDf)] # create a list of all created DFs

    df_final = ft.reduce(lambda left, right: pd.merge(left, right,how='left', on='order_id'), DFS) # apply merge join on each output DFs and create a single output dataset.
    df_final=df_final.drop('Unnamed: 0',axis=1) # drop unused column
    df_final['MOST_RECENT_CON_STAT'] = df_final['MOST_RECENT_CON_STAT'].fillna('UNKNOWN') # fill null values for MOST_RECENT_CON_STAT column
    df_final.fillna(value=0, inplace=True) # fill null cells for rest of columns
    df_final.to_csv(output_data_path) # write the output dataFrame to a single CSV output.

if __name__ == "__main__":
    main()