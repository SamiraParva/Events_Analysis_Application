import tarfile
import pandas as pd
from os import listdir
from os.path import  join


class DataGetter:
    """
    This is a class for extracting input files from zipfile and load them into correspondent dataframes .
      
    Attributes:
        input_zip_file (string): Name of input zip file.
        input_data_path (string): directory which input file has been located.
    """

    def __init__(self,input_zip_file :str,input_data_path: str,input_main_data_path :str):
        """
        The constructor for DataGetter class.
  
        Parameters:
           input_zip_file (string): Name of input zip file.
           input_data_path (string): directory which input file has been extracted.
           input_main_data_path (string) : parent directory of input files.
        """
        self.input_zip_file = input_zip_file
        self.input_data_path=input_data_path
        self.input_main_data_path=input_main_data_path
        self.unzip_data()


    def unzip_data(self):
        """
        The function to unzip input file.
        """
        with tarfile.open(self.input_zip_file, "r") as zip_ref:
            zip_ref.extractall(self.input_main_data_path)
            zip_ref.close()


    def get_file(self,input_file : str)-> str:
        """
        The function to return and validate file names that had been extracted before.
  
        Parameters:
            input_file (string): file name.
          
        Returns:
            file : validated file name.
        """
        path=self.input_data_path
        for file in listdir(path):
            if join(path, file)==join(path,input_file):
                return file


    def data_into_df(self,input_file: str, date_col: str)-> pd.DataFrame:
        """
        The function to load csv files into Pandas dataFrames.
  
        Parameters:
            input_file (string): file name.
            date_col (string): column name in a input data set with datetime format.
          
        Returns:
            df (pd.dataFrame): a dataframe that corresponds to each input data files.
        """
        files=self.get_file(input_file)
        df=pd.read_csv(join(self.input_data_path,files), parse_dates=[date_col])
        return df


    def create_datasets(self):
        """
        The function to return dataframe for each of input data files.
            
        Returns:
            ordersDF (pd.dataFrame): a dataframe that corresponds to orders.csv.
            pollingDF (pd.dataFrame): a dataframe that corresponds to polling.csv.
            connectivity_statusDF (pd.dataFrame): a dataframe that corresponds to connectivity_status.csv.
        """
        ordersDF=self.data_into_df('orders.csv', 'order_creation_time')
        pollingDF=self.data_into_df('polling.csv', 'creation_time')
        connectivity_statusDF=self.data_into_df('connectivity_status.csv', 'creation_time')
        return ordersDF,pollingDF,connectivity_statusDF
