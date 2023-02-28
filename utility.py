import os
import sys
from dotenv import load_dotenv
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
from logging import handlers
import datetime as dt
import json
import matplotlib.pyplot as plt
import boto3
from botocore.exceptions import NoCredentialsError

'''
get S3 credentials from .env file
'''
load_dotenv()
ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")

def create_rotating_log(nameoflog):
    try:
        """
        function: Creates a rotating log file and loffer object for error logging
        output = logger object
        """
        logger = logging.getLogger(nameoflog)
        logger.setLevel(logging.INFO)

        ## Here we define our formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        filename = nameoflog+'.log'
        logHandler = handlers.RotatingFileHandler(filename, maxBytes=10000000, backupCount=4)
        logHandler.setLevel(logging.INFO)
        ## Here we set our logHandler's formatter
        logHandler.setFormatter(formatter)

        logger.addHandler(logHandler)
    except Exception as e:
            logger = logging.getLogger(nameoflog)
            logger.setLevel(logging.INFO)
            log_error(e)

    return logger

logger = create_rotating_log('Crypto')  

def log_error(e):
    '''
    function: logs error to log file with traceback and datetime
    '''
    exception_message = str(e)
    exception_type, exception_object, exception_traceback = sys.exc_info()
    filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
    logger.error(f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno} "+ str(dt.datetime.now()))



def get_soup(url,headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}):
    '''
    Function: gets soup object for a url
    input: URL, headers
    output: soup Object
    '''
    try:
        response = requests.get(url,headers=headers)
        soup = BeautifulSoup(response.text,'lxml')
    except Exception as e:
        log_error(e)
        soup = None
    return soup

def get_crypto_history_urls(url='https://finance.yahoo.com/crypto/?offset=0&count=100'):
    '''
    Function: get top 100 crypto currency codes with their history urls from yahoo finance
    output: json
    '''
    try:
        anchors = []
        soup = get_soup(url)
        anchor_divs = soup.findAll('a',{"class":"Fw(600) C($linkColor)"})
        for element in anchor_divs:
            result = {}
            result["Currency"] = element.text
            link = element["href"]
            link = link.split("?")[0]
            link = 'https://finance.yahoo.com'+link+'/history/'
            result["History"] = link
            anchors.append(result)
        
        #store all currencies data to disk
        with open("./Currency/currencies.json", "w") as outfile:
            json.dump(anchors, outfile)  
    except Exception as e:
        log_error(e)

    return anchors

def get_historical_data(currency):
    
    
    '''
    Function: get historical data of a particular currency code in Dataframe format
    Input: currency string
    Output: DataFrame
    '''
    
    yahoo_api = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=1646042626&period2=1677578626&interval=1d&events=history&includeAdjustedClose=true"%currency
    try:
        soup = get_soup(yahoo_api)
        text = soup.text
        listp = text.splitlines()
        list_oflist = []
        for i in listp:
            data_list = i.split(",")
            list_oflist.append(data_list)
        df = pd.DataFrame(list_oflist[1:],columns=list_oflist[0])
        df['Date'] = df['Date'].astype('datetime64[ns]')
        df['Open'] = df['Open'].astype(float)
        df['High'] = df['High'].astype(float)
        df['Low'] = df['Low'].astype(float)
        df['Close'] = df['Close'].astype(float)
        df['Adj Close'] = df['Adj Close'].astype(float)
        df['Volume'] = df['Volume'].astype(float)
        df.sort_values(by='Date',ascending=False, inplace=True)
        return df
    except Exception as e:
        log_error(e)
        return None
    
def get_average(currency,column,days=30):
    '''
    Function: gives mean of currency value in last n days for x column
    Input: list of 2 currencies, column on which mean to be calculated, last n days of data to be taken
    Output: Float
    '''
    try:
        df = get_historical_data(currency)
        df=df.head(days)
        mean = df[column].mean()
        return mean
    except Exception as e:
        log_error(e)
        return None

def get_correlation(Currencies,column,days=30):
    '''
    Function: gives correlation between 2 currencies for last n days for x column
    Input: list of 2 currencies, column on which correlation should happen, last x days of data to be taken
    Output: Float
    '''
    df_list = []
    new_columns = []
    for i in Currencies:
        df = get_historical_data(i)
        df=df.head(days)
        df.drop(df.columns.difference([column]), axis=1, inplace=True)
        df.rename(columns = {column:i+''+column}, inplace = True)
        df_list.append(df)
        new_columns.append(i+''+column)
    df_concat = pd.concat(df_list, axis=1, join='inner')
    correlation = df_concat[new_columns[0]].corr(df_concat[new_columns[1]])
    return correlation

def get_scatter_plot(currrency,xlabel='Date',ylabel='Value($)',column='Adj Close'):
    '''
    Function: gives scatter plot for a currency
    Input: currency name, x-axis label, y-axis label,column name which contain values
    Output:Plot
    '''
    df = get_historical_data(currrency)
    # Scatter plot
    fig, ax = plt.subplots(figsize = (18,10))
    ax.scatter(df['Date'], df[column])
    
    # x-axis label
    ax.set_xlabel(xlabel)
    
    # y-axis label
    ax.set_ylabel(ylabel)
    plt.show()

def get_plot_two(currrency,xlabel='Date',ylabel='Value($)',column='Adj Close'):
    '''
    Function: gives combined plot for 2 currencies
    Input: list of 2 currencies, x-axis label, y-axis label,column name which contain values
    Output: Plot
    '''
    try:
        df_list = []
        new_columns = []
        for i in currrency:
            df = get_historical_data(i)
            df.drop(df.columns.difference([column,'Date']), axis=1, inplace=True)
            df.rename(columns = {column:i+''+column}, inplace = True)
            df_list.append(df)
            new_columns.append(i+''+column)
        df_concat=pd.merge(df_list[0], df_list[1], on = "Date", how = "inner")
        fig, ax = plt.subplots(figsize=(20,10)) 

        df_concat.plot(x = 'Date', y = new_columns[0], ax = ax) 
        df_concat.plot(x = 'Date', y = new_columns[1], ax = ax, secondary_y = True) 
        plt.show()
    except Exception as e:
        log_error(e)

def upload_to_aws(local_file, bucket, s3_file):
    '''
    Function: uploads data to s3
    Input: local file, bucket name, s3 file name as to be stored
    Output: Boolean
    '''
    s3 = boto3.client('s3',region_name='ap-south-1', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False















    