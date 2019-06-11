#Python core modules
import pandas as pd
import datetime
import sys
import time
import logging

#3rd party app imports
import quandl
from google.cloud import storage


#local imports
from config import keys, data_settings, log_settings
from helpers.gcp_utils import clean_dataframe
from helpers.indicators import (
            build_rsi, 
            build_updownsum, 
            build_ssk, 
            build_proxtoboll, 
            build_vaisi, 
            build_returns, 
            build_ma_deviations, 
            build_roc, 
            build_bop, 
            build_bopma,
    )


# Initialize logger
logging.basicConfig(
    level=log_settings.LOG_LEVEL,
    format=log_settings.LOG_FORMAT,
    datefmt=log_settings.LOG_DATE_FORMAT,
    )
log = logging.getLogger(__name__)


def get_remote_data(tag):
    '''
    Retrieves data from provider and sends e-mail if error occurs.
       -If the code received is None, it means there's not tag in our dictionary for that asset.  Return None.
       -If there's a valid code, attempt to received data from provider. 5 attempts will occur at 2 second intervals if there's
        a failure.
    '''
    quandl.ApiConfig.api_key = keys.QUANDL_KEY

    try:
        code = tag_conv_lib(tag)
        log.debug('Converted ' +tag+' to '+code)
    except KeyError as error:
        log.warning(error)
        log.warning('A key error occurred when converting the tag '+tag+' to its Quandl code.')
        code = None    

    if (code==None):
        log.warning('Code for '+tag+' does not exist')
  
    elif ('BITFINEX' in code or 'EOD' in code):
        for x in range(0,4):    
            try:       
                data = quandl.get(code)
                log.info('Succesfully retrieve data for '+code)
            except Exception as str_error:
                log.warning(str_error)
                if (x<4):
                    log.warning(' Error: could not retrieve Quandl data for code. An error occurred: '+tag+'. Waiting 2 seconds and then attempting again.')                
                    pass
                else:
                    log.warning('Tried to retreive data for '+tag+' five times. Moving to next dataset')
                    return    
            if str_error in locals():
                time.sleep(2)
            else:
                break              
    else:
        for x in range(0,5):    
            try:       
                data = quandl.get_table('SCF/PRICES', quandl_code=code, paginate=True)
                log.info('Succesfully retrieved data for: '+ tag)  
                break
            except:   
                log.warning('Error: could not retrieve Quandl data for code. An error occurred: SCF/'+code+'. Waiting 2 seconds and then attempting again.')           
                time.sleep(5)
                continue

    return data


def tag_conv_lib(tag):
    tag_lib = data_settings.FUTURES_TAG_LIST   
    if (str(tag_lib.get(tag)) == None):        
        log.warning('Error: No data provider tag available for '+tag+'. Occurred in tag_conv_lib Line 33.')
        return None
    return tag_lib[tag]     


def handle_special_cases(df, tag):
    '''If there are any datasets that require special handling or non-standard adjustments add them here.'''
    #adding this special exception because FEDFUNDS30 appears relative to 100 in data source.
    if (tag == 'FEDFUNDS_30_D'):
        df['CLOSE'] = 100 - df['CLOSE']
        log.debug('Special case handled for '+tag)

    return df


def map_data(df,tag):

    df = clean_dataframe(df, tag)
    log.info('Successfully cleaned dataframe')
    df = handle_special_cases(df, tag)

    try:
        df['DAY_OF_WEEK'] =df['DATE'].dt.dayofweek
        df['WEEK_OF_YEAR'] = df['DATE'].dt.week
        df['MONTH_OF_YEAR'] = df['DATE'].dt.month
    except:
        log.warning('....Could not generate date analytics for ' +tag)  
    
    try:
        df = build_rsi(df)
    except:
        log.warning('Error building RSI data for '+tag)
        
    try:
        df = build_bop(df)
    except:
        log.warning('Error building BOP data for '+tag)

    try:
        df = build_bopma(df)
    except:
        log.warning('Error building BOPMA data for '+tag)

    try:
        df = build_updownsum(df)
    except:
        log.warning('Error building UPDOWNSUM data for '+tag)  
        
    try:
        df = build_ssk(df)
    except:
        log.warning('Error building SSK data for '+tag)  

    try:
        df = build_proxtoboll(df)
    except:
        log.warning('Error building PROXTOBOLL data for '+tag)            

    try:
        df = build_vaisi(df)
    except:
        log.warning('Error building VAISI for '+tag)

    try:
        df=build_ma_deviations(df)
    except:
        log.warning('Error building MA DEVIATIONS data for '+tag)

    try:
        df=build_updownsum(df)
    except:
        log.warning('Error building UPDOWNSUM '+tag)                 

    try:
        df=build_roc(df)
    except:
        log.warning('Error building ROC data for '+tag)   

    try:
        df=build_returns(df)
    except:
        log.warning('Error building RETURNS data for '+tag)

    return df    

