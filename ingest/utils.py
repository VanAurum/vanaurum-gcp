#Python core modules
import pandas as pd

#3rd party app imports
import quandl
from google.cloud import storage


#local imports
from config import keys
from config import data_settings
from helpers.indicators import (
            build_rsi, 
            build_updownsum, 
            build_ssk, 
            build_proxtoboll, 
            build_vaisi, 
            build_returns, 
            build_ma_deviations, 
            clean_dataframe, 
            build_roc, 
            build_bop, 
            build_bopma,
    )



def get_remote_data(tag):
    '''
    Retrieves data from provider and sends e-mail if error occurs.
       -If the code received is None, it means there's not tag in our dictionary for that asset.  Return None.
       -If there's a valid code, attempt to received data from provider. 5 attempts will occur at 2 second intervals if there's
        a failure.
    '''
    quandl.ApiConfig.api_key = keys.QUANDL_KEY

    try:
        code=tag_conv_lib(tag)
    except KeyError as error:
        print(error)
        print('A key error occurred when converting the tag '+tag+' to its Quandl code.')
        code=None    

    if (code==None):
        print('Code for '+tag+' does not exist')
  
    elif ('BITFINEX' in code or 'EOD' in code):
        for x in range(0,4):    
            try:       
                data = quandl.get(code)
                str_error = None 
            except Exception as str_error:
                print(str_error)
                if (x<4):
                    msg='Error: could not retrieve Quandl data for code. An error occurred: '+tag+'. Waiting 2 seconds and then attempting again.'
                    exceptions.append(msg)
                    exceptions.append(str_error)                   
                    pass
                else:
                    msg='Tried to retreive data for '+tag+' five times. Moving to next dataset'
                    exceptions.append(msg)

                    return    
            if str_error:
                sleep(2)
            else:
                break              
    else:
        for x in range(0,5):    
            try:       
                data=quandl.get_table('SCF/PRICES', quandl_code=code, paginate=True)
                print('Succesfully retrieved data for: '+ tag)  
                break
            except:   
                print('Error: could not retrieve Quandl data for code. An error occurred: SCF/'+code+'. Waiting 2 seconds and then attempting again.')           
                sleep(5)
                continue

    return data


def tag_conv_lib(tag):
    tag_lib=data_settings.FUTURES_TAG_LIST   
    if (str(tag_lib.get(tag))==None):        
        exceptions.append('Error: No data provider tag available for '+tag+'. Occurred in tag_conv_lib Line 33.')
        return None
  
    return tag_lib[tag]    