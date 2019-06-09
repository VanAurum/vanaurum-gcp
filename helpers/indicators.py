# ingest/func_lib.py

#Standard Python Imports
import pandas as pd
import numpy as np
from scipy import signal

#Local imports

#3rd party imports



def build_ratio(numerator,denominator): 

    query_num=f'SELECT "DATE", "CLOSE", "HIGH", "LOW", "OPEN" FROM "{numerator}";'
    query_den=f'SELECT "DATE", "CLOSE", "HIGH", "LOW", "OPEN" FROM "{denominator}";'
    #If returns is empty, this is the first pass. Load the first asset into returns.
    try:          
        num=query_to_df(query_num)
        num.columns=['DATE',numerator+'CLOSE',numerator+'OPEN',numerator+'HIGH',numerator+'LOW']
    except:
        print('error fetching numerator '+str(numerator)) 
    try:       
        den=query_to_df(query_den)
        den.columns=['DATE',denominator+'CLOSE',denominator+'OPEN',denominator+'HIGH',denominator+'LOW']
    except:
        print('error fetching denominator '+str(denominator))

    merged=pd.merge(num,den,how='left',on='DATE')
    merged.dropna(inplace=True)
    try:
        merged['CLOSE']=merged[numerator+'CLOSE']/merged[denominator+'CLOSE']
        merged['OPEN']=merged[numerator+'OPEN']/merged[denominator+'OPEN']
        merged['HIGH']=merged[numerator+'HIGH']/merged[denominator+'HIGH']
        merged['LOW']=merged[numerator+'LOW']/merged[denominator+'LOW']
    except:
        print('Error calculating ratio for '+numerator+' and '+denominator)
    print(list(merged))
    merged.drop([numerator+'CLOSE', denominator+'CLOSE', numerator+'OPEN',denominator+'OPEN',numerator+'HIGH',denominator+'HIGH',numerator+'LOW',denominator+'LOW'], axis=1,inplace=True)
    print(list(merged))             
    return merged      


def clean_dataframe(df):

    df.columns = [x.upper() for x in df.columns]
    columns=list(df)
    if('Date' not in columns and 'DATE' not in columns and 'date'  not in columns):
        df.reset_index(level=0, inplace=True)
        df.columns = [x.upper() for x in df.columns]

    columns=list(df)
    if ('MID' in columns):
        #If cryptocurrency
        try:
            #If dataset has VOLUME
            col_list=['DATE','HIGH','LOW','MID','LAST','VOLUME']
            df=df[col_list]
            df.columns=['DATE','HIGH','LOW','MID','CLOSE','VOLUME']
        except:
            #If it doesn't have VOLUME
            col_list=['DATE','HIGH','LOW','MID','LAST']
            df=df[col_list]
            df.columns=['DATE','HIGH','LOW','MID','CLOSE']    
    elif ('SETTLE' in columns):
        #If data from SCF
        try:
            #If dataset has VOLUME
            col_list=['DATE','OPEN','HIGH','LOW','SETTLE','VOLUME']
            df=df[col_list]
            df.columns=['DATE','OPEN','HIGH','LOW','CLOSE','VOLUME']
        except:
            col_list=['DATE','OPEN','HIGH','LOW','SETTLE']
            df=df[col_list]
            df.columns=['DATE','OPEN','HIGH','LOW','CLOSE']    
    elif ('CLOSE' in columns):
        #If CLOSE is already specified.
        #Economic data points will be ingested as a dataframe with two columns. By checking if the length of the column list is greater
        #than 2 we screen for that.
        if (len(columns)>2):
            if ('ADJ_CLOSE' in columns):
                try:
                    col_list=['DATE','ADJ_OPEN','ADJ_HIGH','ADJ_LOW','ADJ_CLOSE','ADJ_VOLUME']
                    df=df[col_list]
                    df.columns=['DATE','OPEN','HIGH','LOW','CLOSE','VOLUME']
                except:        
                    pass
        else:
            pass

    else:
        print('Could not clean dataframe.  Columns are misspecified or unhandled.')
        print(list(df))
        return None

    df=df.sort_values('DATE')
    print('Sorted the date column')
    df=df.reset_index(drop=True)
    print('Reset dataframe index')    
    print('CLEANED DATAFRAME HEADERS:...')
    print(list(df))
    return df            

def build_rsi(df):
    
    X=df['CLOSE']    
    for i in range(10,102,2):
        try:
            df['RSI'+str(i)]=CalcRSI(X,i).round(2)*100  
        except:
            print('unable to generate RSI'+str(i))
            continue
    
    return df   


def build_bop(df):
    
    header='BOPMA1'  
    try:
        X=df['CLOSE']
        O=df['OPEN']
        H=df['HIGH']
        L=df['LOW']
    except KeyError as e:
        print('This data from does not have '+str(e))
        return df
        
    try:      
        df[header]=(X-O)/(H-L)  
        df.loc[~np.isfinite(df[header]), header] = np.nan
    except:
        df.drop(header, inplace=True, axis=1)
        
    return df


def build_bopma(df):
    
    if not 'BOPMA1' in list(df):
        return df 
    
    for i in range(2,65,2):
        try:
            df['BOPMA'+str(i)]=df['BOPMA1'].rolling(window=i,center=False).mean()  
        except:
            print('Unable to build BOPMA'+str(i))
            continue
        
    return df    
               

def build_updownsum(df):
    
    X=df['CLOSE']   
    for i in range(5,105,5):    
        try:           
            df['UPDOWNSUM'+str(i)]=UPDOWN(X).rolling(window=i,center=False).sum()  
        except:
            print('Unable to build UPDOWNSUM'+str(i))
            continue
    
    return df


def build_ssk(df): 
    
    X=df['CLOSE']   
    for i in range(5,200,5):    
        try:           
            temp=SlowStochsK(X,i)
            df['SLOWSTOCHS'+str(i)]=SlowStochsD3(temp).round(2)*100    
        except:
            print('Could not build SLOWSTOCHS'+str(i))
            continue   
        
    return df    
    
def build_proxtoboll(df):
    
    X=df['CLOSE']   
    for i in range(5,200,5):   
        try:           
            df['PROXTOBOLL'+str(i)]=ProxToBollinger(X,i)    
        except:
            print('Could not build PROXTOBOLL'+str(i))
            continue 
        
    return df             
    
def build_vaisi(df):
    X=df['CLOSE']
    df['VAISI']=VAISI(X) 
    '''   
    try:           
        df['VAISI']=VAISI(X)   
    except:
        print('Could not build VAISI')
    '''    
    return df

def build_ma_deviations(df):
    X=df['CLOSE']
    Num_MA_Range=list(range(5,205,5))
    Den_MA_Range=list(range(50,450,50))    
    for i in Num_MA_Range:
        for k in Den_MA_Range:
            if((i!=k) & (i<k) & ((i/k)<=0.6)):
                try:            
                    df[str(i)+'DMA_'+str(k)+'DMA_RATIO']=CalcMA(X,i).round(10)/CalcMA(X,k).round(10)
                except:
                    print('Could not build '+str(i)+'DMA_'+str(k)+'DMA_RATIO')
                    continue    
    return df               


def build_returns_deprecated(df):

    X=df['CLOSE']   
    for x in range(1,252,1):           
        try:
            df['RETURNS'+str(x)]=round(X.pct_change(periods=-x)*-1.00,8)
        except:
            print('Could not create RETURNS'+str(x))
            continue          
    return df

def build_returns(df):

    X=df['CLOSE']   
    for x in range(1,252,1):   
        try:           
            df['RETURNS'+str(x)]=round((X.shift(-x)-X)/X,8)  
        except:
            print('Could not create RETURNS'+str(x))  
    return df        

def build_roc(df):

    X=df['CLOSE']   
    for x in range(1,252,1):           
        try:
            df['ROC'+str(x)]=round(X.pct_change(periods=x),8)
        except:
            print('Could not create ROC'+str(x))
            continue          
    return df           

#######################################################
def CalcMA(series, period):
    x=series.rolling(window=period,center=False).mean()
    return x

def SlowStochsK(series,period):
    Period_Max=series.rolling(window=period,center=False).max()
    Period_Min=series.rolling(window=period,center=False).min()
    Result=(series-Period_Min)/(Period_Max - Period_Min)
    return Result


def SlowStochsD3(series):
    D3=series.rolling(window=3,center=False).mean()
    return D3    


def VF6F_Oscillator(Close):
    SS5=SlowStochsK(Close,5)
    SS8=SlowStochsK(Close,8)
    SS13=SlowStochsK(Close,13)
    SS21=SlowStochsK(Close,21)
    SS34=SlowStochsK(Close,34)
    SS55=SlowStochsK(Close,55)
    SS89=SlowStochsK(Close,89)
    VF7F = (SS5+SS8+SS13+SS21+SS34+SS55+SS89)/7  
    arr1=VF7F.values
    arr2=signal.savgol_filter(arr1,9,1,mode='nearest')
    VF6F=pd.DataFrame(arr2)
    return VF6F


def VF9F_Oscillator(Close):
    SS5=SlowStochsK(Close,5)
    SS5D3=SlowStochsD3(SS5)
    SS8=SlowStochsK(Close,8)
    SS8D3=SlowStochsD3(SS8)
    SS13=SlowStochsK(Close,13)
    SS13D3=SlowStochsD3(SS13)
    SS21=SlowStochsK(Close,21)
    SS21D3=SlowStochsD3(SS21)
    SS34=SlowStochsK(Close,34)
    SS34D3=SlowStochsD3(SS34)
    SS55=SlowStochsK(Close,55)
    SS55D3=SlowStochsD3(SS55)
    SS89=SlowStochsK(Close,89)
    SS89D3=SlowStochsD3(SS89)  
    VF9F = (SS5D3+SS8D3+SS13D3+SS21D3+SS34D3+SS55D3+SS89D3)/7
    return VF9F


def ProxToBollinger(Series,period):
    X=Series.rolling(window=period).mean()
    X_STD=Series.rolling(window=period).std()
    UB=X+(2*X_STD)
    LB=X-(2*X_STD)
    Prox=(Series-LB)/(UB-LB)
    return Prox


def ProxToBollingerVar(Series,period,std):
    X=Series.rolling(window=period).mean()
    X_STD=Series.rolling(window=period).std()
    UB=X+(std*X_STD)
    LB=X-(std*X_STD)
    Prox=(Series-LB)/(UB-LB)
    return Prox    


def CalcRSI(prices, n):
    deltas = np.diff(prices)
    seed = deltas[:n]
    up = seed[seed>=0].sum()+0.000001/n
    down = -seed[seed<0].sum()+0.000001/n
    rs = up/down
    rsi = np.zeros_like(prices)
    rsi[n] = 100. - (100./(1.+rs))
    
    for i in range(n, len(prices)):
        delta = deltas[i-1]
        if delta>0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta
        up = (up*(n-1) + upval)/n
        down = (down*(n-1) + downval)/n
        rs = up/down
        rsi[i] = 1. - (1./(1.+rs))
    rsiDF=pd.DataFrame(rsi)
    rsiDF=rsiDF.round(2)
    return rsiDF

def DAISI_Oscillator(Close):
    SS13=SlowStochsK(Close,13)
    SS13D3=SlowStochsD3(SS13)
    SS21=SlowStochsK(Close,21)
    SS21D3=SlowStochsD3(SS21)
    SS34=SlowStochsK(Close,34)
    SS34D3=SlowStochsD3(SS34)
    SS55=SlowStochsK(Close,55)
    SS55D3=SlowStochsD3(SS55)
    SS70=SlowStochsK(Close,70)
    SS70D3=SlowStochsD3(SS70)
    BOLL84=ProxToBollingerVar(Close,84,2.75)
    BOLL13=ProxToBollinger(Close,70)
    BOLL34=ProxToBollinger(Close,70)
    BOLL55=ProxToBollinger(Close,70)
    BOLL100=ProxToBollinger(Close,70)
  
    daisi = (0.20*SS13D3+0.20*SS34D3+0.20*SS55D3+0.20*SS70D3+0.20*BOLL84)
    
    arr1=daisi.values
    arr2=signal.savgol_filter(arr1,9,2,mode='nearest')
    temp=pd.DataFrame(arr2)
    return temp

def VAISI(CLOSE):
    vaisi=DAISI_Oscillator(CLOSE)
    vaisi_5ma=CalcMA(vaisi,5).round(2)
    vaisi_350ma=CalcMA(vaisi,350).round(2)
    vaisi_drift=vaisi_350ma-0.5    
    vaisi_adj=vaisi_5ma-vaisi_drift
    vaisi_good=vaisi_adj.clip(0.0,1.0)
    return vaisi_good

def rolling_zscore(Series, period):
    
    mean=Series.rolling(window=period).mean()
    std=Series.rolling(window=period).std()
    z_score=(Series-mean)/std
    
    return z_score

def UPDOWN(close):
    # True = Higher
    # False = LOWER
    C=close.values    
    m=len(C)
    output=np.zeros_like(C,dtype=int)
    for i in range (m-1) :
        if C[i+1]>C[i]:
            output[i+1] = 1
        else:
            output[i+1] = -1
    df = pd.DataFrame(output).round(0)
    return df    

