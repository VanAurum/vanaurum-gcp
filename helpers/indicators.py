# ingest/func_lib.py

#Standard Python Imports
import pandas as pd
import numpy as np
from scipy import signal

#Local imports

#3rd party imports
       

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

