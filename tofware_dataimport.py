# -*- coding: utf-8 -*-
"""
Created on Wed Oct  5 22:19:41 2022

@author: Lisa Beck
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import h5py

def h5_read_file(filepath):  
    
    '''

    Parameters
    ----------
    filepath : TYPE String
        Path to folder including all processed h5 files from Tofware. 
        Example: r"../../03_Data/CAFE_EU_02/CAFE_EU_02_2020-05-26/Avg-30/Processed/"

    Returns : dataframe including date and time as index, HR peaks and nominal unit mass as header and the corresponding data from the mass spec
    peak labels which can then be used for headers by converting the binary string to a string: 
        
    columns = ['unixtime', 'date']
    for i, label in enumerate(peaklabel):
        columns.append(label.decode())
    -------

    '''
    
    hf = h5py.File(filepath,'r')
    buftime = hf.get('TimingData/BufTimes') # buf time from h5 file
    buftime_h5file = buftime[()] # buftime readout in seconds from AcquisitionTimeZero

    DAQstart_h5file = hf['TimingData'].attrs['AcquisitionTimeZero'] # get start time of file 
    Tofperiod = hf['TimingData'].attrs['TofPeriod'] # Tofperiod used for converting from ions/extraction to ions/second

    time_h5file_matlab = ((DAQstart_h5file+(buftime_h5file*1e7))/(864000000000))+584755 # 584755 = (datenum(1601,1,1,0,0,0)), from IGOR converting to MATLAB
    time_h5file_unix = time_h5file_matlab-719529 # 719529 is the datenum of 01-01-1970 UNIX epoch 
    df_time = pd.DataFrame(time_h5file_unix,columns = ['UnixTime']) # time to pandas dataframe, enables conversion to date
    
    for i in df_time:
        df_time['date'] = (pd.to_datetime(df_time[i],unit='d').round('s'))
   
    peaktable = hf['PeakData/PeakTable'][()]
    df_peaktable = pd.DataFrame(peaktable)

    peaklabel = df_peaktable['label']    
    exactmass = df_peaktable['mass']  
    peakname = []
    for i,label in enumerate(peaklabel):
        peakname.append(label.decode())
    
    #while cycle for confirming the starting position of UMR
    UMR_start=0
    while peaklabel[UMR_start].decode()!='nominal2':
        UMR_start=UMR_start+1
    
    peakdata = hf['PeakData/PeakData'][()]/Tofperiod*10**(9) # unit in ions/second
    df_peakdata_HR = pd.DataFrame(peakdata[:,0,0,0:UMR_start],columns = peakname[0:UMR_start])
    UMR_upper=800 # upper cutoff for UMR
    df_peakdata_UMR = pd.DataFrame(peakdata[:,0,0,UMR_start:UMR_start+UMR_upper],columns = df_peaktable['mass'][UMR_start:UMR_start+UMR_upper])
    df = pd.concat([df_time, df_peakdata_HR, df_peakdata_UMR],axis=1) # df includes time, HR and part of UMR
    df.index = df['date'] 
    df=df.drop('UnixTime',axis=1)
    df=df.drop('date',axis=1)
    
    md = exactmass - round(exactmass)
    md_exactmass = pd.DataFrame([exactmass,md],index=['mass', 'massdefect'])
    md_exactmass.columns = peakname
    md_exactmass=md_exactmass.drop('Total ion current', axis=1)
    if ('nominal2' in md_exactmass): 
        md_exactmass = md_exactmass.drop('nominal2',axis=1)

    
    return df,md_exactmass


def h5_read_folder(filepath):  
    '''

    Parameters
    ----------
    filepath : TYPE String
        Path to folder including all processed h5 files from Tofware. 
        Example: r"../../03_Data/CAFE_EU_02/CAFE_EU_02_2020-05-26/Avg-30/Processed/"

    Returns : dataframe including date and time as index, HR peaks and nominal unit mass as header and the corresponding data from the mass spec
    -------

    '''
    
    df_help = []
    for filename in os.listdir(filepath):
        if filename.endswith(".h5"): # make sure the file ends with .h5
            path = filepath+filename
            path=path.encode('unicode_escape').decode()
            
            [df,md_exactmass] = h5_read_file(path)
            df_help.append(df)
    df = pd.concat(df_help,axis=0)
    df=df.sort_index() # make sure that the data is in sequence regarding time
    
    return df,md_exactmass