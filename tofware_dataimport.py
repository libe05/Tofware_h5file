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

    time_h5file_matlab = ((DAQstart_h5file+(buftime_h5file*1e7))/(864000000000))+584755 # 584755 = (datenum(1601,1,1,0,0,0)), from IGOR converting to MATLAB
    time_h5file_unix = time_h5file_matlab-719529 # 719529 is the datenum of 01-01-1970 UNIX epoch 
    df_time = pd.DataFrame(time_h5file_unix,columns = ['UnixTime']) # time to pandas dataframe, enables conversion to date
    
    for i in df_time:
        df_time['date'] = (pd.to_datetime(df_time[i],unit='d').round('s'))
   
    peaktable = hf['PeakData/PeakTable'][()]
    df_peaktable = pd.DataFrame(peaktable)
    df_peaktable
    
    peakdata = hf['PeakData/PeakData'][()]
    df_peakdata = pd.DataFrame(peakdata[:,0,0,:],columns = df_peaktable['mass'])
    df = pd.concat([df_time, df_peakdata],axis=1)
    peaklabel = df_peaktable['label']    
    exactmass = df_peaktable['mass']
    
    return df,peaklabel,exactmass


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
        path = filepath+filename
        path=path.encode('unicode_escape').decode()
        
        [df,peaklabel,exactmass] = h5_read_file(path)
        df_help.append(df)
    df = pd.concat(df_help,axis=0,ignore_index=True)
    df.index = df['date'] 

    columns = ['unixtime', 'date']
    peakname = []
    for i, label in enumerate(peaklabel):
        columns.append(label.decode())
        peakname.append(label.decode())
         
    df.columns = columns
    md = exactmass - round(exactmass)
    md_exactmass = pd.DataFrame([exactmass,md],index=['mass', 'massdefect'])
    md_exactmass.columns = peakname
   # md_exactmass.rename(['mass', 'mass defect'])
    
    df['datetime'] = df['date']
    # remove unnecessary columns
    df = df.drop('date', axis=1)
    df = df.drop('unixtime', axis=1)
    if ('nominal2' in df): 
        df = df.drop('nominal2', axis=1)
        md_exactmass = md_exactmass.drop('nominal2',axis=1)
    # check size of df to remove unnecessary values from exact masses
    #shape = df.shape
    #num_col = shape[1]-2
    #exactmass = exactmass[0:num_col]
    #md = exactmass - round(exactmass)
    
    return df,md_exactmass