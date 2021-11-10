#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 13:40:25 2021

@author: stefan

Testing data handling using cupy library

Usage
-----
rosi: start gpu
sudo sh /mnt/visdat/Projekte/2020/GWN\ viewer/dev/python/dl/script.sh 

GWN-Viewer
python3 /mnt/visdat/Projekte/2020/GWN\ viewer/dev/python/spatial_join_parameters/spatial_join_gpu.py -src_h /mnt/visdat/Projekte/2020/GWN\ viewer/daten/intersects_areas/kliwes/ -src_p /mnt/visdat/Projekte/2020/GWN\ viewer/daten/restructured/kliwes/ -tgt /mnt/galfdaten/daten_stb/gwn_sachsen/ -src_type kliwes -p 0.1 

Wasserhaushaltsportal
python3 /mnt/visdat/Projekte/2020/GWN\ viewer/dev/python/spatial_join_parameters/spatial_join_gpu.py -src_h /mnt/visdat/Projekte/2019/wasserhaushaltsportal/daten/intersect_areas/ -src_p /mnt/visdat/Projekte/2019/wasserhaushaltsportal/daten/tu_daten/2021_03_04/0/ -tgt /mnt/galfdaten/daten_stb/gwn_sachsen/ -src_type kliwes -p 0.1 

"""
import sys
import os
import h5py
import numpy as np
import cupy as cp
import pandas as pd
import time
import shutil
sys.path.append(os.getcwd() + '/..')
from pg.pg import db_connector as pg
import tensorflow as tf

def get_arguments():
    print("--> get caller arguments")
    src_hydrotop, src_param, src_type, tgt_file, p = None, None, None, None, None
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '-src_h':
            i = i + 1
            src_hydrotop = sys.argv[i]
        if arg == '-src_p':
            i = i + 1
            src_param = sys.argv[i]
        if arg == '-src_type':
            i = i + 1
            src_type = sys.argv[i]
        if arg == '-tgt':
            i = i + 1
            tgt_file = sys.argv[i]
        if arg == '-p':
            i = i + 1
            p = sys.argv[i]
        i = i + 1

    if src_hydrotop == None or src_param == None or tgt_file == None or src_type == None or p == None :
        print("ERROR: arguments of program call missing")
        sys.exit()

    return src_hydrotop, src_param, src_type, tgt_file, p

def check_file_exists(filelist):
    # src_hydrotope folder
    area_folder = os.path.split(filelist[0])[0]
    if os.path.isdir(area_folder):
        print(area_folder, '--> ok, folder exists')
    else:
        print("ERROR: folder not exists --> ", area_folder)
        sys.exit()
    # src_parameter folder
    src_folder = os.path.split(filelist[1])[0]
    if os.path.isdir(src_folder):
        print(filelist[1], '--> ok, folder exists')
    else:
        print("ERROR: folder not exists --> ", src_folder)
        sys.exit()
    # target folder
    tgt_folder = os.path.split(filelist[2])[0]
    if os.path.isdir(tgt_folder):
        print(tgt_folder, '--> ok, folder exists')
    else:
        print("ERROR: path not exists --> ", tgt_folder)
        sys.exit()
        
def read_areas(area_folder, id_area_restricted):
    files = []
    # get files
    for r, d, f in os.walk(area_folder):
        for file in f:
            if '.nc' in file:
                files.append(os.path.join(r, file))

    # loop through filelist and fill the areaContainer
    areaContainer = []
    for f in files:        
        idArea = os.path.split(f)[1].split('.')[-2]
        if id_area_restricted != None:
            if int(idArea) == int(id_area_restricted):
                print('--> load area: ', f)
                f_intersect = h5py.File(f, 'r')
                df_intersect = pd.DataFrame({'id_hydrotop' : f_intersect['idhydrotope_org'][:], 'id_area' : f_intersect['idarea_data'][:], 'area' : f_intersect['area'][:]})
                areaContainer.append({'idArea' : idArea, 'df' : df_intersect})
                print('--> done')
                f_intersect.close()
        else:
            print('--> load area: ', f)
            print('--> idArea: ', idArea)
            f_intersect = h5py.File(f, 'r')
            df_intersect = pd.DataFrame({'id_hydrotop' : f_intersect['idhydrotope_org'][:], 'id_area' : f_intersect['idarea_data'][:], 'area' : f_intersect['area'][:]})
            areaContainer.append({'idArea' : idArea, 'df' : df_intersect})
            print('--> done')
            f_intersect.close()

    return areaContainer        

def read_hydrotopes(src_folder, id_param_restricted, start_year, end_year):
    files = []
    for r, d, f in os.walk(src_folder):
        for file in f:
            if '.h5' in file:
                if id_param_restricted != None and start_year != None and end_year != None:
                    if id_param_restricted in file:
                        if int(file.split('_')[-1].split('.')[0]) in range(start_year, end_year + 1):
                            files.append(os.path.join(r, file))
                elif id_param_restricted != None and start_year == None and end_year == None:
                    if id_param_restricted in file:
                        files.append(os.path.join(r, file))
                elif id_param_restricted == None and start_year != None and end_year != None:
                    if int(file.split('.')[0]) in range(start_year, end_year + 1) and project == 'whp':
                        files.append(os.path.join(r, file))
                else:
                    files.append(os.path.join(r, file))

    return files
        
def read_parameter(src_param, src_type):
    # read parameter dataset
    print('--> read parameter ...')
    # read hdf file
    df_param = pd.read_hdf(src_param, key='table').reset_index()

    if src_type == 'difga':
        df_param['index'] = df_param['index'].astype(str)
    else:
        df_param['index'] = df_param['index'].astype(int)

    # get a column for each month
    df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param.values_block_0.str.split(expand=True,)
    # set nan values
    # remove column values_block_0
    df_param = df_param.drop(columns=['values_block_0'])
    df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']].astype(float)
    df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']].replace(-9999, np.nan)
    
    return df_param

def read_parameter_whp(src_param, src_type):
    # read parameter dataset wasserhaushaltsportal
    print('--> read parameter ...')
    # read hdf file
    df_param = pd.read_hdf(src_param).reset_index()
    # values_block_0 [111 111 222 ....] as int array
    if df_param.shape[1] == 13 :
        df_param.columns = ['index','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
        df_param['index'] = df_param['index'].astype(int)
    
    # values_block_0 [b'111 111 222 ....'] as string
    if df_param.shape[1] == 2 :
        # get a column for each month
        df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param.values_block_0.str.split(expand=True,)
        # set nan values
        # remove column values_block_0
        df_param = df_param.drop(columns=['values_block_0'])
        df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']].astype(float)
    
    return df_param

def do_spatial_join(df_intersect, df_param, df_areasize):
    #print('--> perform spatial join')
    # merge datasets
    df_join = pd.merge(df_intersect, df_param, left_on='id_hydrotop', right_on='index' ,how='left', sort=False)
    # get sum of area sizes
    df_join['id_area'] = df_join['id_area'].astype(int)
    df_areasize['idarea_data'] = df_areasize['idarea_data'].astype(int)
    df_join = pd.merge(df_join, df_areasize, how='inner', left_on='id_area', right_on='idarea_data')
    # calculate percent
    df_join['percent_area'] = df_join['area'] / df_join['area_size']
    # drop columns
    df_join = df_join.drop(columns=['idarea','orgid','idarea_data','area_size'])
    
    if (df_join.shape)[0] != (df_intersect.shape)[0]:
        print('ERROR : different shapes sizes of df_intersect and df_join')
        print(df_intersect.shape, df_param.shape, df_join.shape)
        sys.exit()

    df_param = None
    # test for negative area sizes
    df_area_0 = df_join.loc[df_join['area']<0]
    if (df_area_0.shape)[0] > 0:
        print('ERROR : There negative area sizes')
        print('-->',df_area_0.shape)
        print(df_area_0)
        sys.exit()

    return df_join

def get_area_sizes(id_area, project):
    """get area sizes for an idarea"""
    #print('--> get total area from database, id_area --> ' + str(id_area))
    df_out = pd.DataFrame()
    
    if project == 'whp':
        dbconfig = {
            "db_host" : "192.168.0.194",
            "db_name" : "whp_sachsen",
            "db_user" : "visdat",
            "db_password" : "9Leravu6",
            "db_port" : "9991"
        }
    if project == 'gwn':
        dbconfig = {
            "db_host" : "192.168.0.194",
            "db_name" : "gwn_sachsen",
            "db_user" : "visdat",
            "db_password" : "9Leravu6",
            "db_port" : "9991"
        }
    # query database
    db = pg()
    db.dbConfig(dbconfig)
    db.dbConnect()
    sql = 'select ad.idarea, ad.description_int, ad.idarea_data, round( st_area(ag.the_geom)) as area_size ' +\
        ' from spatial.area_data ad join spatial.area_geom ag on ad.idarea_data = ag.idarea_data and ad.idarea = ag.idarea ' +\
        ' where ad.idarea  = ' + str(id_area) + ' order by ad.idarea_data ' 
    res = db.tblSelect(sql)
    db.dbClose()
    if res[1] > 0:
        df_out = pd.DataFrame(np.array(res[0]), columns={0,1,2,3}).rename(columns={0:'idarea',1:'orgid',2:'idarea_data',3:'area_size'})
    else:
        sys.exit('ERROR: area not found in database, id_area ' + str(id_area))

    return df_out

def save_data(tgt_folder, src_param, ds, id_area):
    print('--> save data')
    # get folder properties from filename
    project_folder = tgt_folder
    fn_param = ((os.path.split(src_param)[1]).split('.'))[0].split('_')
    src_folder = (os.path.split(src_param)[0]).split('/')
    print(src_folder)
    print(src_param)
    if len(fn_param) > 1:
        scenario = fn_param[0]
        parameter = fn_param.split[1]
        year = fn_param[2]
    if len(fn_param) == 1:
        year = fn_param[0]
        scenario = src_folder[-2]
        parameter = src_folder[-1]

    # create folders in project directory
    tgt = project_folder  + 'parameters/' + str(scenario) + '/' + str(parameter) + '/month/' + str(year) + '/'
    if os.path.exists(tgt) == False :
        os.makedirs(tgt)
    #print('--> target folder is : ', tgt)

    # create hdf file
    fn = str(parameter) + '_' + str(scenario) + '.stats.h5'
    if os.path.isfile(tgt + fn) == False:
        f = h5py.File(tgt + fn, 'w')
    else:
        f = h5py.File(tgt + fn, 'r+')
    print('HDF5 file is : ', tgt + fn)
    
    # set dtype for hdf5 table
    namesList = ['id_area', 'area', 'jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    formatList = [np.int, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float]
    ds_dt = np.dtype({'names':namesList,'formats':formatList})

    # write dataset to file
    #print('--> writing data ...')
    if not 'areas' in f:
        f.create_group('areas')
    if not str(id_area) in f['areas']:
        f['areas'].create_group(str(id_area))
    if 'table' in f['areas'][str(id_area)]:
        del f['areas'][str(id_area)]['table'] # delete table if exists
    #print(len(ds))
    #print(tgt, fn)
    
    d = f['areas'][str(id_area)].create_dataset('table', ((len(ds),)), dtype = ds_dt)
    d['id_area'] = ds['id_area']
    d['area'] = ds['area']

    dec = 1

    d['jan'] = ds['jan']/dec
    d['feb'] = ds['feb']/dec
    d['mar'] = ds['mar']/dec
    d['apr'] = ds['apr']/dec
    d['may'] = ds['may']/dec
    d['jun'] = ds['jun']/dec
    d['jul'] = ds['jul']/dec
    d['aug'] = ds['aug']/dec
    d['sep'] = ds['sep']/dec
    d['oct'] = ds['oct']/dec
    d['nov'] = ds['nov']/dec
    d['dec'] = ds['dec']/dec
    
    ds = None
    # close file
    f.close()

## @brief remove folders
def delete_folder(tgt_folder, src_param):
    #print('--> remove data')
    # get folder properties from filename
    project_folder = tgt_folder
    fn_param = ((os.path.split(src_param)[1]).split('.'))[0]
    scenario = (fn_param.split('_'))[0]
    parameter = (fn_param.split('_'))[1]

    # create folders in project directory
    tgt = project_folder  + 'parameters/' + str(scenario) + '/' + str(parameter) + '/month/'
    #print('--> remove target folder is : ', tgt)
    if os.path.exists(tgt) == True :
        shutil.rmtree(tgt)

def calculate_stats_w_mean(df, area_threshold, processor):   
    print('--> Calculate weighted mean')    
    test_id = 4974
    # ---- process all data using cupy / gpu -----
    if processor == 'gpu':     
        print('+++++++++++ GPU +++++++++++')
        # (1) remove data rows where all monthes are NULL
        indexNames = df[df.dec.isnull() & df.nov.isnull() & df.oct.isnull() & df.sep.isnull() & df.aug.isnull() & df.jul.isnull() & df.jun.isnull() & df.may.isnull() & df.apr.isnull() & df.mar.isnull() & df.feb.isnull() & df.jan.isnull()].index
        df = df.drop(indexNames, inplace=False)
        #print(df.loc[df.id_area==1015])
        # (2) create an numpy array
        cp_ar = cp.array(df)
        # (3) extract hydrotope identifiers
        ids = cp_ar[:,1].astype(int)
        # (4) extract percent_area column
        percent_area = cp_ar[:,16]
        area_size = cp_ar[:,2]
        # (5) weighting monthly data by multipling with percent_area        
        data = []
        data.append(cp.multiply(cp_ar[:,4], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,5], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,6], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,7], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,8], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,9], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,10], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,11], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,12], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,13], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,14], percent_area).astype(float))
        data.append(cp.multiply(cp_ar[:,15], percent_area).astype(float))        
        # (6) summarizing weighted data results as average monthly data
        avg_mean = []
        avg_mean.append(cp.bincount(ids, weights=area_size) ) # sum of area size
        avg_mean.append(cp.bincount(ids, weights=data[0]) ) # summarize and get area weighted means for january
        avg_mean.append(cp.bincount(ids, weights=data[1]) ) # february
        avg_mean.append(cp.bincount(ids, weights=data[2]) ) # march
        avg_mean.append(cp.bincount(ids, weights=data[3]) ) # april 
        avg_mean.append(cp.bincount(ids, weights=data[4]) ) # may 
        avg_mean.append(cp.bincount(ids, weights=data[5]) ) # juny 
        avg_mean.append(cp.bincount(ids, weights=data[6]) ) # july 
        avg_mean.append(cp.bincount(ids, weights=data[7]) ) # august 
        avg_mean.append(cp.bincount(ids, weights=data[8]) ) # september 
        avg_mean.append(cp.bincount(ids, weights=data[9]) ) # october 
        avg_mean.append(cp.bincount(ids, weights=data[10]) ) # november 
        avg_mean.append(cp.bincount(ids, weights=data[11]) ) # dececember
        avg_mean.append(cp.bincount(ids, weights=percent_area) ) # sum of percent_area
        avg_mean = cp.array(avg_mean)        
        # (7) add idarea_data
        ids = cp.array([cp.arange(0,len(avg_mean[0]))])
        avg_mean = cp.concatenate((ids, avg_mean), axis=0)
        print(avg_mean.shape)
        # (8) remove rows where sum_area_percent is below threshold
        del_idx = cp.asarray(cp.where(avg_mean[14,:] < float(area_threshold))) # get rows below threshold
        print('--> deleting ', len(del_idx[0]),' rows where area_percent is below ', area_threshold)
        avg_mean_host = np.full(avg_mean.shape, 1).astype(float) # create a numpy array filled with 1 by shape of avg_mean stored on host (and not in gpu)
        avg_mean_host = np.multiply(avg_mean_host, cp.asnumpy(avg_mean).copy()) # fill numpy array with data from cupy array
        df = pd.DataFrame(data=avg_mean_host).T
        df = df[df[0].isin(del_idx[0]) == False] # extract data where ids not in del_idx
        avg_mean = np.array(df).T
        print(avg_mean.shape)   
    
    # ---- process all data using numpy /cpu -----
    if processor == 'cpu':
        print('+++++++++++ CPU +++++++++++')
        # (1) remove data rows where all monthes are NULL
        indexNames = df[df.dec.isnull() & df.nov.isnull() & df.oct.isnull() & df.sep.isnull() & df.aug.isnull() & df.jul.isnull() & df.jun.isnull() & df.may.isnull() & df.apr.isnull() & df.mar.isnull() & df.feb.isnull() & df.jan.isnull()].index
        df = df.drop(indexNames, inplace=False)
        # (2) create an numpy array
        np_ar = np.array(df)
        # (3) extract hydrotope identifiers
        ids = np_ar[:,1].astype(int)
        # (4) extract percent_area column and area_size column
        percent_area = np_ar[:,16].astype(float)
        area_size = np_ar[:,2].astype(float)
        # (5) weighting monthly data by multipling with percent_area
        data = np.multiply([np_ar[:,4],np_ar[:,5],np_ar[:,6],np_ar[:,7],np_ar[:,8],np_ar[:,9],np_ar[:,10],np_ar[:,11],np_ar[:,12],np_ar[:,13],np_ar[:,14], np_ar[:,15]], percent_area).astype(float)
        # (6) summarizing weighted data results as average monthly data
        avg_mean = []
        avg_mean.append(np.bincount(ids, weights=area_size) ) # sum of area size
        avg_mean.append(np.bincount(ids, weights=data[0]) ) # summarize and get area weighted means for january
        avg_mean.append(np.bincount(ids, weights=data[1]) ) # february
        avg_mean.append(np.bincount(ids, weights=data[2]) ) # march
        avg_mean.append(np.bincount(ids, weights=data[3]) ) # april 
        avg_mean.append(np.bincount(ids, weights=data[4]) ) # may 
        avg_mean.append(np.bincount(ids, weights=data[5]) ) # juny 
        avg_mean.append(np.bincount(ids, weights=data[6]) ) # july 
        avg_mean.append(np.bincount(ids, weights=data[7]) ) # august 
        avg_mean.append(np.bincount(ids, weights=data[8]) ) # september 
        avg_mean.append(np.bincount(ids, weights=data[9]) ) # october 
        avg_mean.append(np.bincount(ids, weights=data[10]) ) # november 
        avg_mean.append(np.bincount(ids, weights=data[11]) ) # dececember
        avg_mean.append(np.bincount(ids, weights=percent_area) ) # sum of percent_area
        avg_mean = np.array(avg_mean)
        print(avg_mean.shape)
        # (7) add idarea_data
        ids = np.array([np.arange(0,len(avg_mean[0]))])
        avg_mean = np.concatenate((ids, avg_mean), axis=0)
        #   print('cpu', avg_mean[:,test_id])
        #   print(avg_mean[:, np.where(avg_mean[0,:] == int(test_id))[0]])
        #   print(avg_mean.shape)
        # (8) remove rows where sum_area_percent is below threshold
        """
        del_idx = np.where(avg_mean[14,:] < float(area_threshold))
        print('--> deleting ', len(del_idx[0]),' rows where area_percent is below ', area_threshold)
        avg_mean = np.delete(avg_mean, del_idx, axis=1)
        """
        #   print(avg_mean[:, np.where(avg_mean[0,:]  == int(test_id))[0]])
        #   print(avg_mean[:,  np.where(avg_mean[14,:] <  1         )[0]])
        print(avg_mean.shape)
        
    if processor == 'v_cpu':     
        print('+++++++++++ vectorized CPU +++++++++++')
        #print(df.loc[df.id_area==1015])
        # (2) create an numpy array
        np_ar = np.array(df)
        # (3) extract hydrotope identifiers
        ids = np_ar[:,1].astype(int)
        # (4) extract percent_area column
        percent_area = np_ar[:,16]
        area_size = np_ar[:,2]
        # (5) weighting monthly data by multipling with percent_area   
        t1 = time.time()
        data = (np_ar[:,4:16] * percent_area[:, np.newaxis].astype(float)).T
        print(time.time() - t1) 
        print(data.shape)
        
        t1 = time.time()       
        data = []
        data.append(np.multiply(np_ar[:,4], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,5], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,6], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,7], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,8], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,9], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,10], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,11], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,12], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,13], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,14], percent_area).astype(float))
        data.append(np.multiply(np_ar[:,15], percent_area).astype(float))  
        print(time.time() - t1) 
        print(np.array(data).shape)
        # (6) summarizing weighted data results as average monthly data
        
        
        avg_mean = []
        avg_mean.append(cp.bincount(ids, weights=area_size) ) # sum of area size
        avg_mean.append(cp.bincount(ids, weights=data[0]) ) # summarize and get area weighted means for january
        avg_mean.append(cp.bincount(ids, weights=data[1]) ) # february
        avg_mean.append(cp.bincount(ids, weights=data[2]) ) # march
        avg_mean.append(cp.bincount(ids, weights=data[3]) ) # april 
        avg_mean.append(cp.bincount(ids, weights=data[4]) ) # may 
        avg_mean.append(cp.bincount(ids, weights=data[5]) ) # juny 
        avg_mean.append(cp.bincount(ids, weights=data[6]) ) # july 
        avg_mean.append(cp.bincount(ids, weights=data[7]) ) # august 
        avg_mean.append(cp.bincount(ids, weights=data[8]) ) # september 
        avg_mean.append(cp.bincount(ids, weights=data[9]) ) # october 
        avg_mean.append(cp.bincount(ids, weights=data[10]) ) # november 
        avg_mean.append(cp.bincount(ids, weights=data[11]) ) # dececember
        avg_mean.append(cp.bincount(ids, weights=percent_area) ) # sum of percent_area
        avg_mean = cp.array(avg_mean)        
        
        sys.exit()
        # (7) add idarea_data
        ids = cp.array([cp.arange(0,len(avg_mean[0]))])
        avg_mean = cp.concatenate((ids, avg_mean), axis=0)
        print(avg_mean.shape)
        # (8) remove rows where sum_area_percent is below threshold
        del_idx = cp.asarray(cp.where(avg_mean[14,:] < float(area_threshold))) # get rows below threshold
        print('--> deleting ', len(del_idx[0]),' rows where area_percent is below ', area_threshold)
        avg_mean_host = np.full(avg_mean.shape, 1).astype(float) # create a numpy array filled with 1 by shape of avg_mean stored on host (and not in gpu)
        avg_mean_host = np.multiply(avg_mean_host, cp.asnumpy(avg_mean).copy()) # fill numpy array with data from cupy array
        df = pd.DataFrame(data=avg_mean_host).T
        df = df[df[0].isin(del_idx[0]) == False] # extract data where ids not in del_idx
        avg_mean = np.array(df).T
        print(avg_mean.shape)   
    
    if processor == 'tf_cpu':     
        print('+++++++++++ Tensorflow GPU +++++++++++')
        # (2) create an numpy array
        np_ar = np.array(df)
        tf_ar = tf.convert_to_tensor(np_ar)
        #print(tf_ar)
        # (3) extract hydrotope identifiers
        ids = np_ar[:,0].astype(int)
        tf_ids = tf.cast(tf.constant(ids),tf.int32)
        #print(tf_ids)
        # (4) extract percent_area column
        percent_area = np_ar[:,16]
        area_size = np_ar[:,2]
        tf_percent_area = tf_ar[:,16]
        tf_area_size = tf_ar[:,2]
        print(tf_area_size.dtype, tf_ids.dtype)
        # (5) weighting monthly data by multipling with percent_area   
        t1 = time.time()
        data = (np_ar[:,4:16] * percent_area[:, np.newaxis].astype(float)).T
        print('numpy cpu ', time.time() - t1) 
        t1 = time.time()
        tf_data = tf.transpose(tf_ar[:,4:16] * tf_percent_area[:, np.newaxis])#.astype(float))
        print('tensorflow gpu ',time.time() - t1) 
        print(tf_data.dtype)
        # (6) summarizing weighted data results as average monthly data
        t1 = time.time()
        avg_mean = []
        avg_mean.append(np.bincount(ids, weights=area_size) ) # sum of area size
        avg_mean.append(np.bincount(ids, weights=data[0]) ) # summarize and get area weighted means for january
        avg_mean.append(np.bincount(ids, weights=data[1]) ) # february
        avg_mean.append(np.bincount(ids, weights=data[2]) ) # march
        avg_mean.append(np.bincount(ids, weights=data[3]) ) # april 
        avg_mean.append(np.bincount(ids, weights=data[4]) ) # may 
        avg_mean.append(np.bincount(ids, weights=data[5]) ) # juny 
        avg_mean.append(np.bincount(ids, weights=data[6]) ) # july 
        avg_mean.append(np.bincount(ids, weights=data[7]) ) # august 
        avg_mean.append(np.bincount(ids, weights=data[8]) ) # september 
        avg_mean.append(np.bincount(ids, weights=data[9]) ) # october 
        avg_mean.append(np.bincount(ids, weights=data[10]) ) # november 
        avg_mean.append(np.bincount(ids, weights=data[11]) ) # dececember
        avg_mean.append(np.bincount(ids, weights=percent_area) ) # sum of percent_area
        print('numpy cpu ', time.time() - t1) 
        avg_mean = np.array(avg_mean)
        #print(avg_mean)
        t1 = time.time()
        tf_avg_mean = []
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_area_size) ) # sum of area size
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[0]) ) # summarize and get area weighted means for january
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[1]) ) # february
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[2]) ) # march
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[3]) ) # april 
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[4]) ) # may 
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[5]) ) # juny 
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[6]) ) # july 
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[7]) ) # august 
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[8]) ) # september 
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[9]) ) # october 
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[10]) ) # november 
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_data[11]) ) # dececember
        tf_avg_mean.append(tf.math.bincount(tf_ids, weights=tf_percent_area) ) # sum of percent_area
        print('tensorflow gpu ',time.time() - t1) 
        
        print(np.bincount(ids, weights=area_size) )
        print(tf.math.bincount(tf.cast(tf.constant(ids),tf.int32), weights=tf_area_size, axis=0))
        #print(tf_ids)
        #values = tf.constant([1,1,2,3,2,4,4,5])
        #print(tf.math.bincount(values))
        
        tf_avg_mean = np.array(tf_avg_mean)
        #print(avg_mean)
        #print(tf_avg_mean)
        print(avg_mean.shape, tf_avg_mean.shape)
        #print(data.shape)
        
        sys.exit()
    
    # create pandas dataframe
    df_w_mean = pd.DataFrame({
        'id_area': avg_mean[0,:],
        'area': avg_mean[1,:].astype(int),
        'jan': avg_mean[2,:],
        'feb': avg_mean[3,:],
        'mar': avg_mean[4,:],
        'apr': avg_mean[5,:],
        'may': avg_mean[6,:],
        'jun': avg_mean[7,:],
        'jul': avg_mean[8,:],
        'aug': avg_mean[9,:],
        'sep': avg_mean[10,:],
        'oct': avg_mean[11,:],
        'nov': avg_mean[12,:],
        'dec': avg_mean[13,:]
    })
    #print(df_w_mean)
    #sys.exit()
    return df_w_mean
    
    


################################################################
# main program call
if __name__ ==  '__main__':
    # start program
    print ("--> starting calculation of statistics...")
    ##################################
    ###### variable definitions ######
    # cpu or gpu
    processor = 'cpu'    
    # project whp, gwn
    project = 'whp'
    # start_yer, end_year
    startYear, endYear = None, None
    # id_area restriction
    idArea = None
    # source parameter restriction
    srcParam = None #'10_10'    
    ##################################
    
    # container for area sizes loaded from database
    df_areasize = {}
        
    # get sources and target filenames
    area_folder, src_folder, src_type, tgt_folder, p = get_arguments()
    
    # check if folders and files exists
    check_file_exists([area_folder, src_folder, tgt_folder])

    # read hydrotopes
    areaContainer = read_areas( area_folder, idArea )

    # get files in folder recursively
    files = read_hydrotopes( src_folder, srcParam, startYear, endYear )   
    print('count of files: ', len(files))
    #sys.exit()
    # loop through parameter files
    i = 0
    for src_param in files:
        print('------------------------------------------------')
        print(i, ' von ', len(files))
        s0 = time.time()
        print(src_param)
        # reade parameter file and get an pandas dataframe
        if project == 'whp':
            ds_parameter = read_parameter_whp( src_param, src_type )
        elif project == 'gwn':
            ds_parameter = read_parameter( src_param, src_type )
        
        # loop through areas
        for area in areaContainer:
            # get id_area
            id_area = area['idArea']
            print('idArea ', id_area)
            
            # create dataframe for total area sizes if not exists
            if not id_area in df_areasize:
                df_areasize[id_area] = pd.DataFrame()
                
            # get hydrotope data
            ds_hydrotop = area['df']
            
            # get total area size for areas
            if df_areasize[id_area].shape[0] == 0:
                df_areasize[id_area] = get_area_sizes(id_area, project)
                
            # do spatial join between intersection data and parameter data
            df = do_spatial_join( ds_hydrotop, ds_parameter, df_areasize[id_area] )
            
            # do statistic calculation
            if df.empty:
                df = pd.DataFrame(columns=(df.columns))
            else:
                """
                s =  time.time()
                df0 = calculate_stats_w_mean(df, p, 'gpu')
                e = time.time()
                print('needed time gpu ', e - s)
                """
                """
                s =  time.time()
                #vectorized = np.vectorize(calculate_stats_w_mean)
                #df = vectorized(df, p, 'cpu')
                df = calculate_stats_w_mean(df, p, 'cpu')
                e = time.time()
                print('needed time cpu ', e - s)
                """
                s =  time.time()
                # (1) remove data rows where all monthes are NULL
                indexNames = df[df.dec.isnull() & df.nov.isnull() & df.oct.isnull() & df.sep.isnull() & df.aug.isnull() & df.jul.isnull() & df.jun.isnull() & df.may.isnull() & df.apr.isnull() & df.mar.isnull() & df.feb.isnull() & df.jan.isnull()].index
                df = df.drop(indexNames, inplace=False)
                print(df.columns)
                #print(df)
                df = calculate_stats_w_mean(df, p, 'tf_cpu')
                e = time.time()
                print('needed time cpu ', e - s)
                #sys.exit()
            # save dataset
            #save_data(tgt_folder, src_param, df, id_area)
        e0 = time.time()
        print('needed time for on year and parameter, all areas ', e0 - s0, ' | estimated time left ', ((e0 - s0) * (len(files) - i))/3600, ' hours')
        print('=====================================================')
        i = i + 1
