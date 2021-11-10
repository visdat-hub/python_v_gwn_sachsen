## @package spatial_join_parameters
# Link hdf5 parameter files to areas based on spatial intersection between hydrotopes and area shapefiles.
#
# Calculate statistics for a parameter grouped by area.

import os
import shutil
import sys
import h5py
import pandas as pd
import numpy as np
import time
sys.path.append(os.getcwd() + '/..')
from pg.pg import db_connector as pg

## @brief Get command line arguments.
#
# python3 test_datasets.py -src_h /path/fname.shp -src_type kliwes -tgt /path/fname.shp
#
# python3 ./spatial_join_parameters/spatial_join_parameters.py -src_h /home/stefan/ -src_p /home/stefan/whp_test_param.h5 -src_type kliwes -tgt /var/www/daten_stb/wasserhaushaltsportal/

#
# python3 /mnt/visdat/Projekte/2019/wasserhaushaltsportal/dev/python/spatial_join_parameters/spatial_join_parameters.py -src_h /mnt/visdat/Projekte/2019/wasserhaushaltsportal/daten/intersect_areas/ -src_p /mnt/visdat/Projekte/2019/wasserhaushaltsportal/übergabe\ TUD/Testdaten\ 14-09-2020/ -src_type kliwes -tgt /var/prog/daten_stb/wasserhaushaltsportal/
# src_type is kliwes, difga etc.
#
# @returns src_hydrotop path/filename of intersection between hydrotopes and areas/regions
# @returns src_parameter path/filename of parameter hdf5 file
# @returns tgt_file path/filename of output hdf5 statistic file
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

## @brief Check if user defined files / folders and pathes exists.
#
# sys.exit() if a file or a path not exists
#
# @param filelist comma separeted list [fname, fname, fname]
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

## @brief Read area netcdf dataset
#
# The area dataset represents the intersection between hydrotopes and administrativ areas
#
# @param area_folder Filepath of area datasets
# @return df_param A Pandas DataFrame
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
        #if id_area_restricted != None:
        #    if int(idArea) == int(id_area_restricted):
        #        print('--> load area: ', f)
        #        f_intersect = h5py.File(f, 'r')
        #        df_intersect = pd.DataFrame({'id_hydrotop' : f_intersect['idhydrotope_org'][:], 'id_area' : f_intersect['idarea_data'][:], 'area' : f_intersect['area'][:]})
        #        areaContainer.append({'idArea' : idArea, 'df' : df_intersect})
        #        print('--> done')
        #        f_intersect.close()
        #else:
        print('--> load area: ', f)
        print('--> idArea: ', idArea)
        f_intersect = h5py.File(f, 'r')
        df_intersect = pd.DataFrame({'id_hydrotop' : f_intersect['idhydrotope_org'][:], 'id_area' : f_intersect['idarea_data'][:], 'area' : f_intersect['area'][:]})
        areaContainer.append({'idArea' : idArea, 'df' : df_intersect})
        print('--> done')
        f_intersect.close()
    #print(areaContainer)
    return areaContainer  

## @brief get file list
def read_hydrotopes(src_folder, id_param_restricted, start_year, end_year):
    files = []
    print('-->src_folder',src_folder)
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
                else:
                    files.append(os.path.join(r, file))
    #print(files)
    return files

## @brief Read parameter hdf5-dataset
#
# @param src_param Filepath of parameter dataset for one year
# @return df_param A Pandas DataFrame
def read_parameter(src_param, src_type):
    # read parameter dataset
    print('--> read parameter ...', src_param)
    df_param = pd.read_hdf(src_param, key='table').reset_index()

    if src_type == 'difga':
        df_param['index'] = df_param['index'].astype(str)
    else:
        df_param['index'] = df_param['index'].astype(int)

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
        df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']].replace(-9999, np.nan)
        #sys.exit()
    #print(df_param)
    #sys.exit()
    return df_param

## @brief Perform the spatial join between intersection data and parameter dataset
#
# The structure of the parameter dataset (hdf5 format)
#
# /data/table -> index, values_block_0
#
# values_block_0 is an array that store monthly values
#
# @param df_intersect Dataset of areas
# @param df_param Dataset of parameter
# @return df_join Joined dataset as pandas object (dataframe)
def do_spatial_join(df_intersect, df_param, df_areasize):
    #print('--> perform spatial join')
    # merge datasets
    #print('--> merge datasets')
    df_join = pd.merge(df_intersect, df_param, left_on='id_hydrotop', right_on='index' ,how='left', sort=False)
    print(df_intersect.shape, df_param.shape, df_join.shape)
    print(len(df_join['id_area'].unique()), df_join['id_area'].unique().min(), df_join['id_area'].unique().max())
    # get sum of area sizes
    #df_area = df_join.groupby('id_area')['area'].sum().reset_index().rename(columns={'area':'area_sum'})
    # merge df_area and df
    #df_join = pd.merge(df_join, df_area, how='inner', left_on='id_area', right_on='id_area')
    df_join['id_area'] = df_join['id_area'].astype(int)
    df_areasize['idarea_data'] = df_areasize['idarea_data'].astype(int)
    df_join = pd.merge(df_join, df_areasize, how='inner', left_on='id_area', right_on='idarea_data')
    # calculate percent
    df_join['percent_area'] = df_join['area'] / df_join['area_size']
    # drop columns
    df_join = df_join.drop(columns=['idarea','orgid','idarea_data','area_size'])
    
    print('1--> :',(df_intersect.shape)[0], (df_join.shape)[0],  df_param.shape)
    if (df_join.shape)[0] != (df_intersect.shape)[0]:
        print('ERROR : different shapes sizes of df_intersect and df_join')
        print('2--> :',(df_intersect.shape)[0], (df_join.shape)[0],  df_param.shape) 
        sys.exit()

    df_param = None
    # drop rows where index is nan
    #df_join = df_join.dropna(axis=0, subset=['index'])
    
    # test for negative area sizes
    df_area_0 = df_join.loc[df_join['area']<0]
    if (df_area_0.shape)[0] > 0:
        print('ERROR : There negative area sizes')
        print('-->',df_area_0.shape)
        print(df_area_0)
        sys.exit()

    #print(df_join)
    #print(df_join.loc[df_join.id_area == 4996])
    return df_join

## @brief Calculate weighted mean grouped by areas/region
#
# Mean is weighted by area size.
#
# @param df Pandas DataFrame
# @return df_w_mean Pandas DataFrame
def calculate_stats_w_mean(df, area_threshold):   
    print('--> Calculate weighted mean')    
    # (1) remove data rows where all monthes are NULL
    #print(df.loc[df['id_area']==4996])
    indexNames = df[df.dec.isnull() & df.nov.isnull() & df.oct.isnull() & df.sep.isnull() & df.aug.isnull() & df.jul.isnull() & df.jun.isnull() & df.may.isnull() & df.apr.isnull() & df.mar.isnull() & df.feb.isnull() & df.jan.isnull()].index
    df = df.drop(indexNames, inplace=False)
    #print(df.loc[df['id_area']==4996])
    
    # (2) multiply value and area
    df['jan'] = df['jan'] * df['area']
    df['feb'] = df['feb'] * df['area']
    df['mar'] = df['mar'] * df['area']
    df['apr'] = df['apr'] * df['area']
    df['may'] = df['may'] * df['area']
    df['jun'] = df['jun'] * df['area']
    df['jul'] = df['jul'] * df['area']
    df['aug'] = df['aug'] * df['area']
    df['sep'] = df['sep'] * df['area']
    df['oct'] = df['oct'] * df['area']
    df['nov'] = df['nov'] * df['area']
    df['dec'] = df['dec'] * df['area']

    #print(df.loc[df.id_hydrotop=='ammelsdo'])
    # (3) calculate area weighted means
    # if there are nan (NULL) values in dataset
    if df.isnull().sum().sum() > 0:
        #print('kkkkkkkkkkkk')
        # count data without NULL values grouped by idarea_data
        df_count = df.groupby('id_area')['area','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec','percent_area'].count().reset_index()
        for m in ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']:
            df_count[m] = df_count[m].where(df_count[m] > 0)
        #print(df_count.reset_index().loc[df_count.reset_index().id_area==0])
        # get the sum of percent_area values for each month where values are not NULL
        df_area_percent_month = pd.DataFrame()
        t = df.groupby('id_area')['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec','percent_area']
        df_area_percent_month['jan'] = t.apply(lambda x: x[x['jan'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['feb'] = t.apply(lambda x: x[x['feb'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['mar'] = t.apply(lambda x: x[x['mar'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['apr'] = t.apply(lambda x: x[x['apr'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['may'] = t.apply(lambda x: x[x['may'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['jun'] = t.apply(lambda x: x[x['jun'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['jul'] = t.apply(lambda x: x[x['jul'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['aug'] = t.apply(lambda x: x[x['aug'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['sep'] = t.apply(lambda x: x[x['sep'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['oct'] = t.apply(lambda x: x[x['oct'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['nov'] = t.apply(lambda x: x[x['nov'].isnull() == False]['percent_area'].sum())
        df_area_percent_month['dec'] = t.apply(lambda x: x[x['dec'].isnull() == False]['percent_area'].sum())
        #print(df_area_percent_month.reset_index().loc[df_area_percent_month.reset_index().id_area==0])
        # create a mask to exclude areas below the area_threshold
        df_area_mask = df_area_percent_month.where(df_area_percent_month >= float(area_threshold)).reset_index()
        #print(df_area_mask)
        # summarize parameter values grouped by idarea_data        
        df_stat2 = df.groupby('id_area')['area','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec','percent_area'].apply (lambda x: x.sum(skipna=True)).reset_index()
        #print(df_stat2.reset_index().loc[df_stat2.reset_index().id_area==0])
        # restrict dataset to values where are no NULL values
        df_stat = df_stat2.where(df_count.isnull()==False)
        #print(df_stat.reset_index().loc[df_stat.reset_index().id_area==0])
        # join df_stat and df_area_mask
        df_stat = pd.merge(df_stat, df_area_mask, how='inner', left_on='id_area', right_on='id_area')
        #print(df_area_mask.loc[df_area_mask.id_area == 0])
        #print(df_stat.reset_index().loc[df_stat.reset_index().id_area==0])
        #print('+++++++++++++')
        # get weighted mean of value
        df_stat['jan'] = (df_stat['jan_x'] / df_stat['area'] / df_stat['jan_y']).round(2)
        df_stat['feb'] = (df_stat['feb_x'] / df_stat['area'] / df_stat['feb_y']).round(2)
        df_stat['mar'] = (df_stat['mar_x'] / df_stat['area'] / df_stat['mar_y']).round(2)
        df_stat['apr'] = (df_stat['apr_x'] / df_stat['area'] / df_stat['apr_y']).round(2)
        df_stat['may'] = (df_stat['may_x'] / df_stat['area'] / df_stat['may_y']).round(2)
        df_stat['jun'] = (df_stat['jun_x'] / df_stat['area'] / df_stat['jun_y']).round(2)
        df_stat['jul'] = (df_stat['jul_x'] / df_stat['area'] / df_stat['jul_y']).round(2)
        df_stat['aug'] = (df_stat['aug_x'] / df_stat['area'] / df_stat['aug_y']).round(2)
        df_stat['sep'] = (df_stat['sep_x'] / df_stat['area'] / df_stat['sep_y']).round(2)
        df_stat['oct'] = (df_stat['oct_x'] / df_stat['area'] / df_stat['oct_y']).round(2)
        df_stat['nov'] = (df_stat['nov_x'] / df_stat['area'] / df_stat['nov_y']).round(2)
        df_stat['dec'] = (df_stat['dec_x'] / df_stat['area'] / df_stat['dec_y']).round(2)
        #print(df_stat.reset_index().loc[df_stat.reset_index().id_area==0])
        # reset index
        df_stat = df_stat.reset_index()    
        #print(df_stat)
        #sys.exit()
    elif df.empty == False:
        # standard procedure if there are no NULL values
        #print('huhuhu')
        df_stat = df.groupby('id_area')['area','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec','percent_area'].apply (lambda x: x.sum(skipna=False)).reset_index()
        df_stat = df_stat[df_stat.percent_area >= float(area_threshold)]
        
        df_stat['jan'] = (df_stat['jan'] / df_stat['area']).round(2)
        df_stat['feb'] = (df_stat['feb'] / df_stat['area']).round(2)
        df_stat['mar'] = (df_stat['mar'] / df_stat['area']).round(2)
        df_stat['apr'] = (df_stat['apr'] / df_stat['area']).round(2)
        df_stat['may'] = (df_stat['may'] / df_stat['area']).round(2)
        df_stat['jun'] = (df_stat['jun'] / df_stat['area']).round(2)
        df_stat['jul'] = (df_stat['jul'] / df_stat['area']).round(2)
        df_stat['aug'] = (df_stat['aug'] / df_stat['area']).round(2)
        df_stat['sep'] = (df_stat['sep'] / df_stat['area']).round(2)
        df_stat['oct'] = (df_stat['oct'] / df_stat['area']).round(2)
        df_stat['nov'] = (df_stat['nov'] / df_stat['area']).round(2)
        df_stat['dec'] = (df_stat['dec'] / df_stat['area']).round(2)
        
        df_stat = df_stat.reset_index()
    else:
        # an empty result dataframe
        df_stat = pd.DataFrame(columns={'id_area','area','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'})
    
    #print(df_stat.loc[df_stat.id_area==0])
    #print(df_stat)
    #print('##########', df_stat.iloc[4996])
    #print('------> df_stat', df_stat)
    # reduce columns
    df_w_mean = pd.DataFrame({
        'id_area': df_stat['id_area'],
        'area': df_stat['area'].astype(int),
        'jan': df_stat['jan'],
        'feb': df_stat['feb'],
        'mar': df_stat['mar'],
        'apr': df_stat['apr'],
        'may': df_stat['may'],
        'jun': df_stat['jun'],
        'jul': df_stat['jul'],
        'aug': df_stat['aug'],
        'sep': df_stat['sep'],
        'oct': df_stat['oct'],
        'nov': df_stat['nov'],
        'dec': df_stat['dec']
    })
    #print('##########', df_w_mean.iloc[4996])    
    # set first idarea_data to 0 
    df_w_mean['id_area'] = df_w_mean['id_area'].where(df_w_mean['id_area'] > 0, 0)
    #sys.exit()
    return df_w_mean

## @brief Save values to hdf5 file considering the folder structure for daten_stb
#
# @param tgt_folder Project folder
# @param src_param Filename of parameter file
# @param ds Pandas DataFrame
# @param id_area INT The identifier of region / area
def save_data(tgt_folder, src_param, ds, id_area):
    print('--> save data')
    # get folder properties from filename
    project_folder = tgt_folder
    fn_param = ((os.path.split(src_param)[1]).split('.'))[0]
    scenario = (fn_param.split('_'))[0]
    parameter = (fn_param.split('_'))[1]
    year = (fn_param.split('_'))[2]

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
    print('--> HDF5 file is : ', tgt + fn)

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

## @brief Query original area sizes of areas from database
def get_area_sizes(id_area):
    """get area sizes for an idarea"""
    print('--> get total area from database, id_area --> ' + str(id_area))
    df_out = pd.DataFrame()
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
    print(sql)
    db.dbClose()
    if res[1] > 0:
        df_out = pd.DataFrame(np.array(res[0]), columns={0,1,2,3}).rename(columns={0:'idarea',1:'orgid',2:'idarea_data',3:'area_size'})
    else:
        sys.exit('ERROR: area not found in database, id_area ' + str(id_area))

    return df_out

################################################################
# main program call
if __name__ ==  '__main__':
    # start program
    t0 = time.time()
    print ("--> starting calculation of statistics...")
    df_areasize = {}

    # get sources and target filenames
    area_folder, src_folder, src_type, tgt_folder, p = get_arguments()

    # check if folders and files exists
    check_file_exists([area_folder, src_folder, tgt_folder])

    # read hydrotopes
    areaContainer = read_areas( area_folder , 11)

    # get files in folder recursively
    #files = []
    #for r, d, f in os.walk(src_folder):
    #    for file in f:
    #        if '.h5' in file:
    #            files.append(os.path.join(r, file))

    #for src_param in files:
        #print('++++'  + src_param)
    #    delete_folder(tgt_folder, src_param)
        
    files = read_hydrotopes( src_folder, None, None, None )
    
    # loop through parameter files
    i = 0
    l = len(files)
    for src_param in files:
        print('------------------------------------------------')
        print('++++'  + src_param)
        
        i = i + 1
        #if '/10_10' in src_param:
            #print('--> current file : ' + src_param, i)
        #print('--> file ', i, ' von ', l)
        # read parameter
        df_param = read_parameter( src_param, src_type )
        #print(df_param.shape)
        
        # loop through areas
        for area in areaContainer:
            # get id_area
            id_area = area['idArea']
            print('idArea', id_area)
            
            # create dataframe for total area sizes if not exists
            if not id_area in df_areasize:
                df_areasize[id_area] = pd.DataFrame()
            # get hydrotope data
            df_hydrotop = area['df']
            
            # get total area size for areas
            if df_areasize[id_area].shape[0] == 0:
                df_areasize[id_area] = get_area_sizes(id_area)
            
            # do spatial join between intersection data and parameter data
            df = do_spatial_join( df_hydrotop, df_param, df_areasize[id_area] )
                       
            # do statistic calculation
            if df.empty:
                df = pd.DataFrame(columns=(df.columns))
            else:
                df = calculate_stats_w_mean(df, p)
            #print(df[df.id_area==4996])
            # save statistic data
            save_data(tgt_folder, src_param, df, id_area)
            #break
        
        df = None
