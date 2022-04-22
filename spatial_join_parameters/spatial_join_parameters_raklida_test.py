## @package spatial_join_parameters
# Link hdf5 parameter files to areas based on spatial intersection between hydrotopes and area shapefiles.
#
# Calculate statistics for a parameter grouped by area.

import os
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
# @returns src_path_hydrotop path/filename of intersection between hydrotopes and areas/regions
# @returns src_parameter path/filename of parameter hdf5 file
# @returns tgt_file path/filename of output hdf5 statistic file
def get_arguments():
    print("--> get caller arguments")
    src_folder_hydrotop, src_folder_param, src_param, tgt_param, tgt_folder, p = None, None, None, None, None, None
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '-src_folder_hydrotop':
            i = i + 1
            src_folder_hydrotop = sys.argv[i]
        if arg == '-src_folder_param':
            i = i + 1
            src_folder_param = sys.argv[i]
        if arg == '-tgt_folder':
            i = i + 1
            tgt_folder = sys.argv[i]
        if arg == '-src_param':
            i = i + 1
            src_param = sys.argv[i]
        if arg == '-tgt_param':
            i = i + 1
            tgt_param = sys.argv[i]
        if arg == '-p':
            i = i + 1
            p = sys.argv[i]
            
        i = i + 1

    if src_folder_hydrotop == None or src_folder_param == None or src_param == None or tgt_param == None or tgt_folder == None or p == None :
        print("ERROR: arguments of program call missing")
        sys.exit()

    return src_folder_hydrotop, src_folder_param, src_param, tgt_folder, tgt_param, p

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
def read_areas(area_folder):
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
        #df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']].replace(-9999, np.nan)
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
    #print(df_intersect.shape, df_param.shape, df_join.shape)
    #print(len(df_join['id_area'].unique()), df_join['id_area'].unique().min(), df_join['id_area'].unique().max())
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
    
    #print('1--> :',(df_intersect.shape)[0], (df_join.shape)[0],  df_param.shape)
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
    #print('--> Calculate weighted mean')    
    # (1) remove data rows where all monthes are NULL
    #print(df.loc[df['id_area']==4996])
    indexNames = df[df.dec.isnull() & df.nov.isnull() & df.oct.isnull() & df.sep.isnull() & df.aug.isnull() & df.jul.isnull() & df.jun.isnull() & df.may.isnull() & df.apr.isnull() & df.mar.isnull() & df.feb.isnull() & df.jan.isnull()].index
    df = df.drop(indexNames, inplace=False)

    #print('3--> df.isnull() ',df.loc[df.isnull().jun==True])
    #print('5--> df.isnull().sum() ', df.isnull().sum())
    #print('6--> df.isnull().sum().sum() ', df.isnull().sum().sum())
    #sys.exit()
    
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
        print('kkkkkkkkkkkk')
        
        #print('--> 0 ', df.loc[df['id_area']==2246])
        print('1--> df.isnull() ', df.isnull())
        #print('2--> df.isnull() ', df.isnull().loc[df.isnull()['id_area']==11])
        print('2--> df.isnull() ', df.loc[df['id_area']==11])
        #print('2--> df.isnull() ', df.isnull().loc[df.isnull()['id_area']==5876])
        print('3--> df.isnull() ',df.loc[df.isnull().dec==True])
        print('5--> df.isnull().sum() ', df.isnull().sum())
        
        df['jan'] = df['jan'].fillna(0)
        df['feb'] = df['feb'].fillna(0)
        df['mar'] = df['mar'].fillna(0)
        df['apr'] = df['apr'].fillna(0)
        df['may'] = df['may'].fillna(0)
        df['jun'] = df['jun'].fillna(0)
        df['jul'] = df['jul'].fillna(0)
        df['aug'] = df['aug'].fillna(0)
        df['sep'] = df['sep'].fillna(0)
        df['oct'] = df['oct'].fillna(0)
        df['nov'] = df['nov'].fillna(0)
        df['dec'] = df['dec'].fillna(0)
        
        print('5--> df.isnull().sum() ', df.isnull().sum())
        
        print('6--> df.isnull() ', df.loc[df['id_area']==11])
        
        sys.exit()
        

## @brief Query original area sizes of areas from database
def get_area_sizes(id_area):
    """get area sizes for an idarea"""
    #print('--> get total area from database, id_area --> ' + str(id_area))
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
    #print(sql)
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
    src_folder_hydrotop, src_folder_param, src_param, tgt_folder, tgt_param, p = get_arguments()

    # check if folders and files exists
    check_file_exists([src_folder_hydrotop, src_folder_param, tgt_folder])

    # read hydrotopes
    areaContainer = read_areas( src_folder_hydrotop)


    files = read_hydrotopes( src_folder_param, None, None, None )
    #print('files --> ', files)
    #sys.exit()
    
    # loop through parameter files
    l = len(files)
    for src_file_param in files:
        
        fn_param = ((os.path.split(src_file_param)[1]).split('.'))[0]
        scenario = (fn_param.split('_'))[0]
        parameter = (fn_param.split('_'))[1]
        year = (fn_param.split('_'))[2]
        
        #print('------------------------------------------------')
        #print('src_file_param --> ', src_file_param)
        #print('src_param --> ', src_param)
        #print('scenario --> ', scenario)
        #print('parameter --> ', parameter)
        
        if src_param == parameter:
            
            print('--> src_file_param --> ', src_file_param)
            #print('--> src_param --> ', src_param)
            #print('--> scenario --> ', scenario)
            #print('--> parameter --> ', parameter)
            #sys.exit()
            
            # read parameter
            df_param = read_parameter( src_file_param, '')
           # print(df_param.shape)
            
            # loop through areas
            for area in areaContainer:
                # get id_area
                id_area = area['idArea']
                #print('--> idArea', id_area)
                
                #if int(id_area) == 25 and year == '1962':
                    
                # create dataframe for total area sizes if not exists
                if not id_area in df_areasize:
                    df_areasize[id_area] = pd.DataFrame()
                # get hydrotope data
                df_hydrotop = area['df']
                
                # get total area size for areas
                if df_areasize[id_area].shape[0] == 0:
                    df_areasize[id_area] = get_area_sizes(id_area)
                    
                if int(id_area) == 25:
                    
                    # do spatial join between intersection data and parameter data
                    df = do_spatial_join( df_hydrotop, df_param, df_areasize[id_area] )
                               
                    # do statistic calculation
                    if df.empty:
                        df = pd.DataFrame(columns=(df.columns))
                    else:
                        df = calculate_stats_w_mean(df, p)
                        
                        #sys.exit()
                    
                    # save statistic data
                    
                    print(tgt_folder, tgt_param)
        
            df = None
