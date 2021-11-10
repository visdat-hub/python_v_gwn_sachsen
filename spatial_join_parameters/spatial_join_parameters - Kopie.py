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
    src_hydrotop, src_param, src_type, tgt_file = None, None, None, None
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
        i = i + 1

    if src_hydrotop == None or src_param == None or tgt_file == None or src_type == None :
        print("ERROR: arguments of program call missing")
        sys.exit()

    return src_hydrotop, src_param, src_type, tgt_file

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
        print('--> load area: ', f)
        idArea = os.path.split(f)[1].split('.')[-2]
        print('--> idArea: ', idArea)
        f_intersect = h5py.File(f, 'r')
        df_intersect = pd.DataFrame({'id_hydrotop' : f_intersect['idhydrotope_org'][:], 'id_area' : f_intersect['idarea_data'][:], 'area' : f_intersect['area'][:]})
        #df_intersect = pd.DataFrame({'id_hydrotop' : f_intersect['efl_id'][:], 'id_area' : f_intersect['id'][:], 'area' : f_intersect['area'][:]})
        areaContainer.append({'idArea' : idArea, 'df' : df_intersect})
        print('--> done')
        # close h5py objects
        f_intersect.close()

    return areaContainer

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


    # get a column for each month
    df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param.values_block_0.str.split(expand=True,)
    # set nan values
    # remove column values_block_0
    df_param = df_param.drop(columns=['values_block_0'])
    df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']].astype(float)
    df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']].replace(-9999, np.nan)

    """df_param['jan'] = df_param['jan'].replace(-9999, np.nan)
    df_param['feb'] = df_param['feb'].replace(-9999, np.nan)
    df_param['mar'] = df_param['mar'].replace(-9999, np.nan)
    df_param['apr'] = df_param['apr'].replace(-9999, np.nan)
    df_param['may'] = df_param['may'].replace(-9999, np.nan)
    df_param['jun'] = df_param['jun'].replace(-9999, np.nan)
    df_param['jul'] = df_param['jul'].replace(-9999, np.nan)
    df_param['aug'] = df_param['aug'].replace(-9999, np.nan)
    df_param['sep'] = df_param['sep'].replace(-9999, np.nan)
    df_param['oct'] = df_param['oct'].replace(-9999, np.nan)
    df_param['nov'] = df_param['nov'].replace(-9999, np.nan)
    df_param['dec'] = df_param['dec'].replace(-9999, np.nan)"""

    """df_param['jan'] = df_param['jan'].astype(float)
    df_param['feb'] = df_param['feb'].astype(float)
    df_param['mar'] = df_param['mar'].astype(float)
    df_param['apr'] = df_param['apr'].astype(float)
    df_param['may'] = df_param['may'].astype(float)
    df_param['jun'] = df_param['jun'].astype(float)
    df_param['jul'] = df_param['jul'].astype(float)
    df_param['aug'] = df_param['aug'].astype(float)
    df_param['sep'] = df_param['sep'].astype(float)
    df_param['oct'] = df_param['oct'].astype(float)
    df_param['nov'] = df_param['nov'].astype(float)
    df_param['dec'] = df_param['dec'].astype(float)"""

    #print('--> read parameter ...', df_param)
    #print(df_param['index'].astype(int))
    #print(df_param.iloc[:, [1]].astype(int))
    #print('df_param min max ', df_param['index'].astype(int).min(),df_param['index'].astype(int).max())
    #print('--> done')

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
def do_spatial_join(df_intersect, df_param):
    print('--> perform spatial join')

    # merge datasets
    print('--> merge datasets')
    df_join = pd.merge(df_intersect, df_param, left_on='id_hydrotop', right_on='index' ,how='left', sort=False)
    #print('--> df_intersect, ', df_intersect)
    #print('--> df_param, ', df_param)
    #print('--> df_join, ', df_join)
    #print('--> done, ', df_join.shape)
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

    # return df_join
    return df_join

## @brief Calculate weighted mean grouped by areas/region
#
# Mean is weighted by area size.
#
# @param df Pandas DataFrame
# @return df_w_mean Pandas DataFrame
def calculate_stats_w_mean(df):
    print('--> Calculate weighted mean')
    # multiply value and area
    
    #print('df[jan]', df['jan'])
    #print('df[area]', df['area'])
    #print('df[jan]', df['jan'].dtypes)
    #print('df[area]', df['area'].dtypes)
    #print('df[dec]', df['dec'])
 
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
    #print('df', df)
    # get sum of area
    df_stat = df.groupby('id_area')['area','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].agg(['sum'])
    #df_stat = df.groupby('id_area')['area','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].apply(lambda x: x.sum(skipna=False)).reset_index()
    # get weighted mean of value
    df_stat['jan'] = (df_stat['jan']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['feb'] = (df_stat['feb']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['mar'] = (df_stat['mar']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['apr'] = (df_stat['apr']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['may'] = (df_stat['may']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['jun'] = (df_stat['jun']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['jul'] = (df_stat['jul']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['aug'] = (df_stat['aug']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['sep'] = (df_stat['sep']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['oct'] = (df_stat['oct']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['nov'] = (df_stat['nov']['sum'] / df_stat['area']['sum']).round(2)
    df_stat['dec'] = (df_stat['dec']['sum'] / df_stat['area']['sum']).round(2)
    """
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
    """    
df_stat = df_stat.reset_index()
    
    #print(df_stat.loc[df_stat['id_area'] == 0])
    #sys.exit()

    #print('----------------------> df_stat', df_stat)
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
    #df_w_mean[df_w_mean ==0] = np.nan
    #print(df_w_mean)
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
    print('--> target folder is : ', tgt)

    # create hdf file
    fn = str(parameter) + '_' + str(scenario) + '.stats.h5'
    if os.path.isfile(tgt + fn) == False:
        f = h5py.File(tgt + fn, 'w')
    else:
        f = h5py.File(tgt + fn, 'r+')
    print('--> HDF5 file is : ', fn)

    # set dtype for hdf5 table
    namesList = ['id_area', 'area', 'jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    formatList = [np.int, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float, np.float]
    ds_dt = np.dtype({'names':namesList,'formats':formatList})

    # write dataset to file
    print('--> writing data ...')
    if not 'areas' in f:
        f.create_group('areas')
    if not str(id_area) in f['areas']:
        f['areas'].create_group(str(id_area))
    if 'table' in f['areas'][str(id_area)]:
        del f['areas'][str(id_area)]['table'] # delete table if exists
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

if __name__ ==  '__main__':
    # start program
    t0 = time.time()
    print ("--> starting calculation of statistics...")

    # get sources and target filenames
    area_folder, src_folder, src_type, tgt_folder = get_arguments()

    # check if folders and files exists
    check_file_exists([area_folder, src_folder, tgt_folder])

    # read hydrotopes
    areaContainer = read_areas( area_folder )

    # get files in folder recursively
    files = []
    for r, d, f in os.walk(src_folder):
        for file in f:
            if '.h5' in file:
                files.append(os.path.join(r, file))

    # loop through parameter files
    i = 0
    l = len(files)
    for src_param in files:
        i = i + 1
        print('--> current file : ' + src_param, i)
        print('--> file ', i, ' von ', l)
        # read parameter
        df_param = read_parameter( src_param, src_type )

        # loop through areas
        for area in areaContainer:
            #print(area)
            #sys.exit()
            id_area = area['idArea']
            df_hydrotop = area['df']
            print('--> idArea: ', id_area)
            #print(df_hydrotop.loc[df_hydrotop['id_area'] == 0])
            # do spatial join between intersection data and parameter data
            df = do_spatial_join( df_hydrotop, df_param )

            # do statistic calculation
            df = calculate_stats_w_mean(df)

            # save statistic data
            save_data(tgt_folder, src_param, df, id_area)
        ctime = time.time()
        dt = ctime - t0

        print('elapsed time: ', time.strftime("%H:%M:%S", time.gmtime(dt)))
        print('remaining time: ', time.strftime("%H:%M:%S", time.gmtime(dt/i * (l - i))))
        print('---------------------')

        df = None
