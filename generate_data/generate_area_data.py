## @package generate_parameters
# Generation of wasserhaushaltsportal parameters, e.g. yearly precipitation derived from monthly data
#
# Usage: python3 ./generate_parameters/generate_parameters.py -tgt_id_param 100 -src_id_scenario 0 -src_id_param 1 -proj_dir /var/www/daten_stb/wasserhaushaltsportal/ -stat_type yearly_sum
#
# stat_type:
# - calendrical_yearly_sum (january to december)
# - calendrical_summer_sum (may to october)
# - calendrical_winter_sum (january to april, november to december)
# - hydrolgical_year_sum (november to october)
# - hydrological_winter_sum (november to april)
# - hydrolgical_summer_sum (may to october)

import os
import sys
import h5py
import pandas as pd
import numpy as np
import time
from collections import OrderedDict

## @brief Get command line arguments.
#
# @returns args Array of arguments
def get_arguments():
    print("--> get caller arguments")
    tgt_id_param, src_id_scenario, src_id_param, proj_dir, stat_type = None, None, None, None, None
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '-tgt_id_param':
            i = i + 1
            tgt_id_param = sys.argv[i]
        elif arg == '-src_id_scenario':
            i = i + 1
            src_id_scenario = sys.argv[i]
        elif arg == '-proj_dir':
            i = i + 1
            proj_dir = sys.argv[i]
        elif arg == '-stat_type':
            i = i + 1
            stat_type = sys.argv[i]
        elif arg == '-src_id_param':
            i = i + 1
            src_id_param = sys.argv[i]
        i = i + 1

    if tgt_id_param == None or src_id_scenario == None or src_id_param == None or proj_dir == None or stat_type == None :
        print("ERROR: arguments of program call missing")
        sys.exit()

    return [tgt_id_param, src_id_scenario, src_id_param, proj_dir, stat_type]

## @brief Generate parameter
#
# @param args Array of arguments
# @returns df A pandas data frame
def generate_parameter(args):
    print('--> starting calculation')
    df_result = None
    # check folders
    tgt_id_param, src_id_scenario, src_id_param, proj_dir, stat_type = args[0], args[1], args[2], args[3], args[4]

    #src_folder = proj_dir + '/parameters/' + str(src_id_scenario) + '/' + str(src_id_param) + '/'
    src_folder = proj_dir + '/parameters/' + str(src_id_scenario) + '/' + str(src_id_param) + '/month'
    tgt_folder = proj_dir + '/parameters/' + str(src_id_scenario) + '/' + str(tgt_id_param) + '/total/'
    check_path_exists(src_folder, False)
    check_path_exists(tgt_folder, True)

    # get file container
    files = []
    for r, d, f in os.walk(src_folder):
        for file in f:
            if '.h5' in file:
                year = r.split('/')[-1]
                files.append([r, year, file])
    # sorting list by year
    files = sorted(files, key=lambda year: year[1])
    print('--> files ',files)

    if stat_type == 'calendrical_yearly_avg':
        df_result = calculate_yearly_avg(files)
    if stat_type == 'calendrical_yearly_sum':
        df_result = calculate_yearly_sums(files)
    if stat_type == 'calendrical_yearly_summer':
        df_result = calculate_yearly_sums_summer(files)
    if stat_type == 'calendrical_yearly_winter':
        df_result = calculate_yearly_sums_winter(files)

    return df_result


## @brief Calculate yearly sums of a calendrical year grouped by areas
#
# @param files folder and file structure of monthly data
# @return df_result Dictionary with pandas dataframes for each area
def calculate_yearly_avg(files):
    print('--> calculate yearly averages')
    # get areas and years
    df_areas_years = get_areas_years((files))
    z = 0
    # loop through years, get source files and summarize
    for i in files:
        fpath = i[0]
        year = i[1]
        fname = i[2]
        # open h5 files
        print(fpath, fname)
        f = h5py.File(fpath + '/'+ fname, 'r')
        ds_areas = f['areas']
        for a in ds_areas:
            # calculate averages
            ds_monthly = ds_areas[a]['table'][:]
            df_monthly = pd.DataFrame(ds_monthly)
            df_avg = df_monthly.set_index(['id_area','area']).mean(axis=1)
            
            if z == 0:
                ds = df_avg.reset_index().drop(columns=['area']).rename(columns={0:year})
                df_areas_years[a] = pd.merge(df_areas_years[a], ds, how='left', left_on='idarea_data', right_on='id_area').drop(columns=['id_area'])
            else:
                ds = df_avg.reset_index().rename(columns={0:year}).drop(columns=['area'])
                df_areas_years[a] = pd.merge(df_areas_years[a], ds, how='left', left_on='idarea_data', right_on='id_area').drop(columns=['id_area'])
                
        z = z + 1
        f.close()

    return df_areas_years

## @brief get areas and years and return a dict
def get_areas_years(files):
    """get areas and years"""
    print('--> get areas and years')
    container = {'years' : [], 'areas' : {}}
    # loop through years, get source files
    for i in files:
        fpath = i[0]
        year = i[1]
        fname = i[2]
        f = h5py.File(fpath + '/'+ fname, 'r')
        ds_areas = f['areas']
        if year not in container['years']:
            container['years'].append(year)
            container['years'].sort()
        for a in ds_areas:
            if a not in container['areas']:
                container['areas'][a] = {}
            idarea_data = pd.DataFrame(ds_areas[a]['table'][:])
            for index, row in idarea_data.iterrows():
                if row['id_area'] not in container['areas'][a]:
                    container['areas'][a][int(row['id_area'])] = float(row['area'])
    # sort values
    for a in container['areas']:
        d = container['areas'][a]
        container['areas'][a] = OrderedDict(sorted(d.items(), key=lambda t: t[0]))

    # create a dataframe
    df_container = {}
    for area in container['areas']:
        df_container[area] = pd.DataFrame(columns={'idarea_data','area'})
        data = container['areas'][area]
        for item in data:
            df_container[area] = df_container[area].append({'idarea_data' : item, 'area' : data[item]}, ignore_index=True)

    return df_container

## @brief Calculate yearly sums of a calendrical year grouped by areas
#
# @param files folder and file structure of monthly data
# @return df_result Dictionary with pandas dataframes for each area
def calculate_yearly_sums(files):
    print('--> calculate yearly sums')
    z = 0
    
    # get areas and years
    df_areas_years = get_areas_years((files))
 
    # loop through years, get source files and summarize
    for i in files:
        fpath = i[0]
        year = i[1]
        fname = i[2]
        # open h5 files
        print(fpath, fname)
        f = h5py.File(fpath + '/'+ fname, 'r')
        ds_areas = f['areas']
        print('year ',year)
        for a in ds_areas:
            # calculate sums
            ds_monthly = ds_areas[a]['table'][:]
            df_monthly = pd.DataFrame(ds_monthly)
            df_sum = df_monthly.set_index(['id_area','area']).sum(axis=1, skipna=False)

            if z == 0:
                ds = df_sum.reset_index().drop(columns=['area']).rename(columns={0:year})
                df_areas_years[a] = pd.merge(df_areas_years[a], ds, how='left', left_on='idarea_data', right_on='id_area').drop(columns=['id_area'])
            else:
                ds = df_sum.reset_index().rename(columns={0:year}).drop(columns=['area'])
                df_areas_years[a] = pd.merge(df_areas_years[a], ds, how='left', left_on='idarea_data', right_on='id_area').drop(columns=['id_area'])

        z = z + 1
        f.close()

    return df_areas_years

## @brief Calculate yearly sums of summer monthes of a calendrical year grouped by areas
#
# @param files folder and file structure of monthly data
# @return df_result Dictionary with pandas dataframes for each area
def calculate_yearly_sums_summer(files):
    print('--> calculate yearly sums for summer monthes')
    z = 0
    colList = ['id_area','area','apr','may','jun','jul','aug','sep']
    # get areas and years
    df_areas_years = get_areas_years((files))
    # loop through years, get source files and summarize
    for i in files:
        fpath = i[0]
        year = i[1]
        fname = i[2]
        # open h5 files
        f = h5py.File(fpath + '/'+ fname, 'r')
        ds_areas = f['areas']
        for a in ds_areas:
            # calculate sums
            ds_monthly = ds_areas[a]['table'][:]
            df_monthly = pd.DataFrame(ds_monthly)
            df_sum = df_monthly[colList].set_index(['id_area','area']).sum(axis=1, skipna=True)
            if z == 0:
                ds = df_sum.reset_index().drop(columns=['area']).rename(columns={0:year})
                df_areas_years[a] = pd.merge(df_areas_years[a], ds, how='left', left_on='idarea_data', right_on='id_area').drop(columns=['id_area'])
            else:
                ds = df_sum.reset_index().rename(columns={0:year}).drop(columns=['area'])
                df_areas_years[a] = pd.merge(df_areas_years[a], ds, how='left', left_on='idarea_data', right_on='id_area').drop(columns=['id_area'])

        z = z + 1
        f.close()

    return df_areas_years

## @brief Calculate yearly sums of winter monthes of a calendrical year grouped by areas
#
# @param files folder and file structure of monthly data
# @return df_result Dictionary with pandas dataframes for each area
def calculate_yearly_sums_winter(files):
    print('--> calculate yearly sums for summer monthes')
    z = 0
    colList = ['id_area','area','jan','feb','mar','oct','nov','dec']
    # get areas and years
    df_areas_years = get_areas_years((files))
    # loop through years, get source files and summarize
    for i in files:
        fpath = i[0]
        year = i[1]
        fname = i[2]
        # open h5 files
        f = h5py.File(fpath + '/'+ fname, 'r')
        ds_areas = f['areas']
        for a in ds_areas:
            # calculate sums
            ds_monthly = ds_areas[a]['table'][:]
            df_monthly = pd.DataFrame(ds_monthly)
            df_sum = df_monthly[colList].set_index(['id_area','area']).sum(axis=1, skipna=False)
            if z == 0:
                ds = df_sum.reset_index().drop(columns=['area']).rename(columns={0:year})
                df_areas_years[a] = pd.merge(df_areas_years[a], ds, how='left', left_on='idarea_data', right_on='id_area').drop(columns=['id_area'])
            else:
                ds = df_sum.reset_index().rename(columns={0:year}).drop(columns=['area'])
                df_areas_years[a] = pd.merge(df_areas_years[a], ds, how='left', left_on='idarea_data', right_on='id_area').drop(columns=['id_area'])
        z = z + 1
        f.close()

    return df_areas_years

# @brief Save datasets to h5 file
#
# @param args Command line arguments
# @param dict dictionary object that contains pandas dataframes
def save_data(args, dict):
    print('--> save data')
    tgt_id_param, src_id_scenario, src_id_param, proj_dir, stat_type = args[0], args[1], args[2], args[3], args[4]
    src_folder = proj_dir + '/parameters/' + str(src_id_scenario) + '/' + str(src_id_param) + '/'
    tgt_folder = proj_dir + '/parameters/' + str(src_id_scenario) + '/' + str(tgt_id_param) + '/total/'

    # create h5 stat file
    print('--> target folder is : ', tgt_folder)
    # create hdf file
    fn = str(tgt_id_param) + '_' + str(src_id_scenario) + '.stats.h5'
    if os.path.isfile(tgt_folder + fn) == False:
        f = h5py.File(tgt_folder + fn, 'w')
    else:
        f = h5py.File(tgt_folder + fn, 'r+')
    print('--> HDF5 file is : ', fn)

    # write dataset to file
    print('--> writing data ...')
    if not 'areas' in f:
        f.create_group('areas')

    for id_area in dict:
        # pandas dataset
        ds = dict[str(id_area)]
        if 'index' in ds.columns:
            ds = ds.drop(columns=['index'])
        # rename
        #if 'idarea_data' in ds.columns:
        ds = ds.rename(columns={'idarea_data' : 'id_area'})
        #print(ds)
        #sys.exit()
        #ds[ds==0]=np.nan
        if not str(id_area) in f['areas']:
            f['areas'].create_group(str(id_area))
        if 'table' in f['areas'][str(id_area)]:
            del f['areas'][str(id_area)]['table'] # delete table if exists
        # set dtype for hdf5 table
        namesList = ds.columns
        formatList = [(np.float)] * len(namesList)
        # print(formatList)
        ds_dt = np.dtype({'names':namesList,'formats':formatList})
        d = f['areas'][str(id_area)].create_dataset('table', ((ds.shape)[0],), dtype = ds_dt)
        for col in namesList:
            d[col] = ds[col]

    # close file
    f.close()

## @brief Check if a folder exists, if not then create
#
# @param folder The name of folder
# @param create Boolean True/False if folder has to be created if not existing
def check_path_exists(folder, create):
    print('--> check folders')
    if os.path.isdir(folder):
        print(folder, '--> ok, folder exists')
    elif os.path.isdir(folder) == False and create == False:
        print("ERROR: folder not exists --> ", folder)
        sys.exit()
    elif os.path.isdir(folder) == False and create == True:
        os.makedirs(folder)
        print(folder, '--> folder created')

if __name__ ==  '__main__':
    # start program
    t0 = time.time()
    print ("--> starting parameter generation...")

    # get command line arguments
    args = get_arguments()

    # do recalculation
    df = generate_parameter(args)
    #sys.exit()
    # save data
    if df != None:
        save_data(args, df)
    else:
        print('ERROR : No result')

    ctime = time.time()
    dt = ctime - t0
    print('elapsed time: ', time.strftime("%H:%M:%S", time.gmtime(dt)))
