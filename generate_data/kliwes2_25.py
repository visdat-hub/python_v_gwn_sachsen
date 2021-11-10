## @package Generate kliwes datasets
#
# Create 3-dimensional arrays (data cubes), where dim0 is x (columns), dim1 is y (rows) and dim2 is t (time)

import sys
import os
import datetime
import time
from pathlib import Path
import gdal
import h5py
import pandas as pd
import numpy as np
import subprocess
sys.path.append(os.getcwd() + '/..')
from pg.pg import db_connector as pg

class kliwes2_25_generator:
    def __init__(self, args):
        """ configure output """
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.conf = {
            'parameters' : {
                'n' : {'src_id':1, 'tgt_id':10, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'nn' : {'src_id':2, 'tgt_id':35, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                't' : {'src_id':3, 'tgt_id':9, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_avg'},
                'rf' : {'src_id':4, 'tgt_id':40, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_avg'},
                'etp' : {'src_id':5, 'tgt_id':8, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'kwb' : {'src_id':6, 'tgt_id':36, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'e' : {'src_id':7, 'tgt_id':31, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'ao' : {'src_id':9, 'tgt_id':37, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'hy' : {'src_id':10, 'tgt_id':25, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'r' : {'src_id':11, 'tgt_id':33, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'sw' : {'src_id':12, 'tgt_id':34, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'rg2' : {'src_id':13, 'tgt_id':12, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'rg1' : {'src_id':14, 'tgt_id':13, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'k' : {'src_id':16, 'tgt_id':38, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                'gwn' : {'src_id':103, 'tgt_id':32, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : False, 'aggregation' : 'yearly_sum'},
                
                'sn' : {'src_id':6, 'tgt_id':6, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'},
                'wn' : {'src_id':7, 'tgt_id':7, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'}
            },
            'dbconfig' : {
                "db_host" : "192.168.0.194",
                "db_name" : "gwn_sachsen",
                "db_user" : "visdat",
                "db_password" : "9Leravu6",
                "db_port" : "9991"
            },
            'scenarios': {
                'ist_zustand' : {'id' : 30, 'to_hdf' : True}
            }
        }
        

        

    ## @brief Get area raster of kliwes dataset
    #
    # @return area, numpy array
    def get_area(self):
        print('--> get area of kliwes dataset')
        # variables
        fname = None
        fpath = None
        ds = None
        # fname and path
        path = self.args['dst_folder'] + '/areas/' + str(self.args['area_id']) + '/'
        fn = str(self.args['area_id']) + '_' + str(self.args['level_id'])

        # check if file exists
        if os.path.exists(path):
            for r, d, f in os.walk(path):
                for file in f:
                    if fn + '.' in file:
                        print('OK, file found ', file, 'in', path)
                        fname = file
        else:
            sys.exit('ERROR: path not found '+ path)

        if fname != None:
            fpath = path + fname
        else:
            sys.exit('ERROR: file not found '+ fn)

        # read area file
        try:
            print('--> read area dataset')
            f = h5py.File(fpath, 'r')
            ds = f['Band1']
            x = f['x']
            y = f['y']
            print('OK, shape is', ds.shape)
        except:
            sys.exit("ERROR: " + str(sys.exc_info()))

        #print(ds)
        #sys.exit()
        return ds, x, y

    ## @brief Create a dataset for each kliwes parameter and each scenario
    #
    # @param area_ds, pointer to area dataset read by get_area
    def create_parameter_by_scenarios(self, area_ds, x, y):
        print('--> Create yearly parameter datasets by scenarios')
        print('-->self.conf :',self.conf)
        #print('-->area_ds :',area_ds)
        #print('-->x :',x)
        #print('-->y :',y)
        #sys.exit()
        
        # get area properties
        shape_area = area_ds.shape
        df_area = pd.DataFrame(area_ds[:].flatten(), columns=['areaid'])
        print('-->shape_area :',shape_area)
        #sys.exit()
        print(shape_area, df_area.keys())
        #print('-->df_area :',df_area)
        #data = np.array(df_area).reshape((shape_area))
        #print('-->data :',data)
        #sys.exit()
        # get id_areadata from database
        db = pg()
        dbconf = db.dbConfig(self.conf['dbconfig'])
        dbconf = db.dbConnect()
        #sql = 'SELECT idarea_data, description_int FROM spatial.area_data where idarea = ' + str(self.args['area_id'])
        sql = 'SELECT id AS idarea_data, id_org AS orgid FROM spatial.kliwes2_clean_geom ORDER BY id'
        res = db.tblSelect(sql)
        #print(sql)
        db.dbClose()
        new_area = pd.DataFrame(np.array(res[0]), columns={0,1}).rename(columns={0:'idarea_data',1:'orgid'})
        # join  df_area (with idarea_data) and new_area(with idarea_data and original id's)
        df_area = pd.merge(df_area, new_area,  left_on='areaid', right_on='idarea_data' ,how='left', sort=False)

        #print('-->new_area :',new_area)
        #print('-->df_area :',df_area)
        print('--> self.conf : ',self.conf)
        print('--> self.args : ',self.args)
        #sys.exit()

        # loop over scenarios
        for scenario in self.conf['scenarios']:
            
            if self.conf['scenarios'][scenario]['to_hdf'] == True:
                
                print('--> current scenario', scenario, self.conf['scenarios'][scenario]['id'])
                scenario_id = self.conf['scenarios'][scenario]['id']
                path = self.args['src_folder'] + '/' + str(scenario_id) + '/'
                #print('--> path : ', path)
                
                # loop through parameter
                for p in self.conf['parameters']:
                    
                    param_conf = self.conf['parameters'][p]
                    
                    """
                    gdal_dst_path = self.args['dst_folder'] + '/parameters/' + str(scenario_id) + '/' + str(param_conf['tgt_id']) + '/'
                    gdal_dst_filename = '2.nc'
                    
                    gdal_dst_file = Path(gdal_dst_path + gdal_dst_filename)
                    if gdal_dst_file.is_file():
                        print("file exist!")
                    else:
                        print("no such file!")
                        file = open(gdal_dst_path + gdal_dst_filename,'a+')
                    """
                    
                    
                    fpath = path + str(param_conf['src_id']) + '/'
                    print(p, '--> fpath : ', fpath)
                    
                    start_year = None
                    
                    if param_conf['to_hdf'] == True:
                        #print(p)
                        #sys.exit()
                        # read files
                        if os.path.exists(fpath):
                            for r, d, f in os.walk(fpath):
                                
                                print('-->r ',r)
                                print('-->d ',d)
                                print('-->f ',f)
                                print('-->len(f) ',len(f))
                                print('-->sorted(f) ',sorted(f))
                                #sys.exit()
                                
                                # create a ndim array with shape (x,y,t)
                                n_years = len(f)
                                
                                current_year_idx = 0
                                print('number of years', n_years)
                                
                                #shape = (shape_area[0], shape_area[1], n_years)
                                shape = (1680, 2250, n_years)
                                #shape = (1680, 2250, 2)
                                dtype = param_conf['dtype']
                                data_cube = np.zeros((shape), dtype)
                                print('shape of ndim array', data_cube.shape, data_cube.dtype)
                                #sys.exit()
                                
                                # loop through files
                                #for file in f:
                                for file in sorted(f):
                                    
                                    print ('-->',file)
                                    # get year from filename
                                    year = file.split('.')[0].split('_')[2]
                                    
                                    # get first year
                                    if start_year == None:
                                        start_year = year
                                    
                                    # read the h5 file
                                    df_param = pd.read_hdf(fpath + file, key='data').reset_index()
                                    df_param['index'] = df_param['index'].astype(int)
                                    # get a column for each month
                                    df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param.values_block_0.str.split(expand=True,)
                                    # remove column values_block_0
                                    df_param = df_param.drop(columns=['values_block_0'])
                                    # set nan values
                                    df_param = df_param.set_index(['index']).astype('float').replace(-99999, np.nan)
                                    #print('-->df_param :',df_param)
                                    #sys.exit()
                                    
                                    # calculate yearly sum
                                    if param_conf['aggregation'] == 'yearly_sum':
                                        df = df_param.sum(axis=1).reset_index().rename(columns={0:year})
                                    # calculate yearly sum
                                    if param_conf['aggregation'] == 'yearly_avg':
                                        df = df_param.mean(axis=1).reset_index().rename(columns={0:year})
                                    
                                    # join area and parameter
                                    df_join = pd.merge(df_area, df, left_on='orgid', right_on='index' ,how='left', sort=False)
                                    
                                    data = np.array(df_join[year]).reshape((shape_area))
                                    #print('--> data : ',data)
                                    
                                    # assign an specified value as nan
                                    data[np.isnan(data)] = param_conf['nodata']
                                    # set dtype
                                    data = data.astype(dtype)
                                    
                                    dst_fpath = self.args['dst_folder'] + '/parameters/' + str(scenario_id) + '/' + str(param_conf['tgt_id']) + '/1.nc'
                                    print('--> dst_fpath : ', dst_fpath)
                                    #print('--> data : ', data.shape)
                                    
                                    ####################################################################################################################
                                    #
                                    ####################################################################################################################
                                    
                                    if os.path.exists(dst_fpath):
                                        os.remove(dst_fpath) 
                                    
                                    f = h5py.File(dst_fpath, 'w')
                                    ds = f.create_dataset('Band1', (data.shape) , dtype=data.dtype)
                                    ds[:] = data
                                    
                                    ds.attrs['grid_mapping'] = 'transverse_mercator'
                                    #v.attrs['grid_mapping'] = 'transverse_mercator'
                                    ds.attrs['DIMENSION_LIST'] = '[(1-3345), (1-1361)]'
                                    ds.attrs['_FillValue'] = -99999
                                    
                                    dt = h5py.special_dtype(vlen=str)
                                    vm = f.create_dataset('transverse_mercator', (), dtype=dt)
                                    
                                    vm.attrs['GeoTransform'] = '278000 25 0 5561000.0 0 25'
                                    vm.attrs['false_easting'] = 500000.0
                                    vm.attrs['false_northing'] = 0.0
                                    vm.attrs['grid_mapping_name'] = 'transverse_mercator'
                                    vm.attrs['inverse_flattening'] = 298.257222101
                                    vm.attrs['latitude_of_projection_origin'] = 0.0
                                    vm.attrs['long_name'] = 'CRS definition'
                                    vm.attrs['longitude_of_central_meridian'] = 15.0
                                    vm.attrs['longitude_of_prime_meridian'] = 0.0
                                    vm.attrs['scale_factor_at_central_meridian'] = 0.9996
                                    vm.attrs['semi_major_axis'] = 6378137.0
                                    vm.attrs['spatial_ref'] = 'PROJCS["ETRS89 / UTM zone 33N",GEOGCS["ETRS89",DATUM["European_Terrestrial_Reference_System_1989",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6258"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4258"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",15],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","25833"]]'
                                    
                                    vx = f.create_dataset('x', data = x)
                                    vx.attrs['units'] = 'm'
                                    
                                    vy = f.create_dataset('y', data = y)
                                    vy.attrs['units'] = 'm'
                                    
                                    f.close()
                                    
                                    del f
                                    
                                    ####################################################################################################################
                                    #
                                    ####################################################################################################################
                                    
                                    gdal_of = '-of netCDF'
                                    gdal_co = '-co "COMPRESS=DEFLATE" -co "ZLEVEL=9" -co "FORMAT=NC4"'
                                    gdal_r =  '-r average'
                                    gdal_tr = '-tr 100 100'
                                    gdal_te = '-te 278000.0 5561000.0 503000.0 5729000.0'
                                    gdal_srcnodata = '-srcnodata -99999'
                                    gdal_dstnodata = '-dstnodata -99999'
                                    #gdal_ot = '-ot ' + 'int32'
                                    
                                    gdal_src_path =  self.args['dst_folder'] + '/parameters/' + str(scenario_id) + '/' + str(param_conf['tgt_id']) + '/'
                                    gdal_src_filename = '1.nc'
                                    #gdal_src_format = '3'
                                    gdal_dst_path = self.args['dst_folder'] + '/parameters/' + str(scenario_id) + '/' + str(param_conf['tgt_id']) + '/'
                                    gdal_dst_filename = '2.nc'
                                    
                                    gdal_string = 'gdalwarp ' + gdal_of + ' ' + gdal_co + ' ' + gdal_r + ' ' + gdal_tr + ' ' + gdal_te + ' ' +\
                                        gdal_srcnodata + ' ' + gdal_dstnodata + ' ' + gdal_src_path + gdal_src_filename + ' ' + gdal_dst_path + gdal_dst_filename
                                        
                                    
                                    print('--> gdal_string',gdal_string) 
                                    
                                    
                                    if os.path.exists(gdal_dst_path + gdal_dst_filename):
                                        os.remove(gdal_dst_path + gdal_dst_filename) 
                                    
                                    """if os.path.exists(gdal_dst_path + gdal_dst_filename):
                                        last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(gdal_dst_path + gdal_dst_filename))
                                    else:
                                        last_modified = datetime.time()"""
                                    
                                    #time.sleep(5)
                                    
                                    while not os.path.exists(gdal_dst_path + gdal_dst_filename):
                                        #time.sleep(1)
                                        proc = subprocess.Popen(gdal_string.encode('utf8'), shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                                        stdout,stderr=proc.communicate()
                                        print('--> stdout ', stdout)
                                        print('--> stderr ', stderr)
                                        
                                        proc.kill()
                                    
                                    ####################################################################################################################
                                    #
                                    ####################################################################################################################
                                    
                                    print('--> path', gdal_dst_path+gdal_dst_filename)
                                    f = h5py.File(gdal_dst_path+gdal_dst_filename, 'r')
                                    data = f['Band1']
                                    
                                    x2 = f['x']
                                    y2 = f['y']
                                    
                                    
                                    print('OK2, shape is', data.shape)
                                    #print('OK2, shape is', data)
                                    #print('-->x :',x2)
                                    #print('-->y :',y2)
                                    
                                    #print('OK2, shape is', data)
                                    #sys.exit()
                                    
                                    # append parameter matrix of current year  to data cube / ndarray
                                    data_cube[:,:, current_year_idx] = data[:,:]
                                    # increase current year index
                                    current_year_idx = current_year_idx + 1
                                    #break
                                
                                if os.path.exists(gdal_dst_path + gdal_dst_filename):
                                    os.remove(gdal_dst_path + gdal_dst_filename) 
                                    
                                if os.path.exists(dst_fpath):
                                    os.remove(dst_fpath) 
                                
                                # save data to hd5
                                print('--> param_conf ', param_conf)
    
                                # create path if not exists
                                dst_fpath = self.args['dst_folder'] + '/parameters/' + str(scenario_id) + '/' + str(param_conf['tgt_id']) + '/'
                                print('--> dst_fpath : ', dst_fpath)
    
                                if not os.path.exists(dst_fpath):
                                    os.makedirs(dst_fpath)
                                # set file name
                                fname = str(param_conf['tgt_id']) + '_' + str(scenario_id) + '_1.' + str(dtype) + '.' + str(param_conf['decimals']) + '.nd.h5'
                                print('--> fname : ', fname)
    
    
                                dst_fpath = dst_fpath + fname
                                # container for some configurations to save as hdf5
                                save_config = {}
                                save_config['dataset'] = data_cube
                                save_config['dst_path'] = dst_fpath
                                save_config['ds_name'] = 'table'
                                save_config['create_year_scale'] = True
                                save_config['start_year'] = start_year
                                save_config['n_years'] = n_years
                                save_config['x'] = x2
                                save_config['y'] = y2
                                self.save_as_h5(save_config, True, None)


    ## @brief Save an n-dimensional array as hdf5 file
    #
    # @param data dict, dictionary wich contains the relevant data: fpath, dataset, name of dataset, dimensions, scale etc.
    # @param overwrite boolean, overwrite / replace file
    # @param groupName string, name of a group object (optional)
    def save_as_h5(self, data, overwrite, groupName):
        print('--> save as h5')
        path = data['dst_path']
        dataset = data['dataset']
        ds_name = data['ds_name']
        x = data['x']
        y = data['y']

        try:
            if overwrite == True:
                f = h5py.File(path, 'w')
            if overwrite == False:
                f = h5py.File(path, 'a')

            # create a group object
            if groupName != None:
                grp = f.create_group(groupName)
                ds = grp.create_dataset(ds_name, (dataset.shape) , dtype=dataset.dtype, compression="gzip", compression_opts=9)
                #ds = grp.create_dataset(ds_name, (dataset.shape) , dtype=dataset.dtype)
                ds[:] = dataset
            else:
                #ds = f.create_dataset(ds_name, (dataset.shape) , dtype=dataset.dtype, compression="gzip", compression_opts=9)
                ds = f.create_dataset(ds_name, (dataset.shape) , dtype=dataset.dtype)
                ds[:] = dataset

            if data['create_year_scale'] == True:
                years_dim = np.arange(int(data['start_year']),int( data['start_year'])+int(data['n_years']))
                f.create_dataset('year', data = years_dim)

            f.create_dataset('x', data = x)
            f.create_dataset('y', data = y)

            f.close()
            print('--> done')
        except:
            sys.exit("ERROR: " + str(sys.exc_info()))
