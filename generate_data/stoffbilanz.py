## @package Generate stoffbilanz datasets
#
# Create 3-dimensional arrays (data cubes), where dim0 is x (columns), dim1 is y (rows) and dim2 is t (time)

import sys
import os
import h5py
import pandas as pd
import numpy as np
sys.path.append(os.getcwd() + '/..')
from pg.pg import db_connector as pg

class stoffbilanz_generator:
    def __init__(self, args):
        """ configure output """
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.conf = {
            'parameters' : {
                'nssommer' : {'id':6, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5, 'aggregation' : 'yearly_mean', 'nodata':-99999},
                'nswinter' : {'id':7, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5, 'aggregation' : 'yearly_mean', 'nodata':-99999},
                'gesamtabfluss' : {'id':33, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5, 'aggregation' : 'yearly_mean', 'nodata':-99999},
                'abw_gwn' : {'id':19, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5, 'aggregation' : 'yearly_mean', 'nodata':-99999},
                'abw_r' : {'id':20, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5, 'aggregation' : 'yearly_mean', 'nodata':-99999},
                'gwn' : {'id':32, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5, 'aggregation' : 'yearly_mean', 'nodata':-99999},
                'etp' :{'id':31, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5, 'aggregation' : 'yearly_mean', 'nodata':-99999},
                'ns' : {'id':10, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5, 'aggregation' : 'yearly_mean', 'nodata':-99999}
            },
            'dbconfig' : {
                "db_host" : "192.168.0.194",
                "db_name" : "gwn_sachsen",
                "db_user" : "visdat",
                "db_password" : "9Leravu6",
                "db_port" : "9991"
            }
        }

    ## @brief Get area nc raster of stoffbilanz dataset
    #
    # @return area, numpy array
    def get_area(self):
        print('--> get area of stoffbilanz dataset')
        # variables
        fpath = None
        ds = None
        # fname and path
        fpath = '/mnt/visdat/Projekte/2020/GWN viewer/daten/stoffbilanz/stoffbilanz_gis/grid_100m_25833.nc'

        # check if file exists
        if os.path.exists(fpath):
            print('OK, file found ', fpath)
        else:
            sys.exit('ERROR: path not found '+ fpath)

        if fpath == None:
            sys.exit('ERROR: file not found '+ fpath)

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

        return ds, x, y

    ## @brief Create a dataset for each stoffbilanz parameter
    #
    # @param area_ds, pointer to area dataset read by get_area
    def create_parameter(self, area_ds, x, y):
        print('--> Create parameter datasets')
        scenario_id = self.args['scenario_id']
        path = self.args['src_folder'] + '/' + str(scenario_id) + '/'
        shape_area = area_ds.shape
        df_area = pd.DataFrame(area_ds[:].flatten(), columns=['areaid'])
        print(shape_area, df_area.keys())
        # loop through parameter
        for p in self.conf['parameters']:
            param_conf = self.conf['parameters'][p]
            fpath = path + str(param_conf['id']) + '/'
            start_year = None
            if param_conf['to_hdf'] == True:# and p == 'etp':
                print('#################')
                print(p)
                if os.path.exists(fpath):
                    for r, d, f in os.walk(fpath):
                        # create a ndim array with shape (x,y,t)
                        n_years = len(f)
                        current_year_idx = 0
                        print('number of years', n_years)
                        print(r)
                        shape = (shape_area[0], shape_area[1], n_years)
                        dtype = param_conf['dtype']
                        data_cube = np.zeros((shape), dtype)
                        print('shape of ndim array', data_cube.shape, data_cube.dtype)
                        # loop through files
                        for file in f:
                            print ('-->',file)
                            # get year from filename
                            year = file.split('.')[0].split('_')[2]
                            # get first year
                            if start_year == None:
                                start_year = year
                            # read the h5 file
                            #print(fpath, file)
                            df_param = pd.read_hdf(fpath + file, key='table').reset_index()
                            df_param['index'] = df_param['index'].astype(int)
                            #print(df_param['values_block_0'][0])
                            # get a column for each month
                            df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param.values_block_0.str.split(expand=True,)
                            # remove column values_block_0
                            df_param = df_param.drop(columns=['values_block_0'])
                            # set nan values
                            df_param = df_param.set_index(['index']).astype('float').replace(-99999, np.nan)
                            # calculate yearly mean
                            if param_conf['aggregation'] == 'yearly_mean':
                                df = df_param.mean(axis=1).reset_index().rename(columns={0:year})
                            # join area and parameter
                            df_join = pd.merge(df_area, df, left_on='areaid', right_on='index' ,how='left', sort=False)
                            #print(df_join.keys(), df_join.shape)
                            data = np.array(df_join[year]).reshape((shape_area))
                            # format dtype and decimals
                            data = data * (10**param_conf['decimals'])
                            # assign an specified value as nan
                            data[np.isnan(data)] = param_conf['nodata']
                            # set dtype
                            data = data.astype(dtype)
                            # append parameter matrix of current year  to data cube / ndarray
                            data_cube[:,:, current_year_idx] = data
                            # increase current year index
                            current_year_idx = current_year_idx + 1
                        #print(data_cube[1000,1000,:])
                        #sys.exit()
                        # save data to hd5
                        # create path if not exists
                        dst_fpath = self.args['dst_folder'] + '/parameters/' + self.args['scenario_id'] + '/' + str(param_conf['id']) + '/'
                        if not os.path.exists(dst_fpath):
                            os.makedirs(dst_fpath)
                        # set file name
                        fname = str(param_conf['id']) + '_' + str(self.args['scenario_id']) + '_' + str(self.args['level_id']) + '.' + str(dtype) + '.' + str(param_conf['decimals']) + '.nd.h5'
                        dst_fpath = dst_fpath + fname
                        # container for some configurations to save as hdf5
                        save_config = {}
                        save_config['dataset'] = data_cube
                        save_config['dst_path'] = dst_fpath
                        save_config['ds_name'] = 'table'
                        save_config['create_year_scale'] = True
                        save_config['start_year'] = start_year
                        save_config['n_years'] = n_years
                        save_config['x'] = x
                        save_config['y'] = y
                        self.save_as_h5(save_config, True, None)
                        #current_year_idx = current_year_idx + 1

                        #sys.exit()

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

        print('--> path:', path)
        print('--> ds_name:', ds_name)

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
        except:
            sys.exit("ERROR: " + str(sys.exc_info()))
