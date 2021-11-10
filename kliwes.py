## @package Generate kliwes datasets
#
# Create 3-dimensional arrays (data cubes), where dim0 is x (columns), dim1 is y (rows) and dim2 is t (time)

import sys
import os
import h5py
import pandas as pd
import numpy as np
sys.path.append(os.getcwd() + '/..')
from pg.pg import db_connector as pg

class kliwes2_generator:
    def __init__(self, args):
        """ configure output """
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.conf = {
            'parameters' : {
                'sn' : {'id':1, 'decimals':2, 'dtype':'int32', 'nodata':-99999, 'to_hdf' : True, 'aggregation' : 'yearly_sum'}
            },
            'dbconfig' : {
                "db_host" : "192.168.0.194",
                "db_name" : "whp_sachsen",
                "db_user" : "visdat",
                "db_password" : "9Leravu6",
                "db_port" : "9991"
            },
            'scenarios': {
                'ist_zustand' : {'id' : 30}
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

        return ds, x, y

## @brief Create a dataset for each kliwes parameter
    #
    # @param area_ds, pointer to area dataset read by get_area
    def create_parameter(self, area_ds):
        print('--> Create parameter datasets')
        scenario_id = self.args['scenario_id']
        path = self.args['src_folder'] + '/' + str(scenario_id) + '/'
        shape_area = area_ds.shape
        df_area = pd.DataFrame(area_ds[:].flatten(), columns=['areaid'])
        print(shape_area, df_area.keys())
        # get id_areadata from database
        db = pg()
        dbconf = db.dbConfig(self.conf['dbconfig'])
        dbcon = db.dbConnect()
        sql = 'SELECT idarea_data, description_int FROM spatial.area_data where idarea = ' + str(self.args['area_id'])
        res = db.tblSelect(sql)
        db.dbClose()
        new_area = pd.DataFrame(np.array(res[0]), columns={0,1}).rename(columns={0:'idarea_data',1:'orgid'})
        # join  df_area (with idarea_data) and new_area(with idarea_data and original id's)
        df_area = pd.merge(df_area, new_area,  left_on='areaid', right_on='idarea_data' ,how='left', sort=False)

        # loop through parameter
        for p in self.conf['parameters']:
            param_conf = self.conf['parameters'][p]
            fpath = path + str(param_conf['id']) + '/'
            start_year = None
            if param_conf['to_hdf'] == True:
                print(p)
                # read files
                if os.path.exists(fpath):
                    for r, d, f in os.walk(fpath):
                        # create a ndim array with shape (x,y,t)
                        n_years = len(f)
                        current_year_idx = 0
                        print('number of years', n_years)
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
                            df_param = pd.read_hdf(fpath + file, key='table').reset_index()
                            df_param['index'] = df_param['index'].astype(int)
                            # get a column for each month
                            df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param.values_block_0.str.split(expand=True,)
                            # remove column values_block_0
                            df_param = df_param.drop(columns=['values_block_0'])
                            # set nan values
                            df_param = df_param.set_index(['index']).astype('float').replace(-99999, np.nan)
                            # calculate yearly sum
                            if param_conf['aggregation'] == 'yearly_sum':
                                df = df_param.sum(axis=1).reset_index().rename(columns={0:year})
                            if param_conf['aggregation'] == 'yearly_mean':
                                df = df_param.mean(axis=1).reset_index().rename(columns={0:year})
                            # join area and parameter
                            df_join = pd.merge(df_area, df, left_on='orgid', right_on='index' ,how='left', sort=False)
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
                            #break
                        #sys.exit()
                        # save data to hd5
                        # create path if not exists
                        dst_fpath = self.args['dst_folder'] + '/parameters/' + str(self.args['scenario_id']) + '/' + str(param_conf['id']) + '/'
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
                        self.save_as_h5(save_config, True, None)
                        #break
            #break

    ## @brief Create a dataset for each kliwes parameter and each scenario
    #
    # @param area_ds, pointer to area dataset read by get_area
    def create_parameter_by_scenarios(self, area_ds, x, y):
        print('--> Create yearly parameter datasets by scenarios')
        print('-->area_ds :',area_ds)
        # get area properties
        shape_area = area_ds.shape
        df_area = pd.DataFrame(area_ds[:].flatten(), columns=['areaid'])
        print(shape_area, df_area.keys())
        # get id_areadata from database
        db = pg()
        dbconf = db.dbConfig(self.conf['dbconfig'])
        dbconf = db.dbConnect()
        
        sql = 'SELECT id_org AS idarea_data, id_org AS description_int FROM spatial.kliwes2_geom'
        res = db.tblSelect(sql)
        #print(sql)
        db.dbClose()
        new_area = pd.DataFrame(np.array(res[0]), columns={0,1}).rename(columns={0:'idarea_data',1:'orgid'})
        # join  df_area (with idarea_data) and new_area(with idarea_data and original id's)
        df_area = pd.merge(df_area, new_area,  left_on='areaid', right_on='idarea_data' ,how='left', sort=False)

        print('-->df_area :',df_area[1500000:])
        #sys.exit()
        print('--> self.conf : ',self.conf)
        print('--> self.args : ',self.args)

        # loop over scenarios
        for scenario in self.conf['scenarios']:
            print('--> current scenario', scenario, self.conf['scenarios'][scenario]['id'])
            scenario_id = self.conf['scenarios'][scenario]['id']
            path = self.args['src_folder'] + '/' + str(scenario_id) + '/'
            print('--> path : ', path)

            # loop through parameter
            for p in self.conf['parameters']:
                param_conf = self.conf['parameters'][p]
                fpath = path + str(param_conf['id']) + '/'
                print(p, '--> fpath : ', fpath)

                start_year = None

                if param_conf['to_hdf'] == True:
                    print(p)
                    # read files
                    if os.path.exists(fpath):
                        for r, d, f in os.walk(fpath):
                            # create a ndim array with shape (x,y,t)
                            n_years = len(f)
                            current_year_idx = 0
                            print('number of years', n_years)
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
                                df_param = pd.read_hdf(fpath + file, key='data').reset_index()
                                df_param['index'] = df_param['index'].astype(int)
                                # get a column for each month
                                df_param[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_param.values_block_0.str.split(expand=True,)
                                # remove column values_block_0
                                df_param = df_param.drop(columns=['values_block_0'])
                                # set nan values
                                df_param = df_param.set_index(['index']).astype('float').replace(-99999, np.nan)
                                
                                print('-->df_param :',df_param)
                                #sys.exit()
                                # calculate yearly sum
                                if param_conf['aggregation'] == 'yearly_sum':
                                    df = df_param.sum(axis=1).reset_index().rename(columns={0:year})
                                    
                                    
                                    
                                # join area and parameter
                                df_join = pd.merge(df_area, df, left_on='orgid', right_on='index' ,how='left', sort=False)
                                
                                #print(df_join.loc[df_join['index']==2025122])
                                #print(df_join.keys(), df_join.shape)
                                
                                print('--> df_area : ',df_area[1500000:])
                                print('--> df : ',df)
                                print('--> df_join : ',df_join[1500000:])
                                
                                print('--> df_join[year] : ',df_join[year][1500000:])
                                
                                #sys.exit()
                                #print(df_join.loc[df_join['index']==2025122])
                                #print(df_join.loc[df_join['index']==2025122])
                                
                                data = np.array(df_join[year]).reshape((shape_area))
                                
                                print('--> data : ',data)
                                
                                # format dtype and decimals
                                data = data * (10**param_conf['decimals'])
                                # assign an specified value as nan
                                data[np.isnan(data)] = param_conf['nodata']
                                # set dtype
                                data = data.astype(dtype)
                                
                                print('--> data : ',data)
                                sys.exit()
                                # append parameter matrix of current year  to data cube / ndarray
                                data_cube[:,:, current_year_idx] = data
                                # increase current year index
                                current_year_idx = current_year_idx + 1
                                #break
                                #if current_year_idx == 2:
                                #    break
                            #print(data_cube[1000,1000,:])
                            #sys.exit()

                            # save data to hd5

                            # create path if not exists
                            dst_fpath = self.args['dst_folder'] + '/parameters/' + str(scenario_id) + '/' + str(param_conf['id']) + '/'
                            print('--> dst_fpath : ', dst_fpath)

                            if not os.path.exists(dst_fpath):
                                os.makedirs(dst_fpath)
                            # set file name
                            fname = str(param_conf['id']) + '_' + str(scenario_id) + '_' + str(self.args['level_id']) + '.' + str(dtype) + '.' + str(param_conf['decimals']) + '.nd.h5'
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
                            save_config['x'] = x
                            save_config['y'] = y
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
