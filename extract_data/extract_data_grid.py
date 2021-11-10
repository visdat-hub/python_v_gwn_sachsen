## @package Extract data from ndimensional data cubes (ndarray)
#
# Dimensions are x (col), y (row) and t (time)
# Usage
# ----------
# cd /mnt/visdat/Projekte/2020/GWN Viewer/dev/python/extract_data/
#
# python3 extract_data.py -src_folder /var/rosi_data/daten_stb/gwn_sachsen/ -data_type kliwes -scenario_id 0 -level_id 1 -area_id 12 -area_data [1,2,3] -parameter_id 10 -years [1960,1990] -output map_data
#
# Command line arguments
# ----------
# - src_folder : string, folder of h5 data generated by create_import_structure
# - data_type : string, kliwes, difga, raklida_messungen, raklida_referenz, raklida_wettreg66
# - scneario_id : integer, scenario identifier
# - parameter_id : integer, identifier of parameter to extract
# - level_id : integer, level identifier to set spatial resolution
#   - level 1 : 100m
#   - level 2 : 50m
#   - level 3 : 25m
# - area_id : integers, area identifier
# - area_data : int array, identifiers of area (polygon) data, or, if none/all is selected []
# - years : int array, [start_year, last_year]
# - output: string, type of output data --> map_data, base_statistic, histogram, timeline

import sys
import os
import h5py
import pandas as pd
import numpy as np
sys.path.append('/mnt/visdat/Projekte/2020/GWN viewer/dev/python/extract_data/pg')
from pg import db_connector as pg

class extract_data_grid:
    ## @brief Define database configurationma
    def __init__(self):
        """ """
        # define database access
        self.conf = {'dbconfig' :
            {
                "db_host" : "192.168.0.194",
                "db_name" : "gwn_sachsen",
                "db_user" : "visdat",
                "db_password" : "9Leravu6",
                "db_port" : "9991"
            }
        }
        print(os.getcwd())
        # placeholder, set final decimal in function check_parameter_file()
        self.decimals = 2

    ## @brief Get command line arguments.
    #
    # @returns args Array of arguments
    def get_arguments(self, defs, modul):

        print("--> get caller arguments")
        args = {
            'id_level' : 1,
            'id_param' : defs['param']['id_param'],
            'id_scenario' : defs['param']['id_scenario'],
            'id_area' : defs['param']['id_area'],
            'id_areadata' : defs['param']['id_areadata'],
            'start_year' : defs['param']['time']['start_year'],
            'end_year' : defs['param']['time']['end_year'],
            'proj_dir' : defs['project']['proj_dir'],
            'error' : 0
        }
        # diff params
        if 'param_diff' in defs:
            args['id_param_diff'] = defs['param_diff']['id_param']
            args['id_scenario_diff'] = defs['param_diff']['id_scenario']
            args['start_year_diff'] = defs['param_diff']['time']['start_year']
            args['end_year_diff'] = defs['param_diff']['time']['end_year']
            
        try:
            args['diff'] = defs['project']['diff']
        except:
            args['diff'] = 0

        try:
            args['id_tab'] = defs['project']['idtab']
        except:
            args['id_tab'] = 0
            
        return args

    ## @brief Check if folder exists
    #
    # @param folder_list, list of folders to check
    def check_folder(self, folder_list):
        print('--> check folders')
        for f in folder_list:
            if not os.path.exists(f):
                sys.exit('ERROR: folder not exists --> ' + f)
            else:
                print('OK, folder found --> ', f)

    ## @brief Check if file exists
    #
    # @param args, command line arguments
    # @returns fname string, file name
    def check_area_file(self, args):
        print('--> check area filename')
        # variables
        fname = None
        fpath = None
        path = args['proj_dir'] + '/areas/' + str(args['id_area']) + '/'
        fn = str(args['id_area']) + '_' + str(args['id_level'])

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

        return fpath

    ## @brief Check if file exists
    #
    # @param args, command line arguments
    # @returns fname string, file name
    def check_parameter_file(self, args):
        print('--> check parameter filename')
        path = args['proj_dir'] + 'parameters/'  + str(args['id_scenario']) + '/' + str(args['id_param']) + '/map/'
        fn = str(args['id_param']) + '_' + str(args['id_scenario']) + '_' + str(args['id_level']) + '_' + str(args['start_year']) + '_' + str(args['end_year']) + '.int32.2.h5'

        print('--> path :', path)
        print('--> fn :', fn)

        if os.path.exists(path + fn) == True:
            return path + fn
        else:
            return 'error'
    
    ## @brief Check if file exists
    #
    # @param args, command line arguments
    # @returns fname string, file name
    def check_diff_parameter_file(self, args):
        print('--> check diff parameter filename')
        path = args['proj_dir'] + 'parameters/'  + str(args['id_scenario_diff']) + '/' + str(args['id_param_diff']) + '/map/'
        fn = str(args['id_param_diff']) + '_' + str(args['id_scenario_diff']) + '_' + str(args['id_level']) + '_' + str(args['start_year_diff']) + '_' + str(args['end_year_diff']) + '.int32.2.h5'

        print('--> path :', path)
        print('--> fn :', fn)

        if os.path.exists(path + fn) == True:
            return path + fn
        else:
            return 'error'
    
    
    ## @brief Check if file exists
    #
    # @param args, command line arguments
    # @returns fname string, file name
    def check_parameter_abw_file(self, args, idparam):
        print('--> check parameter filename')
        path = args['proj_dir'] + 'parameters/'  + str(args['id_scenario']) + '/' + str(idparam) + '/map/'
        fn = str(idparam) + '_' + str(args['id_scenario']) + '_' + str(args['id_level']) + '_' + str(args['start_year']) + '_' + str(args['end_year']) + '.int32.2.h5'

        print('--> path :', path)
        print('--> fn :', fn)

        if os.path.exists(path + fn) == True:
            return path + fn
        else:
            return 'error'
    
    ## @brief Check if file exists
    #
    # @param args, command line arguments
    # @returns fname string, file name
    def check_parameters_abw_file(self, args):
        print('--> check parameter abw filename')
        
        if args['id_param'] == 19:
            
             parameter_fpath_quotient = self.check_parameter_abw_file(args, 10)
             parameter_fpath_divisor = self.check_parameter_abw_file(args, 32)
        
        if args['id_param'] == 20:
            
             parameter_fpath_quotient = self.check_parameter_abw_file(args, 10)
             parameter_fpath_divisor = self.check_parameter_abw_file(args, 33)
        
        if args['id_param'] == 21:
            
             parameter_fpath_quotient = self.check_parameter_abw_file(args, 10)
             parameter_fpath_divisor = self.check_parameter_abw_file(args, 27)
             
        if args['id_param'] == 22:
            
             parameter_fpath_quotient = self.check_parameter_abw_file(args, 10)
             parameter_fpath_divisor = self.check_parameter_abw_file(args, 12)
             
        if args['id_param'] == 23:
            
             parameter_fpath_quotient = self.check_parameter_abw_file(args, 10)
             parameter_fpath_divisor = self.check_parameter_abw_file(args, 13)
            
        if args['id_param'] == 24:
            
             parameter_fpath_quotient = self.check_parameter_abw_file(args, 10)
             parameter_fpath_divisor = self.check_parameter_abw_file(args, 34)
             
        #print(parameter_fpath_quotient)
        #print(parameter_fpath_divisor)
             
        return parameter_fpath_quotient, parameter_fpath_divisor
    
        
    ## @brief Check if file exists
    #
    # @param args, command line arguments
    # @returns fname string, file name
    def check_parameter_export_file(self, args):
        print('--> check parameter filename')
        path = args['proj_dir']+'parameters/'+str(args['id_scenario'])+'/'+str(args['id_param'])+'/map/'
        fn = str(args['id_param'])+'_'+str(args['id_scenario'])+'_1_'+str(args['start_year'])+'_'+str(args['end_year'])+'.int32.2.h5'
        param_export_fpath = path +fn
    
        print('--> path :', path)
        print('--> fn :', fn)

        if os.path.exists(param_export_fpath) != True:
            exists = 0
        else:
            exists = 1

        return exists, param_export_fpath
    
    
    def create_map_data_abw(self, arg):
        
        #print('--> parameter_fpath :', arg['parameter_fpath'])
        fn_p1 = arg['parameter_fpath_quotient']
        h5_p1 = h5py.File(fn_p1, 'r')
        data_p1 = h5_p1['Band1']
        #print(data_p1.shape, data_p1.dtype)
        
        #print('--> parameter_fpath_diff :', arg['parameter_fpath_diff'])
        fn_p2 = arg['parameter_fpath_divisor']
        h5_p2 = h5py.File(fn_p2, 'r')
        data_p2 = h5_p2['Band1']
        #print(data_p2.shape, data_p2.dtype)
        
        x = h5_p2['x']
        y = h5_p2['y']
        
        data = data_p2[:] / data_p1[:] * 100
        data = np.where((data_p2[:] == float(-99999)),-99999, data)
        
        path = arg['param_export_fpath']
        #print('--> path :', path)
        
        f = h5py.File(path, 'w')
        ds = f.create_dataset('Band1', (data.shape) , dtype=data.dtype)
        ds[:] = data
        f.create_dataset('x', data = x)
        f.create_dataset('y', data = y)
        f.close()
        
    ## @brief Calculate base statstics
    #
    # @param args
    # @param paramter, 3-dimensional array
    # @param area, 2-dimensional array
    # @returns df, pandas dataframe
    def get_base_statistic(self, args, parameter, area):
        
        print('--> get base statistic')
        area = np.where(np.isnan(area), np.nan, 1)
        parameter = np.multiply(area, parameter)
        print(parameter.shape, area.shape)
        print(np.nanmin(area), np.nanmax(area))
        #print(parameter)
        
        df = pd.DataFrame(np.array([area.flatten(), parameter.flatten()]).T)
        #print('--> df 1  :',df)
        df = df.rename(columns={0:'area',1:'value'}).dropna()
        #print('--> df 2 :',df)
        
        # drop values and areas < -99999.. -9999
        df = df[df['value']!=-99999]
        df = df[df['value']!=-9999]
        df = df[df['area']!=-99999]
        df = df[df['area']!=-9999]
        #print('--> df 3 :', df)

        df_stat = {}

        df_stat['mean'] = df['value'].mean()
        df_stat['min'] = df['value'].min()
        df_stat['max'] = df['value'].max()
        df_stat['median'] = df['value'].median()
        df_stat['std'] = df['value'].std()
        df_stat['sum'] = df['value'].sum()

        print('--> df_stat 3 :', df_stat)
        return df_stat

    ## @brief Calculate histogram over a set of areas and time
    #
    # @param args
    # @param paramter, 3-dimensional array
    # @param area, 2-dimensional array
    # @returns df, pandas dataframe
    def get_histogram(self, args, parameter, area):
        
        print('--> get histogram')
        print(parameter.shape)

        # get average over time
        #mean = np.nanmean(parameter, axis=2).round(0)

        #print(mean.shape, area.shape)
        #print('min', np.nanmin(mean),'max', np.nanmax(mean))

        df = pd.DataFrame(np.array([area.flatten(), parameter.flatten()]).T)
        df = df.rename(columns={0:'area',1:'parameter'}).dropna()
        
        # drop values and areas < -99999.. -9999
        df = df[df['parameter']!=-99999]
        df = df[df['parameter']!=-9999]
        df = df[df['area']!=-99999]
        df = df[df['area']!=-9999]
        
        mean = (df['parameter']/10**(self.decimals)).astype(float)
        
        print('--> mean : ', np.nanmean(mean))
        if np.isnan(np.nanmean(mean)) == True:
            sys.exit('ERROR in get_histogram(): parameter array contains NAN only')

        # get parameter classes
        classes = self.get_parameter_classes(args)
        #print('--> classes : ', classes)
        print('--> classes[lower_limit].iloc[0] : ', classes['lower_limit'].iloc[0])
        print('--> classes[lower_limit].iloc[-1] : ', classes['upper_limit'].iloc[-1])
        
        print('--> np.nanmin(mean): : ', np.nanmin(mean))
        print('--> np.nanmax(mean): : ', np.nanmax(mean))

        # set lower limit for bins
        if classes['lower_limit'].iloc[0] == None:
            if classes['upper_limit'].iloc[0] > np.nanmin(mean):
                classes['lower_limit'].iloc[0] = np.nanmin(mean)-1
            else:
                classes['lower_limit'].iloc[0] = float(classes['upper_limit'].iloc[0]-1)
        # set upper limit for bins
        #print('--> classes : ', classes)
        
        if classes['upper_limit'].iloc[-1] == None:
            if classes['lower_limit'].iloc[-1] < np.nanmax(mean):
                classes['upper_limit'].iloc[-1] = np.nanmax(mean)+1
            else:
                classes['upper_limit'].iloc[-1] = float(classes['lower_limit'].iloc[-1]+1)

        print('--> classes : ', classes)

        # get bins for histogram function
        classes['bins'] = classes['upper_limit']
        bins = classes['lower_limit'].astype(float).tolist()
        upper_bin = float(classes['upper_limit'].iloc[-1])
        bins.append(upper_bin)
        print('--> bins : ' + str(bins))

        # calculate histogram
        histo, bins = np.histogram(mean, bins)
        
        classes = self.get_parameter_classes(args)
        print('--> classes : ', classes)
        df_stat = pd.DataFrame([ classes['idclass'], classes['lower_limit'], classes['upper_limit'], classes['red'], classes['green'], classes['blue']]).T
        

        
        print('--> histo : ' + str(histo))
        df_stat['count'] = histo
        
        print('--> df_histo : ', df_stat)

        return df_stat
    
    ## @brief Calculate diff histogram over a set of areas and time
    #
    # @param args
    # @param paramter, 3-dimensional array
    # @param area, 2-dimensional array
    # @returns df, pandas dataframe
    def get_diff_histogram(self, args, parameter, area):
        
        print('--> get diff histogram')
        print('--> self.decimals', self.decimals)

        # get average over time
        #mean = np.nanmean(parameter, axis=2).round(0)

        #print(mean.shape, area.shape)
        #print('min', np.nanmin(mean),'max', np.nanmax(mean))

        df = pd.DataFrame(np.array([area.flatten(), parameter.flatten()]).T)
        df = df.rename(columns={0:'area',1:'parameter'}).dropna()
        
        #print('--> df : ',df)
         
        # drop values and areas < -99999.. -9999
        df = df[df['parameter']!=-99999]
        df = df[df['parameter']!=-9999]
        df = df[df['parameter']!=-999]
        df = df[df['area']!=-99999]
        df = df[df['area']!=-9999]
        df = df[df['area']!=-999]
        
        #print('--> df : ',df)
        
        mean = (df['parameter']/10**(self.decimals)).astype(float)
        
        print('--> mean : ', np.nanmean(mean))
        if np.isnan(np.nanmean(mean)) == True:
            sys.exit('ERROR in get_histogram(): parameter array contains NAN only')

        # get parameter classes
        classes = self.get_diff_parameter_classes(args)
        print('--> classes : ', classes)
        #print('--> classes[lower_limit].iloc[0] : ', classes['lower_limit'].iloc[0])
        #print('--> classes[lower_limit].iloc[-1] : ', classes['upper_limit'].iloc[-1])
        #print('--> mean: : ', mean)
        print('--> np.nanmin(mean): : ', np.nanmin(mean))
        print('--> np.nanmax(mean): : ', np.nanmax(mean))
        
        #print('--> classes[lower_limit].iloc[]: : ', classes['lower_limit'].iloc[2])
        
       # print('--> classes[lower_limit].iloc[0]: : ', classes['lower_limit'].iloc[0])
        #print('--> classes[upper_limit].iloc[0]: : ', classes['upper_limit'].iloc[0])
        
        #print('--> classes[lower_limit].iloc[-1]: : ', classes['lower_limit'].iloc[-1])
        #print('--> classes[upper_limit].iloc[-1]: : ', classes['upper_limit'].iloc[-1])
        
        
        # get diff parameter classes
        classes = self.get_diff_parameter_classes(args)
        classes = classes.sort_values(by=['idclass'], ascending=False)
        #print('--> classes : ' + str(classes))
        
        # set lower limit for bins
        if classes['lower_limit'].iloc[0] == None:
            if float(classes['upper_limit'].iloc[0]) > np.nanmin(mean):
                classes['lower_limit'].iloc[0] = np.nanmin(mean)
            else:
                classes['lower_limit'].iloc[0] = float(classes['upper_limit'].iloc[0])
        # set upper limit for bins
        if classes['upper_limit'].iloc[-1] == None:
            if float(classes['lower_limit'].iloc[-1]) < np.nanmax(mean):
                classes['upper_limit'].iloc[-1] = np.nanmax(mean)
            else:
                classes['upper_limit'].iloc[-1] = float(classes['lower_limit'].iloc[-1])
                
        # get bins for histogram function
        classes['bins'] = classes['upper_limit'].astype(float)
        bins = classes['lower_limit'].astype(float).tolist()
        #upper_bin = classes['upper_limit'].iloc[-1].astype(float)
        upper_bin = float(classes['upper_limit'].iloc[-1])
        bins.append(upper_bin)
        #print(classes)
        print('--> bins : ' + str(bins))
        
        # calculate histogram
        histo, bins = np.histogram(mean, bins)
        print('--> histo : ' + str(histo))
        
        classes = self.get_diff_parameter_classes(args)
        df_stat = pd.DataFrame([ classes['idclass'], classes['lower_limit'], classes['upper_limit'], classes['red'], classes['green'], classes['blue']]).T
        df_stat['count'] = histo
        
        print('--> df_histo : ', df_stat)

        return df_stat
    
    ## @brief Extract data of kliwes for a parameter restricted by area and time
    #
    # @return parameter_selection, numpy array
    # @return area_selection, numpy array
    def extract_parameter(self, args):

        print('--> args : ' + str(args))
        print('--> extract data')
        # get area data as list
        id_area = args['id_area']
        
        print('--> len(argsid_areadata : ' + str(len(args['id_areadata'])))
        
        if len(args['id_areadata']) > 0:
            id_areadata = list(map(int, args['id_areadata']))
        else:
            id_areadata = 'all'
        print('--> id_area : ', id_area, ', number of area data', len(id_areadata))
        # get parameter
        id_param = args['id_param']
        print('--> id_param id : ', id_param)

        # get area
        #try:
        f_area = h5py.File(args['area_fpath'], 'r')
        ds_area = f_area['Band1']
        # select area without spatial restriction
        if id_areadata != 'all':
            area_selection = np.where(np.isin(ds_area, id_areadata), ds_area, np.nan)
        else:
            area_selection = ds_area[:]

        print(area_selection.shape)
        print(np.nanmin(area_selection), np.nanmax(area_selection))
        # restrict area selection to calculated extent
        # get area selection
        a = np.where(~np.isnan(area_selection))
        # get bounding box / extent
        min_col, max_col, min_row, max_row = np.min(a[0]), np.max(a[0]), np.min(a[1]), np.max(a[1])  
        area_selection = area_selection[min_col:max_col,min_row:max_row]
        print('area_selection, shape', area_selection.shape, 'min/max col/row', min_col, max_col, min_row, max_row)
        print(np.nanmin(area_selection), np.nanmax(area_selection))
        
        # get parameter
        #try:
        f_param = h5py.File(args['parameter_fpath'], 'r')
        parameter_selection = np.flipud(f_param['Band1'])
        
        # restrict area selection to calculated extent
        # get area selection
        #a = np.where(~np.isnan(parameter_selection))
        # get bounding box / extent
        #min_col, max_col, min_row, max_row = np.min(a[0]), np.max(a[0]), np.min(a[1]), np.max(a[1])
        parameter_selection = parameter_selection[min_col:max_col,min_row:max_row]
        print('parameter_selection, shape', parameter_selection.shape, 'min/max col/row', min_col, max_col, min_row, max_row)

        #print('-->  parameter_selection[:,:,0]: ',parameter_selection[:,:,0])
        #print('-->  area_selection[:,:,0]: ',area_selection[:,:,0])

        # return area as 2-dimensional array
        return parameter_selection, area_selection
        #return parameter_selection[:,:], area_selection[:,:]
    
    ## @brief Extract data of kliwes for a diff parameter restricted by area and time
    #
    # @return parameter_selection, numpy array
    # @return area_selection, numpy array
    def extract_diff_parameter(self, args):

        print('--> args : ' + str(args))
        print('--> extract diff data')
        # get area data as list
        id_area = args['id_area']
        
        print('--> len(argsid_areadata : ' + str(len(args['id_areadata'])))
        
        if len(args['id_areadata']) > 0:
            id_areadata = list(map(int, args['id_areadata']))
        else:
            id_areadata = 'all'
        print('--> id_area : ', id_area, ', number of area data', len(id_areadata))
        # get parameter
        id_param = args['id_param_diff']
        print('--> id_param id : ', id_param)

        # get area
        #try:
        f_area = h5py.File(args['area_fpath'], 'r')
        ds_area = f_area['Band1']
        # select area without spatial restriction
        if id_areadata != 'all':
            area_selection = np.where(np.isin(ds_area, id_areadata), ds_area, np.nan)
        else:
            area_selection = ds_area[:]

        print(area_selection.shape)
        # restrict area selection to calculated extent
        # get area selection
        a = np.where(~np.isnan(area_selection))
        # get bounding box / extent
        min_col, max_col, min_row, max_row = np.min(a[0]), np.max(a[0]), np.min(a[1]), np.max(a[1])
        area_selection = area_selection[min_col:max_col,min_row:max_row]
        print('area_selection, shape', area_selection.shape, 'min/max col/row', min_col, max_col, min_row, max_row)
        print(np.nanmin(area_selection), np.nanmax(area_selection))
        
        # get parameter
        #try:
        f_param = h5py.File(args['diff_parameter_fpath'], 'r')
        #parameter_selection = f_param['Band1']
        parameter_selection = np.flipud(f_param['Band1'])
        
        # restrict area selection to calculated extent
        # get area selection
        #a = np.where(~np.isnan(parameter_selection))
        # get bounding box / extent
        #min_col, max_col, min_row, max_row = np.min(a[0]), np.max(a[0]), np.min(a[1]), np.max(a[1])
        parameter_selection = parameter_selection[min_col:max_col,min_row:max_row]
        print('parameter_selection, shape', parameter_selection.shape, 'min/max col/row', min_col, max_col, min_row, max_row)

        #print('-->  parameter_selection[:,:,0]: ',parameter_selection[:,:,0])
        #print('-->  area_selection[:,:,0]: ',area_selection[:,:,0])

        # return area as 2-dimensional array
        return parameter_selection, area_selection
        #return parameter_selection[:,:], area_selection[:,:]
    
    
    ## @brief Get parameter classes from database
    #
    # @param args
    # @returns df, pandas dataframe
    def get_parameter_classes(self, args):
        print('--> get parameter classes')
        db = pg()
        db.dbConfig(self.conf['dbconfig'])
        db.dbConnect()
        
        sql = 'SELECT count(*) AS count FROM sessions.class_description WHERE idtab = '+ str(args['id_tab']) + ' AND idparam = ' + str(args['id_param'])
        res = db.tblSelect(sql)

        count = int(np.array(res[0][0]).item())
        print('--> count', count)
        
        if count > 0:
            sql = 'SELECT idclass, lower_limit, upper_limit, red, green, blue FROM sessions.class_description where idtab = '+ str(args['id_tab']) + ' AND idparam = ' + str(args['id_param']) + ' order by idclass'

        else:
         sql = 'SELECT idclass, lower_limit, upper_limit, red, green, blue FROM viewer_data.class_description where idparam = ' + str(args['id_param']) + ' order by idclass'
        
        
        res = db.tblSelect(sql)
        #print('--> res',res)
        db.dbClose()
        if res[1] > 0:
            classes = np.array(res[0])
            df_classes = pd.DataFrame([classes[:,0],classes[:,1],classes[:,2],classes[:,3],classes[:,4],classes[:,5]]).T
            df_classes = df_classes.rename(columns={0:'idclass', 1:'lower_limit', 2:'upper_limit', 3:'red', 4:'green', 5:'blue'})
        else:
            sys.exit('ERROR: no parameter classes found')

        print('OK')
        return df_classes

   	## @brief Get diff parameter classes from database
    #
    # @param args
    # @returns df, pandas dataframe
    def get_diff_parameter_classes(self, args):
        print('--> get parameter classes')
        db = pg()
        db.dbConfig(self.conf['dbconfig'])
        db.dbConnect()

        sql = 'SELECT count(*) AS count FROM sessions.class_description_diff WHERE idtab = '+ str(args['id_tab']) + ' AND idparam = ' + str(args['id_param'])
        res = db.tblSelect(sql)

        count = int(np.array(res[0][0]).item())
        print('--> count', count)
        
        if count > 0:
            sql = 'SELECT idclass, lower_limit, upper_limit, red, green, blue FROM sessions.class_description_diff where idtab = '+ str(args['id_tab']) + ' AND idparam = ' + str(args['id_param']) + ' order by idclass desc'

        else:
            sql = 'SELECT idclass, lower_limit, upper_limit, red, green, blue FROM viewer_data.class_description_diff where idparam = ' + str(args['id_param']) + ' order by idclass desc'
        
        res = db.tblSelect(sql)
        #print(res[0])
        #print('--> res',res)
        db.dbClose()
        if res[1] > 0:
            classes = np.array(res[0])
            df_classes = pd.DataFrame([classes[:,0],classes[:,1],classes[:,2],classes[:,3],classes[:,4],classes[:,5]]).T
            df_classes = df_classes.rename(columns={0:'idclass', 1:'lower_limit', 2:'upper_limit', 3:'red', 4:'green', 5:'blue'})
        else:
            sys.exit('ERROR: no parameter classes found')
        #print(df_classes)
        print('OK')
        return df_classes
    
