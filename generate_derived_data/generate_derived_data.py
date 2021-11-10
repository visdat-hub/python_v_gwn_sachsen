
## @package generate_derived_data
# Generation of GWN parameters from calculated stats, e.g. Abflusssbeiwert
#
# cd /mnt/visdat/Projekte/2020/GWN\ viewer/dev/python/generate_derived_data/
# Usage: python3 ./generate_derived_data.py -tgt_param gwn_abw -stat_type yearly -src_id_scenario 0 -id_areas [0,1,2] -proj_dir /mnt/galfdaten/daten_stb/gwn_sachsen/
#
# tgt_param: gwn_abw, r_abw, rd_abw, rg1_abw, rg2_abw, sicker_abw
# id_areas: array of id_areas
# stat_type: yearly, monthly

import os
import sys
import pandas as pd
import numpy as np
import h5py

class generate_derived_data:
    def __init__(self):
        """ configure output """
        ## configuration of parameter properties
        self.conf = {
            'derived_parameters' : {
                'gwn_abw' : {'id':19, 'decimals':3, 'dtype':'int32', 'to_hdf' : True},
                'r_abw' : {'id':20, 'decimals':3, 'dtype':'int32', 'to_hdf' : True},
                'rd_abw' : {'id':21, 'decimals':3, 'dtype':'int32', 'to_hdf' : True},
                'rg1_abw' : {'id':22, 'decimals':3, 'dtype':'int32', 'to_hdf' : True},
                'rg2_abw' : {'id':23, 'decimals':3, 'dtype':'int32', 'to_hdf' : True},
                'sicker_abw' : {'id':24, 'decimals':3, 'dtype':'int32', 'to_hdf' : True},
                'r_abw_difga' : {'id':20, 'decimals':3, 'dtype':'int32', 'to_hdf' : True},
                'r_difga' : {'id':33, 'decimals':3, 'dtype':'int32', 'to_hdf' : True}
            }
        }

    ## @brief Get command line arguments.
    #
    # @returns args Array of arguments
    def get_arguments(self):
        print("--> get caller arguments")
        tgt_param, src_id_scenario, proj_dir, id_areas, stat_type = None, None, None, None, None
        i = 1
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg == '-tgt_param':
                i = i + 1
                tgt_param = sys.argv[i]
            elif arg == '-src_id_scenario':
                i = i + 1
                src_id_scenario = sys.argv[i]
            elif arg == '-proj_dir':
                i = i + 1
                proj_dir = sys.argv[i]
            elif arg == '-id_areas':
                i = i + 1
                id_areas = sys.argv[i]
            elif arg == '-stat_type':
                i = i + 1
                stat_type = sys.argv[i]
            i = i + 1

        if tgt_param == None or src_id_scenario == None or proj_dir == None or id_areas == None or stat_type == None:
            print("ERROR: arguments of program call missing")
            sys.exit()

        return [tgt_param, src_id_scenario, proj_dir, stat_type, id_areas]

    ## @brief Get area identifiers as array
    #
    # @returns areas Array of identifiers
    def get_areas(self, areas):
        print('--> get areas')
        areas = areas.split(',')
        areas = list(map(int, areas))
        return areas

    ## @brief Abflusssbeiwert for r
    #
    # sum of parameters 12, 13, 25, 26, 27, 28, 29) diveded by 10
    def derive_abw(self, args, areas, flow_params, tgt_param):
        print('--> abw')
        id_scenario = args[1]
        proj_dir = args[2]
        stat_type = args[3]
        # loop over areas
        for area in areas:
            print('++++++++++++++++++++++++++')
            print('--> id_area: ' + str(area) )
            if stat_type == 'monthly':
                print(stat_type)
                # get precipitation
                # get flow components

            if stat_type == 'yearly':
                print(stat_type)
                # get precipitation
                src_path = proj_dir + '/parameters/' + str(id_scenario) + '/10/total/10_' + str(id_scenario) +'.stats.h5'
                if os.path.isfile(src_path):
                    print(src_path)
                    p_HDFStore = pd.HDFStore(src_path, mode='r')
                    #print('1')
                    df_p = pd.read_hdf(p_HDFStore , key='/areas/' + str(area) + '/table')
                    #print('2')
                   # print(df_p)
                    df_p_areas = pd.DataFrame([df_p.pop('id_area'), df_p.pop('area')]).T
                    print(df_p.shape, df_p_areas.shape)
                else:
                    sys.exit('ERROR: precipitation file not found: ' + src_path)
                # get flow components
                d = 0
                for flow_p in flow_params:
                    print('--> read ' + str(flow_p))
                    src_path = proj_dir + '/parameters/' + str(id_scenario) + '/' + str(flow_p) + '/total/' + str(flow_p) + '_' + str(id_scenario) +'.stats.h5'
                    fp_HDFStore = pd.HDFStore(src_path, mode='r')
                    df_flow_p = pd.read_hdf(fp_HDFStore , key='/areas/' + str(area) + '/table')
                    df_flow_p_idareas = pd.DataFrame([df_flow_p.pop('id_area'), df_flow_p.pop('area')]).T
                    if len(df_flow_p.shape) == 2:
                        df_flow_p = np.expand_dims(df_flow_p, axis = 0)
                    if d == 0:
                        np_flow = df_flow_p
                    else:
                        np_flow = np.concatenate((np_flow, df_flow_p), axis=0)
                    d = d + 1
                # summarize flow components
                #sys.exit()
                print ('shape of flow_p', np_flow.shape)
                flow_p_sum = np.sum(np_flow, axis = 0)
                print(flow_p_sum.shape)
                # get Abflusssbeiwert
                abw = np.divide(flow_p_sum, df_p)
                
                #sys.exit()
                # merge idareas and abw
                df = pd.concat([df_p_areas, abw], axis=1)
                print(df.shape)
                #sys.exit()
                # save dataframe
                tgt_id = self.conf['derived_parameters'][str(tgt_param)]['id']
                path = proj_dir + '/parameters/' + str(id_scenario) + '/' + str(tgt_id) + '/total/'
                fn = str(tgt_id) + '_' + str(id_scenario) +'.stats.h5'
                print(path, fn)
                if not os.path.exists(path):
                    os.makedirs(path)
                #print(df)
                #df[df==0] = np.nan

                # set dtype for hdf5 table
                namesList = df.columns
                formatList = [(np.float)] * len(namesList)
                # print(formatList)
                ds_dt = np.dtype({'names':namesList,'formats':formatList})
                # rename colnames and table name
                with h5py.File(path + fn) as f:
                    if not 'areas' in f:
                        f.create_group('areas')
                    if not str(area) in f['areas']:
                        f['areas'].create_group(str(area))
                    if 'table' in f['areas'][str(area)]:
                        del f['areas'][str(area)]['table'] # delete table if exists
                    d = f['areas'][str(area)].create_dataset('table', ((df.shape)[0],), dtype = ds_dt)
                    for col in namesList:
                        d[col] = df[col]
                f.close()

            if stat_type == 'monthly':
                print(stat_type)
                # looping over years
                src_path = proj_dir + '/parameters/' + str(id_scenario) + '/10/month/'
                for r, d, f in os.walk(src_path):
                    year = r.split('/')[-1]
                    print(year)
                    for file in f:
                        # get precipitation for a month
                        p_HDFStore = pd.HDFStore(r + '/' + file, mode='r')
                        df_p = pd.read_hdf(p_HDFStore , key='/areas/' + str(area) + '/table')
                        df_p_areas = pd.DataFrame([df_p.pop('id_area'), df_p.pop('area')]).T
                        print(df_p.shape, df_p_areas.shape)
                        # get flow components
                        d = 0
                        for flow_p in flow_params:
                            print('--> read ' + str(flow_p))
                            src_param = proj_dir + '/parameters/' + str(id_scenario) + '/' + str(flow_p) + '/month/' + str(year) + '/' + str(flow_p) + '_' + str(id_scenario) +'.stats.h5'
                            fp_HDFStore = pd.HDFStore(src_param, mode='r')
                            df_flow_p = pd.read_hdf(fp_HDFStore , key='/areas/' + str(area) + '/table')
                            df_flow_p_idareas = pd.DataFrame([df_flow_p.pop('id_area'), df_flow_p.pop('area')]).T
                            if len(df_flow_p.shape) == 2:
                                df_flow_p = np.expand_dims(df_flow_p, axis = 0)
                            if d == 0:
                                np_flow = df_flow_p
                            else:
                                np_flow = np.concatenate((np_flow, df_flow_p), axis=0)
                            d = d + 1
                        # summarize flow components
                        print ('shape of flow_p', np_flow.shape)
                        flow_p_sum = np.sum(np_flow, axis = 0)
                        #flow_p_sum[flow_p_sum == 0] = np.nan
                        print(flow_p_sum.shape)
                        # get Abflusssbeiwert
                        df_p = df_p.fillna(0)
                        
                        print('--> df_p.shape', df_p.shape)
                        print('--> flow_p_sum ',flow_p_sum)
                        print('-->  df_p',df_p)
                        abw = np.divide(flow_p_sum, df_p)
                        # merge idareas and abw
                        #print(flow_p_sum[21,:])
                        #print(df_p.iloc[21])
                        #print(abw.iloc[21])     
                        #print(df_p[df_p==0])
                        df = pd.concat([df_p_areas, abw], axis=1)#.fillna(0)
                        #print(df)
                        print(df.shape)

                        # save dataframe
                        tgt_id = self.conf['derived_parameters'][str(tgt_param)]['id']
                        path = proj_dir + '/parameters/' + str(id_scenario) + '/' + str(tgt_id) + '/month/' + str(year) + '/'
                        fn = str(tgt_id) + '_' + str(id_scenario) +'.stats.h5'
                        print(path, fn)
                        if not os.path.exists(path):
                            os.makedirs(path)
                        #print(df)
                        #print (df.columns)
                        #df[df[['area', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']]==0] = np.nan
                        #print(df.iloc[21])
                        # set dtype for hdf5 table
                        namesList = df.columns
                        formatList = [(np.float)] * len(namesList)
                        # print(formatList)
                        ds_dt = np.dtype({'names':namesList,'formats':formatList})
                        # rename colnames and table name
                        with h5py.File(path + fn) as f:
                            if not 'areas' in f:
                                f.create_group('areas')
                            if not str(area) in f['areas']:
                                f['areas'].create_group(str(area))
                            if 'table' in f['areas'][str(area)]:
                                del f['areas'][str(area)]['table'] # delete table if exists
                            d = f['areas'][str(area)].create_dataset('table', ((df.shape)[0],), dtype = ds_dt)
                            for col in namesList:
                                d[col] = df[col]
                        f.close()
                        
                        #if year == '1967':                            
                        #    sys.exit()
                        
    
    def derive_r(self, args, areas, flow_params, tgt_param):
        """r"""        
        print('--> r')
        id_scenario = args[1]
        proj_dir = args[2]
        stat_type = args[3]
        # loop over areas
        for area in areas:
            print('++++++++++++++++++++++++++')
            print('--> id_area: ' + str(area) )
            if stat_type == 'monthly':
                print(stat_type)
                # get precipitation
                # get flow components

            if stat_type == 'yearly':
                print('--> ' + stat_type)
                # get precipitation
                src_path = proj_dir + '/parameters/' + str(id_scenario) + '/10/total/10_' + str(id_scenario) +'.stats.h5'
                if os.path.isfile(src_path):
                    print(src_path)
                    p_HDFStore = pd.HDFStore(src_path, mode='r')
                    df_p = pd.read_hdf(p_HDFStore , key='/areas/' + str(area) + '/table')
                    df_p_areas = pd.DataFrame([df_p.pop('id_area'), df_p.pop('area')]).T
                    print(df_p.shape, df_p_areas.shape)
                else:
                    sys.exit('ERROR: precipitation file not found: ' + src_path)
                # get flow components
                d = 0
                for flow_p in flow_params:
                    print('--> read ' + str(flow_p))
                    src_path = proj_dir + '/parameters/' + str(id_scenario) + '/' + str(flow_p) + '/total/' + str(flow_p) + '_' + str(id_scenario) +'.stats.h5'
                    fp_HDFStore = pd.HDFStore(src_path, mode='r')
                    df_flow_p = pd.read_hdf(fp_HDFStore , key='/areas/' + str(area) + '/table')
                    df_flow_p_idareas = pd.DataFrame([df_flow_p.pop('id_area'), df_flow_p.pop('area')]).T
                    if len(df_flow_p.shape) == 2:
                        df_flow_p = np.expand_dims(df_flow_p, axis = 0)
                    if d == 0:
                        np_flow = df_flow_p
                    else:
                        np_flow = np.concatenate((np_flow, df_flow_p), axis=0)
                    d = d + 1
                # summarize flow components
                print ('shape of flow_p', np_flow.shape)
                flow_p_sum = np.sum(np_flow, axis = 0)
                print(flow_p_sum.shape)
                # get Abflusssbeiwert
                df_p[df_p.columns] = 1
                abw = np.divide(flow_p_sum, df_p)
                # merge idareas and abw
                df = pd.concat([df_p_areas, abw], axis=1)
                print(df.shape)

                # save dataframe
                tgt_id = self.conf['derived_parameters'][str(tgt_param)]['id']
                path = proj_dir + '/parameters/' + str(id_scenario) + '/' + str(tgt_id) + '/total/'
                fn = str(tgt_id) + '_' + str(id_scenario) +'.stats.h5'
                #sys.exit()
                print(path, fn)
                if not os.path.exists(path):
                    os.makedirs(path)
                #print(df)
                #df[df==0] = np.nan
                #sys.exit('ERROR')

                # set dtype for hdf5 table
                namesList = df.columns
                formatList = [(np.float)] * len(namesList)
                # print(formatList)
                ds_dt = np.dtype({'names':namesList,'formats':formatList})
                # rename colnames and table name
                with h5py.File(path + fn) as f:
                    if not 'areas' in f:
                        f.create_group('areas')
                    if not str(area) in f['areas']:
                        f['areas'].create_group(str(area))
                    if 'table' in f['areas'][str(area)]:
                        del f['areas'][str(area)]['table'] # delete table if exists
                    d = f['areas'][str(area)].create_dataset('table', ((df.shape)[0],), dtype = ds_dt)
                    for col in namesList:
                        d[col] = df[col]
                f.close()

            if stat_type == 'monthly':
                print(stat_type)
                # looping over years
                src_path = proj_dir + '/parameters/' + str(id_scenario) + '/10/month/'
                for r, d, f in os.walk(src_path):
                    year = r.split('/')[-1]
                    print(year)
                    for file in f:
                        # get precipitation for a month
                        p_HDFStore = pd.HDFStore(r + '/' + file, mode='r')
                        df_p = pd.read_hdf(p_HDFStore , key='/areas/' + str(area) + '/table')
                        df_p_areas = pd.DataFrame([df_p.pop('id_area'), df_p.pop('area')]).T
                        print(df_p.shape, df_p_areas.shape)
                        # get flow components
                        d = 0
                        for flow_p in flow_params:
                            print('--> read ' + str(flow_p))
                            src_param = proj_dir + '/parameters/' + str(id_scenario) + '/' + str(flow_p) + '/month/' + str(year) + '/' + str(flow_p) + '_' + str(id_scenario) +'.stats.h5'
                            fp_HDFStore = pd.HDFStore(src_param, mode='r')
                            df_flow_p = pd.read_hdf(fp_HDFStore , key='/areas/' + str(area) + '/table')
                            df_flow_p_idareas = pd.DataFrame([df_flow_p.pop('id_area'), df_flow_p.pop('area')]).T
                            if len(df_flow_p.shape) == 2:
                                df_flow_p = np.expand_dims(df_flow_p, axis = 0)
                            if d == 0:
                                np_flow = df_flow_p
                            else:
                                np_flow = np.concatenate((np_flow, df_flow_p), axis=0)
                            d = d + 1
                        # summarize flow components
                        print ('shape of flow_p', np_flow.shape)
                        flow_p_sum = np.sum(np_flow, axis = 0)
                        print(flow_p_sum.shape)
                        # get Abflusssbeiwert
                        df_p[df_p.columns] = 1
                        abw = np.divide(flow_p_sum, df_p)
                        # merge idareas and abw
                        df = pd.concat([df_p_areas, abw], axis=1)#.fillna(0)
                        #print(df)
                        #sys.exit()
                        # save dataframe
                        tgt_id = self.conf['derived_parameters'][str(tgt_param)]['id']
                        path = proj_dir + '/parameters/' + str(id_scenario) + '/' + str(tgt_id) + '/month/' + str(year) + '/'
                        fn = str(tgt_id) + '_' + str(id_scenario) +'.stats.h5'
                        print(path, fn)
                        if not os.path.exists(path):
                            os.makedirs(path)
                        #print(df)
                        #df[df==0] = np.nan

                        # set dtype for hdf5 table
                        namesList = df.columns
                        formatList = [(np.float)] * len(namesList)
                        # print(formatList)
                        ds_dt = np.dtype({'names':namesList,'formats':formatList})
                        # rename colnames and table name
                        with h5py.File(path + fn) as f:
                            if not 'areas' in f:
                                f.create_group('areas')
                            if not str(area) in f['areas']:
                                f['areas'].create_group(str(area))
                            if 'table' in f['areas'][str(area)]:
                                del f['areas'][str(area)]['table'] # delete table if exists
                            d = f['areas'][str(area)].create_dataset('table', ((df.shape)[0],), dtype = ds_dt)
                            for col in namesList:
                                d[col] = df[col]
                        f.close()
        

if __name__ ==  '__main__':
    # start program
    print ("--> starting derived parameter generation...")

    f = generate_derived_data()
    # get command line arguments
    args = f.get_arguments()

    # calculate parameters
    tgt_param = args[0]
    
    # get area ids
    areas = f.get_areas(args[4])
    
    # Difga
    if tgt_param == 'r_difga':
        flow_params = [12, 13 ,16]
        f.derive_r(args, areas, flow_params, tgt_param)
        
    if tgt_param == 'r_abw_difga':
        flow_params = [12, 13, 16]
        f.derive_abw(args, areas, flow_params, tgt_param)
        
        
    if tgt_param == 'rd_abw':
       flow_params = [16]
       f.derive_abw(args, areas, flow_params, tgt_param)
    if tgt_param == 'rg1_abw':
        flow_params = [12]
        f.derive_abw(args, areas, flow_params, tgt_param)
    if tgt_param == 'rg2_abw':
        flow_params = [13]
        f.derive_abw(args, areas, flow_params, tgt_param)
    if tgt_param == 'sicker_abw':
        flow_params = [34]
        f.derive_abw(args, areas, flow_params, tgt_param)
    if tgt_param == 'gwn_abw':
        flow_params = [32]
        f.derive_abw(args, areas, flow_params, tgt_param)
    if tgt_param == 'r_abw':
        flow_params = [33]
        f.derive_abw(args, areas, flow_params, tgt_param)
    
    