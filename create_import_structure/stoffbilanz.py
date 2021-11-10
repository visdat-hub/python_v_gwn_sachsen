## @package Read stoffbilanz datasets
import sys
import os
import h5py
import pandas as pd
import numpy as np
import geopandas as gpd

class stoffbilanz_import:
    def __init__(self, args):
        """ configure output """
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.conf = {
            'stoffbilanz' : {
                'nssommer' : {'id':6, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5},
                'nswinter' : {'id':7, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5},
                'gesamtabfluss' : {'id':33, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5},
                'abw_gwn' : {'id':19, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5},
                'abw_r' : {'id':20, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5},
                'gwn' : {'id':32, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5},
                'etp' :{'id':31, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5},
                'ns' : {'id':10, 'decimals':2, 'dtype':'int32', 'to_hdf' : True, 'id_scenario' :5}
            }
        }

    ## @brief Read shapefiles / dbase files
    #
    # @param src, string : source folder
    # @return dbf_list, list: list of files
    def read_shapefiles(self, src):
        print('--> read stoffbilanz shapefiles')
        files = []

        # get stoffbilanz dbase of shapefiles
        if 'stoffbilanz' in src:
            for r, d, f in os.walk(src):
                print(r)
                if 'stoffbilanz_gis' not in r :
                    for file in f:
                        if '.dbf' in file:
                            if not 'lock' in file: # exlude locked files
                                if not '.txt' in file:
                                    files.append(os.path.join(r, file))

        if len(files) > 0:
            print('OK, found %d dbf files' % len(files))
            #for i in files:
            #    print(i)
        else:
            sys.exit('ERROR: No dbf files found')
        #print(files)
        return files

    ## @brief read dbase into pandas dataframe
    #
    # @param args, dict: dictionary of arguments
    # @returns stacked_df: pandas dataframe
    def read_as_pandas(self, args):
        print('--> read dbase into pandas dataframe')
        df_all = {}
        u_pnames = []

        # get unique parameters
        for f in args['dbf_files']:
            pname, idparam = self.get_parameter_from_fname(args, f)
            years_range = self.get_years_from_fname(f)
            if not pname in u_pnames:
                u_pnames.append([pname, idparam, years_range[0], years_range[1], f])
        #print('>>>>> parameters', u_pnames)

        # loop over parameter names
        for item in u_pnames:
            pname = item[0]
            #if pname == 'etp':
            parameterId = item[1]
            minYear = item[2]
            maxYear = item[3]
            f = item[4]

            # create a key in dict if not exists
            if not pname in df_all.keys():
                df_all[pname] = pd.DataFrame()
            # read dbase file
            print('reading...', pname, minYear, '->', maxYear)
            table = gpd.read_file(f)
            #print(table.keys())
            df = pd.DataFrame(table.drop(columns='geometry'))
            df['min_year'] = int(minYear)
            df['max_year'] = int(maxYear)
            #print(df)
            df_all[pname] = pd.concat([df_all[pname], df])
            #sys.exit()
            #break


        #print(df_all)
        #sys.exit()
        return df_all

    ## @brief Save parameters as hdf5
    #
    # @param df, pandas DataFrame
    def transform_as_hdf(self, args, dict):
        print('--> transform data to hdf5')
        for p in dict:
            if self.conf['stoffbilanz'][p]['to_hdf'] == True:
                print('parameter --> ', p)
                df_all = dict[p]
                print(df_all.keys())
                start_year = df_all['min_year'].min()
                last_year = df_all['max_year'].max()
                for y in range(int(start_year),int(last_year)+1):
                    print(p, y)
                    # select data for a defined years range
                    df = df_all.loc[(df_all['min_year'] <= y) & (df_all['max_year'] > y)]
                    if y == (int(last_year)):
                        print('--> last year')
                        df = df_all.loc[(df_all['min_year'] <= y) & (df_all['max_year'] >= y)]
                    # get data
                    df_param = pd.DataFrame()
                    df_param['idgrid'] = df['IDGRID']
                    #print(df_param)
                    #df_param['year'] = y
                    df_param['jan'],df_param['feb'],df_param['mar'],df_param['apr'],df_param['may'],df_param['jun'],df_param['jul'],df_param['aug'],df_param['sep'],df_param['oct'],df_param['nov'],df_param['dec'] = df['VAL'],df['VAL'],df['VAL'],df['VAL'],df['VAL'],df['VAL'],df['VAL'],df['VAL'],df['VAL'],df['VAL'],df['VAL'],df['VAL']
                    #break
                    df_final = pd.DataFrame(columns=['index','values_block_0'])
                    df_final['index'] = df_param['idgrid']
                    df_final['values_block_0'] = df_param[df_param.columns[1:]].apply(
                        lambda x: ' '.join(x.astype(str)),
                        axis=1
                    )
                    df_final = df_final.set_index('index')
                    #print(df_final)
                    #print(df_final.dtypes)
                    #sys.exit()
                    # save as hdf5
                    path = self.args['dst_folder'] + '/' + str(self.args['scenario_id']) + '/' + str(self.conf['stoffbilanz'][p]['id']) + '/'
                    if not os.path.exists(path):
                        os.makedirs(path)
                    fn = str(self.args['scenario_id']) + '_' + str(self.conf['stoffbilanz'][p]['id']) + '_' + str(y) + '.h5'
                    print(path + fn)
                    try:
                        df_final[['values_block_0']].to_hdf(path + fn, key='table' , mode='w', format='table')
                    except:
                        sys.exit("ERROR: " + str(sys.exc_info()))
                    #sys.exit()
                    #break
            #break


    ## @brief Get parameter name and id from filename
    #
    # @param filename: string
    # @returns pname, id: integer
    def get_parameter_from_fname(self, args, file):
        #print('--> get parameter name and id from filename')
        pname, idparam = None, None
        for p in self.conf[args['data_type']]:
            if p in file:
                idparam = self.conf[args['data_type']][p]['id']
                pname = p
                break

        if pname == None or idparam == None:
            sys.exit('ERROR: Missing parameter name or parameter id')

        return pname, idparam

    ## @brief Get years span from filename
    #
    # @param filename: string
    # @returns year_array: [integer], min_year, max_year
    def get_years_from_fname(self, file):
        #print('--> get year and month from filename')
        years = []

        min_year = file.split('.')[0].split('_')[-2]
        max_year = file.split('.')[0].split('_')[-1]
        years = [min_year, max_year]
        #print(years)

        if len(years) == 0 or len(years) > 2:
            sys.exit('ERROR: Missing years')

        return years
