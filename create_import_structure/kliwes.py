## @package Read kliwes datasets
import sys
import os
import h5py
import pandas as pd
import numpy as np
import zipfile

class kliwes_import:
    def __init__(self, args):
        """ configure output """
        ## command line arguments
        self.args = args
        ## configuration of parameter properties
        self.conf = {
            'parameters' : {
                'pi' : {'id':1, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'rg1' : {'id':12, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'rg2' : {'id':13, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'drain' :{'id':25, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'rh' : {'id':26, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'ro' : {'id':27, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'mkr' : {'id':28, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'tkr' : {'id':29, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'er' : {'id':31, 'decimals':2, 'dtype':'int32', 'to_hdf' : True}
            },
            'derived_parameters' : {
                'pi_summer' : {'id':6, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'pi_winter' : {'id':7, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'rd' : {'id':16, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'gwn' : {'id':32, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'q_gesamt' : {'id':33, 'decimals':2, 'dtype':'int32', 'to_hdf' : True},
                'sicker' : {'id':34, 'decimals':2, 'dtype':'int32', 'to_hdf' : True}
            },
            'scenarios': {
                '00wx' : {'id' : 12},
                '11wx' : {'id' : 13},
                '22wx' : {'id' : 14},
                '33wx' : {'id' : 15},
                '44wx' : {'id' : 16},
                '55wx' : {'id' : 17},
                '66wx' : {'id' : 18},
                '77wx' : {'id' : 19},
                '88wx' : {'id' : 20},
                '99wx' : {'id' : 21},
                'ist' : {'id' : 5}
            }
           
        }
        """
         'scenarios': {
                '55wx' : {'id' : 17},
                '66wg' : {'id' : 10},
                'ist' : {'id' : 0}
            }
        """
        """
        'scenarios': {
                '00wg_f' : {'id' : 7},
                '00wg_v' : {'id' : 8},
                '00wg_w' : {'id' : 9},
                '00wg'   : {'id' : 6},
                '66wg' : {'id' : 10},
                '99wg' : {'id' : 11},
                '00wx' : {'id' : 12},
                '11wx' : {'id' : 13},
                '22wx' : {'id' : 14},
                '33wx' : {'id' : 15},
                '44wx' : {'id' : 16},
                '55wx' : {'id' : 17},
                '66wx' : {'id' : 18},
                '77wx' : {'id' : 19},
                '88wx' : {'id' : 20},
                '99wx' : {'id' : 21},
                'ist' : {'id' : 0}
            }
        """

    ## @brief Read recursively csv files in folder and subfolders from zipfiles
    #
    # @param src, string : source folder
    # @return csv_list, list: list of files
    """def read_csv(self, src):
        print('--> read csv files')
        files = []

        # get KLIWES files
        if 'kliwes' in src:
            for r, d, f in os.walk(src):
                for file in f:
                    #if '.csv' in file:
                    #    files.append(os.path.join(r, file))
                    if '.zip' in file:
                        zf = os.path.join(r, file)
                        with zipfile.ZipFile(zf,'r') as z:
                            filelist = z.namelist()
                            for xf in filelist:
                                if '.csv' and '66wg' in xf:
                                    files.append([zf, xf])
                                if '.csv' and 'KohleLeipzig_55wx'in xf:
                                    files.append([zf, xf])

        if len(files) > 0:
            print('OK, found %d csv files' % len(files))
            for i in files:
                print(i)
        else:
            sys.exit('ERROR: No csv files found')
        return files"""

    ## @brief Read recursively csv files in folder and subfolders from zipfiles for each scenario
    #
    # @param src, string : source folder
    # @return csv_list, list: list of files
    def read_csv_by_scenario(self, src):
        print('--> read csv files')
        files = {}

        # get KLIWES files
        if 'kliwes' in src:
            for scenario in self.conf['scenarios']:
                print('--> reading files for scenario', scenario)
                files[scenario] = []
                for r, d, f in os.walk(src):
                    for file in f:
                        if '.zip' in file:
                            zf = os.path.join(r, file)
                            with zipfile.ZipFile(zf,'r') as z:
                                filelist = z.namelist()
                                for xf in filelist:
                                    if '.csv' and scenario in xf:
                                        files[scenario].append([zf, xf])
                                        break

                print(files[scenario])
        return files

    ## @brief read csv into pandas dataframe
    #
    # @param args, dict: dictionary of arguments
    # @returns stacked_df: pandas dataframe
    def read_as_pandas_by_scenarios(self, args):
        print('--> read csv into pandas dataframe sorted by scenarios')
        dict_df = {} # container for pandas dataframes

        # loop over scenarios
        for scenario in args['csv_files']:
            print('-->', scenario)
            dict_df[scenario] = pd.DataFrame()
            fileList = args['csv_files'][scenario]
            # read csv file
            for f in fileList:
                print('reading', f)
                zf = zipfile.ZipFile(f[0],'r')
                namelist = zf.namelist()
                for fname in namelist:
                    if fname == f[1]: #and 'Kohle' in f[1]:
                        print('unzip ', f[1])
                        unzipped = zf.open(f[1])
                        #data = pd.read_csv(unzipped, sep=';', header=0, decimal=',')
                        data = pd.read_csv(unzipped, encoding='ISO-8859-1', sep=';', header=0, decimal=',')
                        dict_df[scenario] = pd.concat([dict_df[scenario], data])
                #break

            print("OK, read %d Megabytes " % (np.asarray(dict_df[scenario]).nbytes / 1000000))
            #break
        #sys.exit()
        return dict_df

    ## @brief read csv into pandas dataframe
    #
    # @param args, dict: dictionary of arguments
    # @returns stacked_df: pandas dataframe
    def read_as_pandas(self, args):
        print('--> read csv into pandas dataframe')

        # placeholder
        stacked_df = pd.DataFrame()
        kohle_df = pd.DataFrame()
        # read csv file
        for f in args['csv_files']:
            print('reading', f)
            zf = zipfile.ZipFile(f[0],'r')
            namelist = zf.namelist()
            for fname in namelist:
                if fname == f[1] and '66wg' in f[1]:
                    print('unzip ', f[1])
                    unzipped = zf.open(f[1])
                    data = pd.read_csv(unzipped, sep=';', header=0, decimal=',')
                    stacked_df = pd.concat([stacked_df, data])

                if fname == f[1] and '55wx' in f[1]:
                    print('unzip ', f[1])
                    unzipped = zf.open(f[1])
                    kohle_df = pd.read_csv(unzipped, encoding='ISO-8859-1', sep=';', header=0, decimal=',')

        print("OK, read %d Megabytes " % (np.asarray(stacked_df).nbytes / 1000000))
        print("OK, read %d Megabytes " % (np.asarray(kohle_df).nbytes / 1000000)) if len(kohle_df) != 0 else print('kohle_df empty')
        print(stacked_df.keys())
        # exception KohleLeipzig_55wx
        if len(kohle_df) != 0:
            print(kohle_df.keys())
            kohle_df_revised = pd.DataFrame({
                'teg_id' : kohle_df['teg_id'],
                'gwkz' : kohle_df['gwkz'],
                'name' : kohle_df['gewaesser'] + ' - ' + kohle_df['name'],
                'feg_id' : None,
                'monat': kohle_df['monat'],
                'jahr' : kohle_df['jahr'],
                'eg_id' : None,
                'drain' : kohle_df['drain'],
                'er' : kohle_df['er'],
                'rg2' : kohle_df['rg2'] ,
                'mkr' : kohle_df['mkr'],
                'pi' : kohle_df['pi'],
                'rg1': kohle_df['rg1'],
                'rh' : kohle_df['rh'],
                'ro' : kohle_df['ro'],
                'sicker' : kohle_df['sicker'],
                'tkr' : kohle_df['tkr']
            })

            stacked_df = pd.concat([stacked_df, kohle_df_revised])
        # overall dataframe
        print("OK, read %d Megabytes " % (np.asarray(stacked_df).nbytes / 1000000))

        return stacked_df

    ## @brief Save parameters as hdf5 by scenario
    #
    # @param df, pandas DataFrame
    def transform_as_hdf_by_scenario(self, dict_df):
        for scenario in dict_df:
            print('save scenario', scenario)
            # get scenario id
            self.args['scenario_id'] = str(self.conf['scenarios'][scenario]['id'])
            # save data
            self.transform_as_hdf(dict_df[scenario])


    ## @brief Save parameters as hdf5
    #
    # @param df, pandas DataFrame
    def transform_as_hdf(self, df):
        print('--> transform data to hdf5')
        # loop over each parameter
        for p in self.conf['parameters']:
            if self.conf['parameters'][p]['to_hdf'] == True:
                print('parameter --> ', p)
                start_year = df['jahr'].min()
                last_year = df['jahr'].max()
                print('start year', start_year)
                print('last year', last_year)
                for y in range(start_year,last_year+1):
                    print(y)
                    # get data
                    df_param = df[df['jahr'] == y]
                    df_param = df_param[['teg_id','jahr','monat', p]]
                    df_pivot = df_param.pivot(index='teg_id', columns='monat', values=p)
                    df_pivot = df_pivot.reset_index()
                    # reorganize data with monthes as columns
                    df_year = pd.DataFrame(columns=['index','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])
                    df_year['index'] = df_pivot['teg_id']
                    df_year['jan'] = df_pivot[1] if 1 in df_pivot else -9999
                    df_year['feb'] = df_pivot[2] if 2 in df_pivot else -9999
                    df_year['mar'] = df_pivot[3] if 3 in df_pivot else -9999
                    df_year['apr'] = df_pivot[4] if 4 in df_pivot else -9999
                    df_year['may'] = df_pivot[5] if 5 in df_pivot else -9999
                    df_year['jun'] = df_pivot[6] if 6 in df_pivot else -9999
                    df_year['jul'] = df_pivot[7] if 7 in df_pivot else -9999
                    df_year['aug'] = df_pivot[8] if 8 in df_pivot else -9999
                    df_year['sep'] = df_pivot[9] if 9 in df_pivot else -9999
                    df_year['oct'] = df_pivot[10] if 10 in df_pivot else -9999
                    df_year['nov'] = df_pivot[11] if 11 in df_pivot else -9999
                    df_year['dec'] = df_pivot[12] if 12 in df_pivot else -9999
                    # reorganize data to final structure: index, values_block_0[]
                    df_final = pd.DataFrame(columns=['index','values_block_0'])
                    df_final['index'] = df_year['index']
                    df_final['values_block_0'] = df_year[df_year.columns[1:]].apply(
                        lambda x: ' '.join(x.astype(str)),
                        axis=1
                    )
                    df_final = df_final.set_index('index')
                    #print(df_final)
                    print(df_final.dtypes)
                    # save as hdf5
                    path = self.args['dst_folder'] + '/' + str(self.args['scenario_id']) + '/' + str(self.conf['parameters'][p]['id']) + '/'
                    if not os.path.exists(path):
                        os.makedirs(path)
                    fn = str(self.args['scenario_id']) + '_' + str(self.conf['parameters'][p]['id']) + '_' + str(y) + '.h5'
                    print(path + fn)
                    try:
                        df_final[['values_block_0']].to_hdf(path + fn, key='table' , mode='w', format='table')
                    except:
                        sys.exit("ERROR: " + str(sys.exc_info()))

    ## @brief create derived parameters and store them into a pandas dataframe
    #
    # @param args, dict: dictionary of arguments
    # @returns stacked_df: pandas dataframe
    def create_derived_parameters(self, args):
        print('--> Create derived parameters')
        df = {}
        # loop over derived parameters to create
        for p in self.conf['derived_parameters']:
            if p == 'q_gesamt' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['q_gesamt'] = self.derive_q_gesamt(args)
                
            if p == 'sicker' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['sicker'] = self.derive_sicker(args)

            if p == 'pi_summer' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['pi_summer'] = self.derive_pi_summer(args)

            if p == 'pi_winter' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['pi_winter'] = self.derive_pi_winter(args)
                
            if p == 'gwn' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['gwn'] = self.derive_gwn(args)
            if p == 'rd' and self.conf['derived_parameters'][p]['to_hdf'] == True:
                print('+++')
                df['rd'] = self.derive_rd(args)
            

        if len(df) == 0:
            sys.exit('ERROR: dataframe creation failed')

        return df
    
    ## @brief Derive q_gesamt
    #
    # sum of parameters 12, 13, 25, 26, 27, 28, 29
    def derive_q_gesamt(self, args):
        print('--> Derive q_gesamt')
        ds = {}
        # loop over files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/10/'
        print('--> src', src)
        for r, d, f in os.walk(src):
            for file in f:
                df = pd.DataFrame()
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters for one year# read the h5 file
                df_12 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/12/' + str(args['scenario_id']) + '_12_' + str(year) + '.h5', key='table').reset_index()
                df_13 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/13/' + str(args['scenario_id']) + '_13_' + str(year) + '.h5', key='table').reset_index()
                df_25 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/25/' + str(args['scenario_id']) + '_25_' + str(year) + '.h5', key='table').reset_index()
                df_26 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/26/' + str(args['scenario_id']) + '_26_' + str(year) + '.h5', key='table').reset_index()
                df_27 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/27/' + str(args['scenario_id']) + '_27_' + str(year) + '.h5', key='table').reset_index()
                df_28 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/28/' + str(args['scenario_id']) + '_28_' + str(year) + '.h5', key='table').reset_index()
                df_29 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/29/' + str(args['scenario_id']) + '_29_' + str(year) + '.h5', key='table').reset_index()
                # set integer type for column, set nan values
                df_12['index'] = df_12['index'].astype('float').replace(-9999, np.nan)
                df_13['index'] = df_13['index'].astype('float').replace(-9999, np.nan)
                df_25['index'] = df_25['index'].astype('float').replace(-9999, np.nan)
                df_26['index'] = df_26['index'].astype('float').replace(-9999, np.nan)
                df_27['index'] = df_27['index'].astype('float').replace(-9999, np.nan)
                df_28['index'] = df_28['index'].astype('float').replace(-9999, np.nan)
                df_29['index'] = df_29['index'].astype('float').replace(-9999, np.nan)
                # get a column for each month
                df_12[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_12.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_13[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_13.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_25[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_25.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_26[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_26.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_27[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_27.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_28[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_28.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_29[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_29.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                # remove column values_block_0
                df_12 = df_12.drop(columns=['values_block_0'])
                df_13 = df_13.drop(columns=['values_block_0'])
                df_25 = df_25.drop(columns=['values_block_0'])
                df_26 = df_26.drop(columns=['values_block_0'])
                df_27 = df_27.drop(columns=['values_block_0'])
                df_28 = df_28.drop(columns=['values_block_0'])
                df_29 = df_29.drop(columns=['values_block_0'])
                # create sum of flows
                decimals = self.conf['derived_parameters']['q_gesamt']['decimals']
                df['index'] = df_12['index']
                df['jan'] = (df_12['jan'] + df_13['jan'] + df_25['jan'] + df_26['jan'] + df_27['jan'] + df_28['jan'] + df_29['jan']).round(decimals)
                df['feb'] = (df_12['feb'] + df_13['feb'] + df_25['feb'] + df_26['feb'] + df_27['feb'] + df_28['feb'] + df_29['feb']).round(decimals)
                df['mar'] = (df_12['mar'] + df_13['mar'] + df_25['mar'] + df_26['mar'] + df_27['mar'] + df_28['mar'] + df_29['mar']).round(decimals)
                df['apr'] = (df_12['apr'] + df_13['apr'] + df_25['apr'] + df_26['apr'] + df_27['apr'] + df_28['apr'] + df_29['apr']).round(decimals)
                df['may'] = (df_12['may'] + df_13['may'] + df_25['may'] + df_26['may'] + df_27['may'] + df_28['may'] + df_29['may']).round(decimals)
                df['jun'] = (df_12['jun'] + df_13['jun'] + df_25['jun'] + df_26['jun'] + df_27['jun'] + df_28['jun'] + df_29['jun']).round(decimals)
                df['jul'] = (df_12['jul'] + df_13['jul'] + df_25['jul'] + df_26['jul'] + df_27['jul'] + df_28['jul'] + df_29['jul']).round(decimals)
                df['aug'] = (df_12['aug'] + df_13['aug'] + df_25['aug'] + df_26['aug'] + df_27['aug'] + df_28['aug'] + df_29['aug']).round(decimals)
                df['sep'] = (df_12['sep'] + df_13['sep'] + df_25['sep'] + df_26['sep'] + df_27['sep'] + df_28['sep'] + df_29['sep']).round(decimals)
                df['oct'] = (df_12['oct'] + df_13['oct'] + df_25['oct'] + df_26['oct'] + df_27['oct'] + df_28['oct'] + df_29['oct']).round(decimals)
                df['nov'] = (df_12['nov'] + df_13['nov'] + df_25['nov'] + df_26['nov'] + df_27['nov'] + df_28['nov'] + df_29['nov']).round(decimals)
                df['dec'] = (df_12['dec'] + df_13['dec'] + df_25['dec'] + df_26['dec'] + df_27['dec'] + df_28['dec'] + df_29['dec']).round(decimals)
                # assign dataframe to dictionary
                ds[str(year)] = df

        return ds
    
    ## @brief Derive q_gesamt
    #
    # sum of parameters 12, 13, 25, 26, 27, 28, 29
    def derive_rd(self, args):
        print('--> Derive rd')
        ds = {}
        # loop over files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/10/'
        print('--> src', src)
        for r, d, f in os.walk(src):
            for file in f:
                df = pd.DataFrame()
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters for one year# read the h5 file
                df_25 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/25/' + str(args['scenario_id']) + '_25_' + str(year) + '.h5', key='table').reset_index()
                df_26 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/26/' + str(args['scenario_id']) + '_26_' + str(year) + '.h5', key='table').reset_index()
                df_27 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/27/' + str(args['scenario_id']) + '_27_' + str(year) + '.h5', key='table').reset_index()
                df_28 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/28/' + str(args['scenario_id']) + '_28_' + str(year) + '.h5', key='table').reset_index()
                df_29 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/29/' + str(args['scenario_id']) + '_29_' + str(year) + '.h5', key='table').reset_index()
                # set integer type for column, set nan values
                df_25['index'] = df_25['index'].astype('float').replace(-9999, np.nan)
                df_26['index'] = df_26['index'].astype('float').replace(-9999, np.nan)
                df_27['index'] = df_27['index'].astype('float').replace(-9999, np.nan)
                df_28['index'] = df_28['index'].astype('float').replace(-9999, np.nan)
                df_29['index'] = df_29['index'].astype('float').replace(-9999, np.nan)
                # get a column for each month
                df_25[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_25.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_26[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_26.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_27[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_27.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_28[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_28.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_29[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_29.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                # remove column values_block_0
                df_25 = df_25.drop(columns=['values_block_0'])
                df_26 = df_26.drop(columns=['values_block_0'])
                df_27 = df_27.drop(columns=['values_block_0'])
                df_28 = df_28.drop(columns=['values_block_0'])
                df_29 = df_29.drop(columns=['values_block_0'])
                # create sum of flows
                decimals = self.conf['derived_parameters']['q_gesamt']['decimals']
                df['index'] = df_25['index']
                df['jan'] = (df_25['jan'] + df_26['jan'] + df_27['jan'] + df_28['jan'] + df_29['jan']).round(decimals)
                df['feb'] = (df_25['feb'] + df_26['feb'] + df_27['feb'] + df_28['feb'] + df_29['feb']).round(decimals)
                df['mar'] = (df_25['mar'] + df_26['mar'] + df_27['mar'] + df_28['mar'] + df_29['mar']).round(decimals)
                df['apr'] = (df_25['apr'] + df_26['apr'] + df_27['apr'] + df_28['apr'] + df_29['apr']).round(decimals)
                df['may'] = (df_25['may'] + df_26['may'] + df_27['may'] + df_28['may'] + df_29['may']).round(decimals)
                df['jun'] = (df_25['jun'] + df_26['jun'] + df_27['jun'] + df_28['jun'] + df_29['jun']).round(decimals)
                df['jul'] = (df_25['jul'] + df_26['jul'] + df_27['jul'] + df_28['jul'] + df_29['jul']).round(decimals)
                df['aug'] = (df_25['aug'] + df_26['aug'] + df_27['aug'] + df_28['aug'] + df_29['aug']).round(decimals)
                df['sep'] = (df_25['sep'] + df_26['sep'] + df_27['sep'] + df_28['sep'] + df_29['sep']).round(decimals)
                df['oct'] = (df_25['oct'] + df_26['oct'] + df_27['oct'] + df_28['oct'] + df_29['oct']).round(decimals)
                df['nov'] = (df_25['nov'] + df_26['nov'] + df_27['nov'] + df_28['nov'] + df_29['nov']).round(decimals)
                df['dec'] = (df_25['dec'] + df_26['dec'] + df_27['dec'] + df_28['dec'] + df_29['dec']).round(decimals)
                # assign dataframe to dictionary
                ds[str(year)] = df

        return ds
    
    
    ## @brief Derive q_gesamt
    #
    # sum of parameters 12, 13, 25, 26, 27, 28, 29
    def derive_sicker(self, args):
        print('--> Derive sicker')
        ds = {}
        # loop over files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/10/'
        print('--> src', src)
        for r, d, f in os.walk(src):
            for file in f:
                df = pd.DataFrame()
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters for one year# read the h5 file
                df_12 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/12/' + str(args['scenario_id']) + '_12_' + str(year) + '.h5', key='table').reset_index()
                df_13 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/13/' + str(args['scenario_id']) + '_13_' + str(year) + '.h5', key='table').reset_index()
                # set integer type for column, set nan values
                df_12['index'] = df_12['index'].astype('float').replace(-9999, np.nan)
                df_13['index'] = df_13['index'].astype('float').replace(-9999, np.nan)
                # get a column for each month
                df_12[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_12.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_13[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_13.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                # remove column values_block_0
                df_12 = df_12.drop(columns=['values_block_0'])
                df_13 = df_13.drop(columns=['values_block_0'])
                 # create sum of flows
                decimals = self.conf['derived_parameters']['q_gesamt']['decimals']
                df['index'] = df_12['index']
                df['jan'] = (df_12['jan'] + df_13['jan']).round(decimals)
                df['feb'] = (df_12['feb'] + df_13['feb']).round(decimals)
                df['mar'] = (df_12['mar'] + df_13['mar']).round(decimals)
                df['apr'] = (df_12['apr'] + df_13['apr']).round(decimals)
                df['may'] = (df_12['may'] + df_13['may']).round(decimals)
                df['jun'] = (df_12['jun'] + df_13['jun']).round(decimals)
                df['jul'] = (df_12['jul'] + df_13['jul']).round(decimals)
                df['aug'] = (df_12['aug'] + df_13['aug']).round(decimals)
                df['sep'] = (df_12['sep'] + df_13['sep']).round(decimals)
                df['oct'] = (df_12['oct'] + df_13['oct']).round(decimals)
                df['nov'] = (df_12['nov'] + df_13['nov']).round(decimals)
                df['dec'] = (df_12['dec'] + df_13['dec']).round(decimals)
                # assign dataframe to dictionary
                ds[str(year)] = df

        return ds

    ## @brief Derive pi_summer
    #
    # sum of parameters 12, 13, 25, 26, 27, 28, 29
    def derive_pi_summer(self, args):
        print('--> Derive pi_summer')
        ds = {}
        # loop over files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/10/'
        print('--> src', src)
        for r, d, f in os.walk(src):
            for file in f:
                df = pd.DataFrame()
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters for one year# read the h5 file
                df_10 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/10/' + str(args['scenario_id']) + '_10_' + str(year) + '.h5', key='table').reset_index()
                # set integer type for column, set nan values
                df_10['index'] = df_10['index'].astype('float').replace(-9999, np.nan)
                # get a column for each month
                df_10[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_10.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                # remove column values_block_0
                df_10 = df_10.drop(columns=['values_block_0'])
                # create sum of flows
                decimals = self.conf['derived_parameters']['pi_summer']['decimals']
                df['index'] = df_10['index']
                df['jan'] = 0
                df['feb'] = 0
                df['mar'] = 0
                df['apr'] = df_10['apr'].round(decimals)
                df['may'] = df_10['may'].round(decimals)
                df['jun'] = df_10['jun'].round(decimals)
                df['jul'] = df_10['jul'].round(decimals)
                df['aug'] = df_10['aug'].round(decimals)
                df['sep'] = df_10['sep'].round(decimals)
                df['oct'] = 0
                df['nov'] = 0
                df['dec'] = 0
                # assign dataframe to dictionary
                ds[str(year)] = df

        return ds


    ## @brief Derive pi_winter
    #
    # sum of parameters 12, 13, 25, 26, 27, 28, 29
    def derive_pi_winter(self, args):
        print('--> Derive pi_winter')
        ds = {}
        # loop over files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/10/'
        print('--> src', src)
        for r, d, f in os.walk(src):
            for file in f:
                df = pd.DataFrame()
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters for one year# read the h5 file
                df_10 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/10/' + str(args['scenario_id']) + '_10_' + str(year) + '.h5', key='table').reset_index()
                # set integer type for column, set nan values
                df_10['index'] = df_10['index'].astype('float').replace(-9999, np.nan)
                # get a column for each month
                df_10[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_10.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                # remove column values_block_0
                df_10 = df_10.drop(columns=['values_block_0'])
                # create sum of flows
                decimals = self.conf['derived_parameters']['pi_winter']['decimals']
                df['index'] = df_10['index']
                df['jan'] = df_10['jan'].round(decimals)
                df['feb'] = df_10['feb'].round(decimals)
                df['mar'] = df_10['mar'].round(decimals)
                df['apr'] = 0
                df['may'] = 0
                df['jun'] = 0
                df['jul'] = 0
                df['aug'] = 0
                df['sep'] = 0
                df['oct'] = df_10['oct'].round(decimals)
                df['nov'] = df_10['nov'].round(decimals)
                df['dec'] = df_10['dec'].round(decimals)
                # assign dataframe to dictionary
                ds[str(year)] = df

        return ds
    
    ## @brief derive groundwater recharge (gwn)
    def derive_gwn(self, args):
        print('--> derive groundwater recharge')
        ds = {}
        # loop over rg2 files
        src = args['src_folder'] + '/' + str(args['scenario_id']) + '/13/'
        for r, d, f in os.walk(src):
            for file in f:
                # get year from fname
                year = file.split('.')[0].split('_')[-1]
                print(year)
                # read parameters 13 for one year# read the h5 file
                df_rg2 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/13/' + str(args['scenario_id']) + '_13_' + str(year) + '.h5', key='table').reset_index()
                df_rg2['index'] = df_rg2['index'].replace(-9999, np.nan)
                df_rg2[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_rg2.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_rg2 = df_rg2.drop(columns=['values_block_0'])
                #print(df_rg2)
                # read parameters 12 for one year# read the h5 file
                df_rg1 = pd.read_hdf(args['src_folder'] + '/' + str(args['scenario_id']) + '/12/' + str(args['scenario_id']) + '_12_' + str(year) + '.h5', key='table').reset_index()
                df_rg1['index'] = df_rg1['index'].replace(-9999, np.nan)
                df_rg1[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']] = df_rg1.values_block_0.str.split(expand=True,).astype(float).replace(-9999, np.nan)
                df_rg1 = df_rg1.drop(columns=['values_block_0'])
                #print(df_rg1)
                # read locker_fest
                df_kliwes_lockerfest = pd.read_csv('/mnt/visdat/Projekte/2020/GWN viewer/daten/kliwes/kliwes_locker_fest.csv', sep=';').sort_values(by=['id_org'])
                #print('--> df_kliwes_lockerfest read...OK ' + df_kliwes_lockerfest.columns)
                #print(df_difga_lockerfest)
                # summarize df12 and df13
                df_merge = pd.merge(df_rg2, df_rg1, how='left', on='index')
                #print(df_merge.columns)
                df_sum = pd.DataFrame()
                df_sum['index'] = df_merge['index']
                df_sum['jan'] = df_merge['jan_x'] + df_merge['jan_y']
                df_sum['feb'] = df_merge['feb_x'] + df_merge['feb_y']
                df_sum['mar'] = df_merge['mar_x'] + df_merge['mar_y']
                df_sum['apr'] = df_merge['apr_x'] + df_merge['apr_y']
                df_sum['may'] = df_merge['may_x'] + df_merge['may_y']
                df_sum['jun'] = df_merge['jun_x'] + df_merge['jun_y']
                df_sum['jul'] = df_merge['jul_x'] + df_merge['jul_y']
                df_sum['aug'] = df_merge['aug_x'] + df_merge['aug_y']
                df_sum['sep'] = df_merge['sep_x'] + df_merge['sep_y']
                df_sum['oct'] = df_merge['oct_x'] + df_merge['oct_y']
                df_sum['nov'] = df_merge['nov_x'] + df_merge['nov_y']
                df_sum['dec'] = df_merge['dec_x'] + df_merge['dec_y']
                #print('------------------------------')
                #print(df_rg2.loc[df_rg2['index']==20277])
                #print(df_rg1.loc[df_rg1['index']==20277])
                # merge df_sum with locker_fest
                df_locker = pd.merge(df_kliwes_lockerfest, df_sum, how='inner', left_on='id_org', right_on='index')
                df_locker = df_locker.loc[df_locker['locker_fest']==0]
                #print(df_locker.loc[df_locker['idarea_data']==289])
                #print(df_locker)
                # merge df_rg2 with locker_fest
                df_fest = pd.merge(df_kliwes_lockerfest, df_rg2, how='inner', left_on='id_org', right_on='index')
                df_fest = df_fest.loc[df_fest['locker_fest']==1]
                #print(df_fest.loc[df_fest['idarea_data']==289])
                #print(df_fest)
                #print('df_locker ' + str(df_locker.shape) + str(df_locker.columns))
                #print('df_fest ' + str(df_fest.shape) + str(df_fest.columns))
                
                # stack datasets
                df_gwn = pd.concat([df_locker, df_fest]).drop(columns=['idarea', 'idarea_data', 'id_org', 'locker_fest'])
                #print('df_gwn' + str(df_gwn.shape) + str(df_gwn.columns)) 
                #print(df_gwn)
                ds[str(year)] = df_gwn
                #break
        
        #sys.exit()
        return ds

    ## @brief Save derived parameters
    def save_derived_parameters(self, args, df):
        print('--> Save derived parameters')
        for p in df:
            print('===>>', p)
            if df[p] != None:
                for y in df[p]:
                    #print(y)
                    df_year = df[p][str(y)]
                    # reorganize data to final structure: index, values_block_0[]
                    df_final = pd.DataFrame(columns=['index','values_block_0'])
                    df_final['index'] = df_year['index']
                    df_final['values_block_0'] = df_year[df_year.columns[1:]].apply(
                        lambda x: ' '.join(x.astype(str)),
                        axis=1
                    )
                    df_final = df_final.set_index('index')
                    #print(df_final)
                    #print(df_final.dtypes)
                    # save as hdf5
                    path = self.args['dst_folder'] + str(self.args['scenario_id']) + '/' + str(self.conf['derived_parameters'][p]['id']) + '/'
                    if not os.path.exists(path):
                        os.makedirs(path)
                    fn = str(self.args['scenario_id']) + '_' + str(self.conf['derived_parameters'][p]['id']) + '_' + str(y) + '.h5'
                    #print(path + fn)
                    #sys.exit()
                    try:
                        df_final[['values_block_0']].to_hdf(path + fn, key='table' , mode='w', format='table')
                    except:
                        sys.exit("ERROR: " + str(sys.exc_info()))
