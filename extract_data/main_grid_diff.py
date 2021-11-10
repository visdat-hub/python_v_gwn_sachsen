import sys, os
import time
import numpy as np
sys.path.append('/mnt/visdat/Projekte/2020/GWN viewer/dev/python/extract_data')
from extract_data_grid import extract_data_grid
print(os.path.exists('/mnt/visdat/Projekte/2020//GWN viewer/dev/python/extract_data/'))

# usage
# python3 /mnt/visdat/Projekte/2020/GWN\ viewer/dev/python/extract_data/main_grid_diff.py

# container for variable definitions
defs = {}
# main parameter
defs['param'] = {}
defs['param']['time'] = {}
defs['param']['id_param'] = 10
defs['param']['id_scenario'] = 10
defs['param']['id_area'] = 1
defs['param']['id_areadata'] = [0]
#defs['param']['time_period'] = 'year'
#defs['param']['group_area'] = 1
defs['param']['time']['start_year'] = 1961
#defs['param']['time']['start_month'] = 0
defs['param']['time']['end_year'] = 1987
#defs['param']['time']['end_month'] = 0

# diff parameter
defs['param_diff'] = {}
defs['param_diff']['time'] = {}
defs['param_diff']['id_param'] = 10
defs['param_diff']['id_scenario'] = 10
#defs['param_diff']['time_period'] = 'year'
#defs['param_diff']['group_area'] = 1
defs['param_diff']['time']['start_year'] = 1961
#defs['param_diff']['time']['start_month'] = 0
defs['param_diff']['time']['end_year'] = 1987
#defs['param_diff']['time']['end_month'] = 0

# project specific definitions
defs['project'] = {}
defs['project']['proj_dir'] = "/mnt/galfdaten/daten_stb/gwn_sachsen/"
defs['project']['diff'] = 1
defs['project']['idtab'] =  406

# start program
t0 = time.time()
print("--> starting parameter extraction...")
p = extract_data_grid()

# get command line arguments
args = p.get_arguments(defs, 'test')
print('--> args : ' + str(args))
print('--> args_proj_dir : ' + str(args['proj_dir']))

# check area file
args['area_fpath'] = p.check_area_file(args)

# check parameter file
args['parameter_fpath'] = p.check_parameter_file(args)

# check diff parameter file
args['diff_parameter_fpath'] = p.check_diff_parameter_file(args)

# extract kliwes dataset
main_parameter, area = p.extract_parameter(args)

diff_parameter, diff_area = p.extract_diff_parameter(args)
print('--> diff_parameter : ', diff_parameter.mean())
print('--> main_parameter : ', main_parameter.mean())

#sys.exit()

area = np.where(area == -99999, -99999, area)
area = np.where(area == -9999, -99999, area)
area = np.where(area == -999, -99999, area)

parameter = np.subtract(main_parameter, diff_parameter)
parameter = np.where(main_parameter == -99999, -99999, parameter)
parameter = np.where(main_parameter == -9999, -99999, parameter)
parameter = np.where(main_parameter == -999, -99999, parameter)

parameter = np.where(diff_parameter == -99999, -99999, parameter)
parameter = np.where(diff_parameter == -9999, -99999, parameter)
parameter = np.where(diff_parameter == -999, -99999, parameter)
#print('--> parameter : ', parameter)

# get data for mapping by mapserver live
#result = p.get_map_data(args, parameter, area)

print('--> args : ' + str(args))


#args['exists'], args['param_export_fpath'] = p.check_parameter_export_file(args)
#p.create_map_data(args)


# todo: get base statistics
#result = p.get_base_statistic(args, parameter, area)

# todo : get histogram:
result = p.get_diff_histogram(args, parameter, area)

# get data for mapping by mapserver
#result = p.get_area_values(args, parameter, area)

#print(result)
