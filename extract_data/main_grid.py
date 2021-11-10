import sys, os
import simplejson
import time
sys.path.append('/mnt/visdat/Projekte/2020/GWN viewer/dev/python/extract_data')
from extract_data_grid import extract_data_grid
print(os.path.exists('/mnt/visdat/Projekte/2020//GWN viewer/dev/python/extract_data/'))


#python3 /mnt/visdat/Projekte/2020/GWN\ viewer/dev/python/extract_data/main_grid.py


defs = {}
defs['param'] = {}
defs['param']['time'] = {}
defs['project'] = {}
defs['param']['id_param'] = 10
defs['param']['id_scenario'] = 10
defs['param']['id_area'] = 3
defs['param']['id_areadata'] = [4]
#defs['param']['time_period'] = 'year'
#defs['param']['group_area'] = 1
defs['param']['time']['start_year'] = 1961
#defs['param']['time']['start_month'] = 0
defs['param']['time']['end_year'] = 1987
#defs['param']['time']['end_month'] = 0
defs['project']['proj_dir'] = "/mnt/galfdaten/daten_stb/gwn_sachsen/"
defs['project']['diff'] =   ''
defs['project']['idtab'] =  373


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

# extract kliwes dataset
parameter, area = p.extract_parameter(args)

# get data for mapping by mapserver live
#result = p.get_map_data(args, parameter, area)

print('--> args : ' + str(args))


#args['exists'], args['param_export_fpath'] = p.check_parameter_export_file(args)
#p.create_map_data(args)


# todo: get base statistics
#result = p.get_base_statistic(args, parameter, area)

# todo : get histogram:
result = p.get_histogram(args, parameter, area)

# get data for mapping by mapserver
#result = p.get_area_values(args, parameter, area)

#print(result)
