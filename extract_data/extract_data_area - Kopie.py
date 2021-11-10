## @package extract_parameter
#
# Extract values by arguments and calculate statistics:
#
# id_param, id_scenario, id_area, id_areadata[], time_period, start_year, start_month, end_year, end_month, proj_dir, stat_type
#
# Calculate statistics for data selection:
# - area weighted / arithmetic mean
# - area weighted / arithmetic median
# - area weighted / arithmetic frequency distribution [percent]
# - minimum, maximum
#
# Extract single values for data selection:
#
# - arithmetic mean for each id_areadata for the total time period as map input data
# - area weighted mean for each time step (month/year or year) for the total area selection as time period diagram input data
#
# Frequency distribution:
#
# - frequency of a parameter for selected areas and time steps of selected time period
# - frequency take into account the size of areas and is indicated in percent
# - frequency is calculated over time or over areas
# - the corresponding map shows an arithmetic mean over time period for each polygon
#
# Usage: python3  ./extract_parameter/extract_parameter.py -stat_type base_statistic -stat_spec w_mean -id_param 1 -id_scenario 0
# -id_area 10 -id_areadata [1,2,3] -time_period year -start_year 2001 -start_month 0 -end_year 2006 -end_month 0
# -proj_dir /var/www/daten_stb/wasserhaushaltsportal/
#
# - start_month / end_month: values from 1 to 12, 0 if time_period is a year
# - time_period: year or month
# - stat_type: single_values_area, single_values_time, base_statistic, frequency_distribution
# - stat_spec: w_mean, min, max (for single values of time period or areas)
# - id_areadata: comma separated list of identifiers [1,2,3] or an empty array [] to select all shapes of an area

import os
import sys
import ast
import time
import h5py

import numpy as np
import pandas as pd
import zipfile
sys.path.append('/mnt/visdat/Projekte/2020/GWN viewer/dev/python/extract_data/pg')
from pg import db_connector as pg

class extract_data_area():

	def __init__(self):

		# define database access
		self.conf = {'dbconfig' :
			{
				"db_host" : "192.168.0.194",
				"db_name" : "gwn_sachsen",
				"db_user" : "stb_user",
				"db_password" : "4e35fddae14432a8ed8dd6dd5b830e09",
				"db_port" : "9991"
			}
		}
        
		#
		self.logfile = open("/mnt/galfdaten/daten_stb/gwn_sachsen/download.log", "a")

	## @brief Get command line arguments.
	#
	# @returns args Array of arguments
	def get_arguments(self, defs, modul):

		print("--> get caller arguments")

		args = {}
		error = 0

		t = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))

		try:
			args['id'] = defs['project']['id']
			id = str(args['id'])
		except:
			error = 1
			self.logfile.write(t + '\t' + '' + '\t' + modul + '\t' + ' defs[project][id] doesn\'t exists \n')

		try:
			args['proj_dir'] = defs['project']['proj_dir']
			args['python_dir'] = defs['project']['python_dir']

			# check project path
			if self.check_path_exists(args['proj_dir']) == False:
				error = 2
				self.logfile.write(t + '\t' + id + '\t' + modul + '\t\'' + args['proj_dir'] + '\'Project path doesn\'t exists \n')
		except:
			error = 1
			self.logfile.write(id + ' ' + t + ' defs[project][proj_dir] doesn\'t exists  \n')


		try:
		    args['id_param'] =  defs['param']['id_param']
		except:
		   error = 1
		   self.logfile.write(t + '\t' + id + '\t' + modul + '\t' + ' defs[param][id_param] doesn\'t exists \n')

		try:
		    args['id_scenario'] =  defs['param']['id_scenario']
		except:
		   error = 1
		   self.logfile.write(t + '\t' + id + '\t' + modul + '\t' + ' defs[param][id_scenario] doesn\'t exists \n')

		try:
		    args['id_area'] =  defs['param']['id_area']
		except:
		   error = 1
		   self.logfile.write(t + '\t' + id + '\t' + modul + '\t' + ' defs[param][id_area] doesn\'t exists \n')

		try:
		    args['id_areadata'] =  defs['param']['id_areadata']
		except:
		   error = 1
		   self.logfile.write(t + '\t' + id + '\t' + modul + '\t' + ' defs[param][id_areadata]doesn\'t exists \n')

		try:
		    args['start_year'] =  defs['param']['time']['start_year']
		except:
		   error = 1
		   self.logfile.write(t + '\t' + id + '\t' + modul + '\t' + ' defs[param][time][start_year] doesn\'t exists \n')

		try:
		    args['end_year'] =  defs['param']['time']['end_year']
		except:
		   error = 1
		   self.logfile.write(t + '\t' + id + '\t' + modul + '\t' + ' defs[param][time][end_year] doesn\'t exists \n')

		try:
			args['time_period'] =  defs['param']['time_period']
		except:
			args['time_period'] = "year"

		try:
		    args['start_month'] =  defs['param']['time']['start_month']
		except:
		    args['start_month'] = 0

		try:
		    args['end_month'] =  defs['param']['time']['end_month']
		except:
		    args['end_month'] = 0

		try:
		    args['diff_start_month'] =  defs['diffparam']['time']['start_month']
		except:
		    args['diff_start_month'] = 0

		try:
		    args['diff_end_month'] =  defs['diffparam']['time']['end_month']
		except:
		    args['diff_end_month'] = 0


		try:
		    args['diff'] = defs['project']['diff']
		except:
		    args['diff'] = 0

		try:
		    args['id_tab'] = defs['project']['idtab']
		except:
		    args['id_tab'] = 0


		if  args['diff'] == 1:

			try:
			    args['diff_id_scenario'] =  defs['diffparam']['id_scenario']
			except:
			   error = 2
			   self.logfile.write(t + '\t' + id + '\t' + modul + '\t' + ' defs[diffparam][id_scenario] doesn\'t exists \n')

			try:
			    args['diff_start_year'] =  defs['diffparam']['time']['start_year']
			except:
			   error = 2
			   self.logfile.write(t + '\t' + id + '\t' + modul + '\t' + ' defs[diffparam][time][start_year] doesn\'t exists \n')

			try:
			    args['diff_end_year'] =  defs['diffparam']['time']['end_year']
			except:
			   error = 2
			   self.logfile.write(t + '\t' + id + '\t' + modul + '\t' + ' defs[diffparam][time][end_year] doesn\'t exists \n')


		args['error'] = error
		args['modul'] = modul
		args['stat_spec'] = 'w_mean'

		# select files
		if error != 2:
			args  = self.select_files(args)
			if args['diff'] == 1:
				args  = self.select_diff_files(args)

		return args

	## @brief Check if a folder/file exists
    #
    # @param path The name of folder or filename
	def check_path_exists(self, path):

		print('--> check path : ' + str(path))

		if os.path.isdir(path):
			bool = True
			#text = '--> ok, folder exists'
		elif os.path.isfile(path):
			bool = True
			#text = '--> ok, folder exists'
		else:
			bool = False
			#text = 'ERROR: Path doesn\'t exists'

		return bool

    ## @brief Select data files defined by arguments
    #
    # @param args command line arguments
    # @return fileList List of files to process
	def select_files(self, args):

		#print('--> selecting files')
		fileList = []
		error = args['error']

		t = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))

		# set source path for time_period : year
		if args['time_period'] == 'year':
			srcPath = args['proj_dir'] + '/parameters/' + str(args['id_scenario']) + '/' + str(args['id_param']) + '/total/' + str(args['id_param']) + '_' + str(args['id_scenario']) + '.stats.h5'
			if self.check_path_exists(srcPath) == False:
				error = 1
				self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t\'' + srcPath + '\' file doesn\'t exists \n')
			else:
				fileList.append(srcPath)
		# set source path for time_period : month
		elif args['time_period'] == 'month':
			srcPath = args['proj_dir'] + '/parameters/' + str(args['id_scenario']) + '/' + str(args['id_param']) + '/month/'
			#print('--> srcPath : ' + str(srcPath))

			if self.check_path_exists(srcPath) == False:
				error = 1
				self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t\'' +srcPath + '\' path doesn\'t exists \n')

			fname = str(args['id_param']) + '_' + str(args['id_scenario']) + '.stats.h5'
			# print('--> fname : ' + str(fname))
			for y in range(int(args['start_year']), int(args['end_year']) + 1):
				src = srcPath + '/' + str(y) + '/' + fname
				if self.check_path_exists(srcPath) == False:
					error = 1
					self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t\'' +src + '\' file doesn\'t exists \n')
				# print('--> src : ' + str(src))
				fileList.append([y, src])

		# exception handling
		else:
			 error = 1
			 self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t\'' + args['time_period'] + '\' time_period argument is not valid \n')

		# check length of filelist
		if len(fileList) == 0:
			print('ERROR: No files found.')
			error = 1
			self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t' + 'No files found \n')
		else:
			# print('--> ', len(fileList), ' file(s) found.')
			# print('--> fileList : ' + str(fileList))

			if args['time_period'] == 'year':

				for f in fileList:
					fp = h5py.File(f, 'r') if args['time_period'] == 'year' else h5py.File(f[1], 'r')

					if 'areas' in fp:
						fp_areas = fp['areas']
						if str(args['id_area']) in fp_areas:
							fp_areas_id = fp_areas[str(args['id_area'])]
							if 'table' in fp_areas_id:
								gg = 0
							else:
								error = 1
								self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t' + f + '\tOrder \'areas/' + str(args['id_area']) + '/table\' doesn\'t exists \n')
						else:
							error = 1
							self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t' + f + '\tOrder \'areas/' + str(args['id_area']) + '\' doesn\'t exists \n')

					else:
						error = 1
						self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t' + f + '\tOrder \'areas\' doesn\'t exists \n')

		args['files'] = fileList
		args['error'] = error
		return args

    ## @brief Select diff data files defined by arguments
    #
    # @param args command line arguments
    # @return fileList List of files to process
	def select_diff_files(self, args):

		#print('--> selecting files')
		fileList = []
		error = args['error']

		t = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))

		# set source path for time_period : year
		if args['time_period'] == 'year':
			srcPath = args['proj_dir'] + '/parameters/' + str(args['diff_id_scenario']) + '/' + str(args['id_param']) + '/total/' + str(args['id_param']) + '_' + str(args['diff_id_scenario']) + '.stats.h5'
			if self.check_path_exists(srcPath) == False:
				error = 1
				self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t\'' + srcPath + '\' file doesn\'t exists \n')
			else:
				fileList.append(srcPath)
		# set source path for time_period : month
		elif args['time_period'] == 'month':
			srcPath = args['proj_dir'] + '/parameters/' + str(args['diff_id_scenario']) + '/' + str(args['id_param']) + '/month/'
			#print('--> srcPath : ' + str(srcPath))

			if self.check_path_exists(srcPath) == False:
				error = 1
				self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t\'' +srcPath + '\' path doesn\'t exists \n')

			fname = str(args['id_param']) + '_' + str(args['diff_id_scenario']) + '.stats.h5'
			# print('--> fname : ' + str(fname))
			for y in range(int(args['diff_start_year']), int(args['diff_end_year']) + 1):
				src = srcPath + '/' + str(y) + '/' + fname
				if self.check_path_exists(srcPath) == False:
					error = 1
					self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t\'' +src + '\' file doesn\'t exists \n')
				# print('--> src : ' + str(src))
				fileList.append([y, src])

		# exception handling
		else:
			 error = 1
			 self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t\'' + args['time_period'] + '\' time_period argument is not valid \n')

		# check length of filelist
		if len(fileList) == 0:
			print('ERROR: No files found.')
			error = 1
			self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t' + 'No files found \n')
		else:
			# print('--> ', len(fileList), ' file(s) found.')
			# print('--> fileList : ' + str(fileList))

			if args['time_period'] == 'year':

				for f in fileList:
					fp = h5py.File(f, 'r') if args['time_period'] == 'year' else h5py.File(f[1], 'r')

					if 'areas' in fp:
						fp_areas = fp['areas']
						if str(args['id_area']) in fp_areas:
							fp_areas_id = fp_areas[str(args['id_area'])]
							if 'table' in fp_areas_id:
								gg = 0
							else:
								error = 1
								self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t' + f + '\tOrder \'areas/' + str(args['id_area']) + '/table\' doesn\'t exists \n')
						else:
							error = 1
							self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t' + f + '\tOrder \'areas/' + str(args['id_area']) + '\' doesn\'t exists \n')

					else:
						error = 1
						self.logfile.write(t + '\t' + str(args['id']) + '\t' + args['modul'] + '\t' + f + '\tOrder \'areas\' doesn\'t exists \n')

		args['diff_files'] = fileList
		args['error'] = error
		return args

	## @brief Get datasets from files and store them into a DataFrame
	#
	# @param fileList List of h5 files
	# @param args Command Line Arguments
	# @return ds A pandas dataframe
	def get_data(self, args):

		print('--> get data from hdf5 files')
		fileList = args['files']
		print('--> fileList : ' + str(fileList))

		dataContainer = []
		file_counter = 0
		for f in fileList:
			#print('--> f :', f)
			# year or month
			fp = h5py.File(f, 'r') if args['time_period'] == 'year' else h5py.File(f[1], 'r')
			ds = fp['/areas/' + str(args['id_area']) + '/table']
			ds_colNames = ds.dtype.fields.keys()

			# time step is a year
			if args['time_period'] == 'year':
				# create column object
				colNames = ['id_area','area']
				t_cols = list(range(int(args['start_year']), int(args['end_year']) + 1))
				for year in ds_colNames:
					if str(year) in str(t_cols):
						colNames.append(year)

				df = pd.DataFrame(ds[tuple(colNames)], columns=colNames)
				fp.close()

				#print('--> colNames : ', colNames)
				#print('--> df : ', df)

			# time step is a month
			if args['time_period'] == 'month':
				# create column object
				colNames = ['year']
				for i in ds_colNames:
				    colNames.append(i)
				df_temp = pd.DataFrame(ds[:], columns=colNames)
				df_temp['year'] = f[0]

				df = pd.DataFrame()
				df['year'] = df_temp['year']
				df['id_area'] = df_temp['id_area']
				df['area'] = df_temp['area']
				df['jan'] = df_temp['jan']
				df['feb'] = df_temp['feb']
				df['mar'] = df_temp['mar']
				df['apr'] = df_temp['apr']
				df['may'] = df_temp['may']
				df['jun'] = df_temp['jun']
				df['jul'] = df_temp['jul']
				df['aug'] = df_temp['aug']
				df['sep'] = df_temp['sep']
				df['oct'] = df_temp['oct']
				df['nov'] = df_temp['nov']
				df['dec'] = df_temp['dec']

				# reduce monthes of start year and set values before start month to NaN
				if int(args['start_month']) > 1 and file_counter == 0:
					print ('--> start_month : ', args['start_month'])
					idx_start_month = int(args['start_month']) + 1
					df.iloc[:, 3:idx_start_month+1] = np.nan

				# reduce monthes of end year and set values after end month to NaN
				file_counter += 1
				if int(args['end_month']) < 12 and file_counter == len(fileList):
					print ('--> end_month : ', args['end_month'])
					idx_end_month = int(args['end_month']) + 1
					df.iloc[:, idx_end_month+2:] = np.nan

			if len(args['id_areadata']) > 0:
				df = df[df['id_area'].isin(args['id_areadata'])]

			# add area to dataContainer
			dataContainer.append(df)

		# store areas in a single pandas dataframe without slices
		df = pd.concat([pd.DataFrame(dataContainer[i]) for i in range( len(dataContainer))], ignore_index=True)
		dataContainer = None

		#print('--> df',df)
		return df

	## @brief Get second datasets for difference calculations from files and store them into a DataFrame
	#
	# @param fileList List of h5 files
	# @param args Command Line Arguments
	# @return ds A pandas dataframe
	def get_diff_data(self, args):

		print('--> get diff data from hdf5 files')
		fileList = args['diff_files']
		print('--> fileList : ' + str(fileList))

		dataContainer = []
		file_counter = 0
		for f in fileList:
			#print('--> f :', f)
			# year or month
			fp = h5py.File(f, 'r') if args['time_period'] == 'year' else h5py.File(f[1], 'r')
			ds = fp['/areas/' + str(args['id_area']) + '/table']
			ds_colNames = ds.dtype.fields.keys()

			# time step is a year
			if args['time_period'] == 'year':
				# create column object
				colNames = ['id_area','area']
				t_cols = list(range(int(args['diff_start_year']), int(args['diff_end_year']) + 1))
				for year in ds_colNames:
					if str(year) in str(t_cols):
						colNames.append(year)

				df = pd.DataFrame(ds[tuple(colNames)], columns=colNames)
				fp.close()

				#print('--> colNames : ', colNames)
				#print('--> df : ', df)

			# time step is a month
			if args['time_period'] == 'month':
				# create column object
				colNames = ['year']
				for i in ds_colNames:
				    colNames.append(i)
				df_temp = pd.DataFrame(ds[:], columns=colNames)
				df_temp['year'] = f[0]

				df = pd.DataFrame()
				df['year'] = df_temp['year']
				df['id_area'] = df_temp['id_area']
				df['area'] = df_temp['area']
				df['jan'] = df_temp['jan']
				df['feb'] = df_temp['feb']
				df['mar'] = df_temp['mar']
				df['apr'] = df_temp['apr']
				df['may'] = df_temp['may']
				df['jun'] = df_temp['jun']
				df['jul'] = df_temp['jul']
				df['aug'] = df_temp['aug']
				df['sep'] = df_temp['sep']
				df['oct'] = df_temp['oct']
				df['nov'] = df_temp['nov']
				df['dec'] = df_temp['dec']

				# reduce monthes of start year and set values before start month to NaN
				if int(args['diff_start_month']) > 1 and file_counter == 0:
					print ('--> start_month : ', args['diff_start_month'])
					idx_start_month = int(args['diff_start_month']) + 1
					df.iloc[:, 3:idx_start_month+1] = np.nan

				# reduce monthes of end year and set values after end month to NaN
				file_counter += 1
				if int(args['diff_end_month']) < 12 and file_counter == len(fileList):
					print ('--> end_month : ', args['diff_end_month'])
					idx_end_month = int(args['diff_end_month']) + 1
					df.iloc[:, idx_end_month+2:] = np.nan

			if len(args['id_areadata']) > 0:
				df = df[df['id_area'].isin(args['id_areadata'])]

			# add area to dataContainer
			dataContainer.append(df)

		# store areas in a single pandas dataframe without slices
		df = pd.concat([pd.DataFrame(dataContainer[i]) for i in range( len(dataContainer))], ignore_index=True)
		dataContainer = None

		#print('--> df',df)
		return df

	## @brief Calculate base statistics for a dataset based on group areas by time period
    #
    # stat_type: base_statistic
    #
    # - arithmetic mean
    # - area weighted mean
    # - minimum and maximum
    # - median based on the area weighted means of areas
    # - area weighted median take into account the area sizes
    # @param ds A pandas dataset
    # @param args Command line arguments as list
    # @return df_stat A list of pandas dataframes
	def get_base_statistic_year(self, ds, args):

		print('--> extract_baseStatistic')

		df_stat = {}

		df_temp = ds.iloc[:,2:].stack().reset_index()
		df_temp = df_temp.rename(columns={df_temp.columns[0]:'id',df_temp.columns[1]:'year', df_temp.columns[2]:'val'})
		df_temp = df_temp.groupby(['id'])['val'].mean()
		#print(df_temp)

		df_stat['mean'] = df_temp.mean()
		df_stat['min'] = df_temp.min()
		df_stat['max'] = df_temp.max()
		df_stat['median'] = df_temp.median()
		df_stat['std'] = df_temp.std()
		df_stat['sum'] = df_temp.sum()
		#print('--> df_stat :' + str(df_stat))

		return df_stat

    ## @brief Calculate base statistics for a dataset based on a yearly time period
    #
    # stat_type: base_statistic
    #
    # - arithmetic mean
    # - area weighted mean
    # - minimum and maximum
    # - median based on the area weighted means of areas
    # - area weighted median take into account the area sizes
    # @param ds A pandas dataset
    # @param args Command line arguments as list
    # @return df_stat A list of pandas dataframes
	def get_base_statistic_year_advanced(self, ds, args):
		print('--> extract_baseStatistic_year')
		print(ds)

		df_stat = {}
		# get time period
		t_cols = list(range(int(args['start_year']), int(args['end_year']) + 1))
		print('--> t_cols' + str(t_cols))

		cols = []
		for i in t_cols:
			cols.append(str(i))

		print('--> cols' + str(cols))

		# get total area
		sum_area = ds['area'].sum()

		#print('--> ds.iloc[:,2:].stack(): ' + str(ds.iloc[:,2:].stack()))

		# get standard statistics
		df_stat['mean'] = ds.iloc[:,2:].stack().mean()
		df_stat['min'] = ds.iloc[:,2:].stack().min()
		df_stat['max'] = ds.iloc[:,2:].stack().max()
		df_stat['median'] = ds.iloc[:,2:].stack().median()
		df_stat['std'] = ds.iloc[:,2:].stack().std()
		df_stat['sum'] = ds.iloc[:,2:].stack().sum()
		# get weighted mean
		df_stat['w_mean'] = (ds.iloc[:,2:].multiply(ds['area'], axis=0).sum()/sum_area).mean()

		#print(df_stat)

		# get weighted median
		# method is working in time if areas are selected
		'''
		ds_area = ds['area']
		ds_median = np.array([])
		for i in ds.iloc[:,2:]:
			z = 0
			for j in ds[i]:
				d = np.full((int(np.floor(ds_area[z]/10000)),), j)
				ds_median = np.concatenate ( (ds_median, d), axis=None ) # ein Wert je Hektar
				z = z + 1
		df_stat['w_median'] = np.median(ds_median)
		'''
		# weighted median for a dataset without area restrictions
		# calculate cumsum of dataset sorted by area size
		# method works for live purposes
		ds_cumsum = (ds.set_index(['id_area','area']).iloc[:,0:].stack()).sort_values().reset_index()
		ds_cumsum['cumsum'] = ds_cumsum['area'].cumsum()
		cumsum_area_50 = ds_cumsum['cumsum'].max()/2
		w_median = ds_cumsum.loc[ds_cumsum['cumsum'] == cumsum_area_50]
		if len(w_median) != 0:
			w_median = w_median.iloc[0,3]
		else:
			lower_w_median = ds_cumsum.iloc[(ds_cumsum.loc[ds_cumsum['cumsum'] < cumsum_area_50])['cumsum'].idxmax()]
			upper_w_median = ds_cumsum.iloc[(ds_cumsum.loc[ds_cumsum['cumsum'] > cumsum_area_50])['cumsum'].idxmin()]
			lower_w_median = (lower_w_median[3:4][0])
			upper_w_median = (upper_w_median[3:4][0])
			w_median = (lower_w_median + upper_w_median) / 2

		df_stat['w_median'] = w_median

		return df_stat

    ## @brief Calculate base statistics for a dataset based on a yearly time period
    #
    # stat_type: base_statistic
    #
    # - arithmetic mean
    # - area weighted mean
    # - minimum and maximum
    # - median based on the area weighted means of areas
    # - area weighted median take into account the area sizes
    # @param ds A pandas dataset
    # @param args Command line arguments as list
    # @return df_stat A list of pandas dataframes
	def get_base_statistic_month(self, ds, args):
		print('--> extract_baseStatistic_year')
		print(ds)

		df_stat = {}
		# get time period
		t_cols = list(range(int(args['start_year']), int(args['end_year']) + 1))
		print('--> t_cols')
		print(t_cols)

		cols = []
		for i in t_cols:
			cols.append(str(i))
		#print(cols)
		# get total area
		sum_area = ds['area'].sum()

		# get standard statistics
		df_stat['mean'] = ds.iloc[:,2:].stack().mean()
		df_stat['min'] = ds.iloc[:,2:].stack().min()
		df_stat['max'] = ds.iloc[:,2:].stack().max()
		df_stat['median'] = ds.iloc[:,2:].stack().median()
		df_stat['std'] = ds.iloc[:,2:].stack().std()
		df_stat['sum'] = ds.iloc[:,2:].stack().sum()

		# get weighted mean
		df_stat['w_mean'] = (ds.iloc[:,2:].multiply(ds['area'], axis=0).sum()/sum_area).mean()

		# get weighted median
		# method is working in time if areas are selected
		'''
		ds_area = ds['area']
		ds_median = np.array([])
		for i in ds.iloc[:,2:]:
			z = 0
			for j in ds[i]:
				d = np.full((int(np.floor(ds_area[z]/10000)),), j)
				ds_median = np.concatenate ( (ds_median, d), axis=None ) # ein Wert je Hektar
				z = z + 1
		df_stat['w_median'] = np.median(ds_median)
		'''
		# weighted median for a dataset without area restrictions
		# calculate cumsum of dataset sorted by area size
		# method works for live purposes
		ds_cumsum = (ds.set_index(['id_area','area']).iloc[:,0:].stack()).sort_values().reset_index()
		ds_cumsum['cumsum'] = ds_cumsum['area'].cumsum()
		cumsum_area_50 = ds_cumsum['cumsum'].max()/2
		w_median = ds_cumsum.loc[ds_cumsum['cumsum'] == cumsum_area_50]
		if len(w_median) != 0:
			w_median = w_median.iloc[0,3]
		else:
			lower_w_median = ds_cumsum.iloc[(ds_cumsum.loc[ds_cumsum['cumsum'] < cumsum_area_50])['cumsum'].idxmax()]
			upper_w_median = ds_cumsum.iloc[(ds_cumsum.loc[ds_cumsum['cumsum'] > cumsum_area_50])['cumsum'].idxmin()]
			lower_w_median = (lower_w_median[3:4][0])
			upper_w_median = (upper_w_median[3:4][0])
			w_median = (lower_w_median + upper_w_median) / 2
			df_stat['w_median'] = w_median

		return df_stat

    ## @brief Extract means over time for each area polygon
    #
    # stat_type: single_values_area
    #
    # @param ds A Pandas DataFrame
    # @param args Command Line Arguments
    # @return df_stat A list of pandas dataframe
	def get_area_values(self, ds, args):
		print('--> extract_single_values_area, groupby id_areadata')
		# get time period
		t_cols = list(range(int(args['start_year']), int(args['end_year']) + 1))
		cols = []
		for i in t_cols:
			cols.append(str(i))
		#print(cols)
		# get total area
		#sum_area = ds['area'].sum()

		# get weighted mean
		#df_stat = ds.set_index(['id_area','area']).mean(axis=1) # working
		df_stat = {}

		#if sum_area > 0:
		df_stat['mean'] = ds.set_index('id_area').groupby({x:'value' for x in tuple(cols)}, axis=1).mean().reset_index() #working
		# df_stat['mean'] = ds.set_index(['id_area']).iloc[:,1:].mean(axis=1)
		#df_stat['min'] = ds.set_index('id_area').groupby({x:'value' for x in tuple(cols)}, axis=1).min().reset_index()
		#df_stat['max'] = ds.set_index('id_area').groupby({x:'value' for x in tuple(cols)}, axis=1).max().reset_index()
		#df_stat['median'] = ds.set_index('id_area').groupby({x:'value' for x in tuple(cols)}, axis=1).median().reset_index()
		#df_stat['std'] = ds.set_index('id_area').groupby({x:'value' for x in tuple(cols)}, axis=1).std().reset_index()
		#df_stat['sum'] = ds.set_index('id_area').groupby({x:'value' for x in tuple(cols)}, axis=1).sum().reset_index()
		#print(df_stat)
		return df_stat

    ## @brief Extract means over months for each area polygon
    #
    # stat_type: single_values_area
    #
    # @param ds A Pandas DataFrame
    # @param args Command Line Arguments
    # @return df_stat A list of pandas dataframe
	def get_avg_months_per_area(self, ds, args):
		print('--> get_avg_months_per_area, groupby id_areadata')
		# get time period
		t_cols = list(range(int(args['start_year']), int(args['end_year']) + 1))
		cols = []
		for i in t_cols:
			cols.append(str(i))
		#print(cols)
		# get total area
		#sum_area = ds['area'].sum()

		# get weighted mean
		#df_stat = ds.set_index(['id_area','area']).mean(axis=1) # working
		df_stat = {}
		#ds = ds.drop(columns=['year'])
		#print(ds)
		df = ds.groupby(['id_area','area'])['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].mean().reset_index()
		df1 = pd.DataFrame({'id_area': df['id_area'], 'month' : 1, 'value' : df['jan']})
		df2 = pd.DataFrame({'id_area': df['id_area'], 'month' : 2, 'value' : df['feb']})
		df3 = pd.DataFrame({'id_area': df['id_area'], 'month' : 3, 'value' : df['mar']})
		df4 = pd.DataFrame({'id_area': df['id_area'], 'month' : 4, 'value' : df['apr']})
		df5 = pd.DataFrame({'id_area': df['id_area'], 'month' : 5, 'value' : df['may']})
		df6 = pd.DataFrame({'id_area': df['id_area'], 'month' : 6, 'value' : df['jun']})
		df7 = pd.DataFrame({'id_area': df['id_area'], 'month' : 7, 'value' : df['jul']})
		df8 = pd.DataFrame({'id_area': df['id_area'], 'month' : 8, 'value' : df['aug']})
		df9 = pd.DataFrame({'id_area': df['id_area'], 'month' : 9, 'value' : df['sep']})
		df10 = pd.DataFrame({'id_area': df['id_area'], 'month' : 10, 'value' : df['oct']})
		df11 = pd.DataFrame({'id_area': df['id_area'], 'month' : 11, 'value' : df['nov']})
		df12 = pd.DataFrame({'id_area': df['id_area'], 'month' : 12, 'value' : df['dec']})
		df_stat['mean'] = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12], ignore_index=True)
		
		#print(df_stat)
		return df_stat
    
    ## @brief Extract means over months for each area polygon
    #
    # stat_type: single_values_area
    #
    # @param ds A Pandas DataFrame
    # @param args Command Line Arguments
    # @return df_stat A list of pandas dataframe
	def get_avg_months_per_are_all(self, ds, args):
		print('--> get_avg_months_per_area, groupby id_areadata')
		# get time period
		t_cols = list(range(int(args['start_year']), int(args['end_year']) + 1))
		cols = []
		for i in t_cols:
			cols.append(str(i))
		#print(cols)
		# get total area
		#sum_area = ds['area'].sum()

		# get weighted mean
		#df_stat = ds.set_index(['id_area','area']).mean(axis=1) # working
		df_stat = {}
		#ds = ds.drop(columns=['year'])
		#print(ds)
		df = ds.groupby(['id_area','area'])['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].mean().reset_index()
		df1 = pd.DataFrame({'id_area': df['id_area'], 'month' : 1, 'value' : df['jan']})
		df2 = pd.DataFrame({'id_area': df['id_area'], 'month' : 2, 'value' : df['feb']})
		df3 = pd.DataFrame({'id_area': df['id_area'], 'month' : 3, 'value' : df['mar']})
		df4 = pd.DataFrame({'id_area': df['id_area'], 'month' : 4, 'value' : df['apr']})
		df5 = pd.DataFrame({'id_area': df['id_area'], 'month' : 5, 'value' : df['may']})
		df6 = pd.DataFrame({'id_area': df['id_area'], 'month' : 6, 'value' : df['jun']})
		df7 = pd.DataFrame({'id_area': df['id_area'], 'month' : 7, 'value' : df['jul']})
		df8 = pd.DataFrame({'id_area': df['id_area'], 'month' : 8, 'value' : df['aug']})
		df9 = pd.DataFrame({'id_area': df['id_area'], 'month' : 9, 'value' : df['sep']})
		df10 = pd.DataFrame({'id_area': df['id_area'], 'month' : 10, 'value' : df['oct']})
		df11 = pd.DataFrame({'id_area': df['id_area'], 'month' : 11, 'value' : df['nov']})
		df12 = pd.DataFrame({'id_area': df['id_area'], 'month' : 12, 'value' : df['dec']})
		df_stat['mean'] = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12], ignore_index=True)
		
		#print(df_stat)
		
		df_stat_mean = df_stat['mean'].groupby(['id_area'])['value'].mean().dropna().reset_index()
		
		#df_group = df_stat['mean'].mean
		
		#print('-> df_stat_mean :', df_stat_mean)
		return df_stat_mean
    
		#print(df_stat)
		return df_stat
    
    ## @brief Extract means over months for each area polygon
    #
    # stat_type: single_values_area
    #
    # @param ds A Pandas DataFrame
    # @param args Command Line Arguments
    # @return df_stat A list of pandas dataframe
	def get_avg_months_per_area_summer(self, ds, args):
		print('--> get_avg_months_per_area, groupby id_areadata')
		# get time period
		t_cols = list(range(int(args['start_year']), int(args['end_year']) + 1))
		cols = []
		for i in t_cols:
			cols.append(str(i))
		#print(cols)
		# get total area
		#sum_area = ds['area'].sum()

		# get weighted mean
		#df_stat = ds.set_index(['id_area','area']).mean(axis=1) # working
		df_stat = {}
		#ds = ds.drop(columns=['year'])
		df = ds.groupby(['id_area'])['apr','may','jun','jul','aug','sep'].mean().reset_index()
		
		#print(df)
		df4 = pd.DataFrame({'id_area': df['id_area'], 'month' : 4, 'value' : df['apr']})
		df5 = pd.DataFrame({'id_area': df['id_area'], 'month' : 5, 'value' : df['may']})
		df6 = pd.DataFrame({'id_area': df['id_area'], 'month' : 6, 'value' : df['jun']})
		df7 = pd.DataFrame({'id_area': df['id_area'], 'month' : 7, 'value' : df['jul']})
		df8 = pd.DataFrame({'id_area': df['id_area'], 'month' : 8, 'value' : df['aug']})
		df9 = pd.DataFrame({'id_area': df['id_area'], 'month' : 9, 'value' : df['sep']})
		df_stat['mean'] = pd.concat([df4,df5,df6,df7,df8,df9], ignore_index=True)
		
		#print(df_stat)
		
		df_stat_mean = df_stat['mean'].groupby(['id_area'])['value'].mean().dropna().reset_index()
		#print(df_stat_mean)
		#df_group = df_stat['mean'].mean
		
		#print('-> df_stat_mean :', df_stat_mean)
		return df_stat_mean
    
    ## @brief Extract means over months for each area polygon
    #
    # stat_type: single_values_area
    #
    # @param ds A Pandas DataFrame
    # @param args Command Line Arguments
    # @return df_stat A list of pandas dataframe
	def get_avg_months_per_area_winter(self, ds, args):
		print('--> get_avg_months_per_area, groupby id_areadata')
		# get time period
		t_cols = list(range(int(args['start_year']), int(args['end_year']) + 1))
		cols = []
		for i in t_cols:
			cols.append(str(i))
		#print(cols)
		# get total area
		#sum_area = ds['area'].sum()

		# get weighted mean
		#df_stat = ds.set_index(['id_area','area']).mean(axis=1) # working
		df_stat = {}
		#ds = ds.drop(columns=['year'])
		df = ds.groupby(['id_area','area'])['jan','feb','mar','oct','nov','dec'].mean().reset_index()
		
		#print(df)
		df1 = pd.DataFrame({'id_area': df['id_area'], 'month' : 1, 'value' : df['jan']})
		df2 = pd.DataFrame({'id_area': df['id_area'], 'month' : 2, 'value' : df['feb']})
		df3 = pd.DataFrame({'id_area': df['id_area'], 'month' : 3, 'value' : df['mar']})
		df10 = pd.DataFrame({'id_area': df['id_area'], 'month' : 10, 'value' : df['oct']})
		df11 = pd.DataFrame({'id_area': df['id_area'], 'month' : 11, 'value' : df['nov']})
		df12 = pd.DataFrame({'id_area': df['id_area'], 'month' : 12, 'value' : df['dec']})
        
		df_stat['mean'] = pd.concat([df1,df2,df3,df10,df11,df12], ignore_index=True)
		
		print(df_stat)
		
		df_stat_mean = df_stat['mean'].groupby(['id_area'])['value'].mean().dropna().reset_index()
		
		#df_group = df_stat['mean'].mean
		
		#print('-> df_stat_mean :', df_stat_mean)
		return df_stat_mean


    ## @brief Extract a yearly time serie for a region of selected areas
    #
    # Base values are area weighted means of selected areas for each time step
    #
    # Return weighted mean, minimum and maximum values as time series
    #
    # stat_type: single_values_time
    #
    # @param ds A pandas dataset
    # @param args Command line arguments as list
    # @return df_stat A list of pandas dataframe
	def get_timeline_year(self, ds, args):
		print('--> extract_single_values_time, time step: year')
		df_stat = {}
		# get time period
		#t_cols = list(range(int(args['start_year']), int(args['end_year']) + 1))

		# area weighted means
		if args['stat_spec'] == 'w_mean':
			# get total area
			sum_area = ds['area'].sum()
			df_stat = (ds.iloc[:,2:].multiply(ds['area'], axis=0).sum(skipna=False)/sum_area)
			df_stat = df_stat.reset_index()
			df_stat = df_stat.rename(columns={df_stat.columns[0]:'year',df_stat.columns[1]:'w_mean'})
		# minimum
		if args['stat_spec'] == 'min':
			df_stat = ds.iloc[:,2:].min()
			df_stat = df_stat.reset_index()
			df_stat = df_stat.rename(columns={df_stat.columns[0]:'year',df_stat.columns[1]:'min'})
			# maximum
		if args['stat_spec'] == 'max':
			df_stat = ds.iloc[:,2:].max()
			df_stat = df_stat.reset_index()
			df_stat = df_stat.rename(columns={df_stat.columns[0]:'year',df_stat.columns[1]:'max'})

		return df_stat.dropna().sort_values(by=['year']).reset_index(drop=True)

    ## @brief Extract a monthly time serie for a region of selected areas
    #
    # Base values are area weighted means of selected areas for each time step
    #
    # Return weighted mean, minimum and maximum values as time series
    #
    # stat_type: single_values_time
    #
    # @param ds A pandas dataset
    # @param args Command line arguments as list
    # @return df_stat A list of pandas dataframes
	def get_timeline_month(self, ds, args):
		print('--> extract_single_values_time, time step: month')
		print(ds)
		df_stat = {}
		# get total area
		#sum_area = ds[ds['year'] == int(args['start_year'])]['area'].sum()
		# get area weighted means for each month grouped by year
		df_stat = ds.groupby(['year'])['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].mean()
		print('--> df_stat : ', df_stat)
		df_stat = df_stat.stack().reset_index()
		df_stat = df_stat.rename(columns={df_stat.columns[1]:'month',df_stat.columns[2]:'w_mean'})
		print('--> df_stat : ', df_stat)
		return df_stat
    
    ## @brief Extract a mean monthly values for a region of selected areas
    #
    # Base values are area weighted means of selected areas for each time step
    #
    # Return average months over a time span
    #
    # stat_type: single_values_time
    #
    # @param ds A pandas dataset
    # @param args Command line arguments as list
    # @return df_stat A list of pandas dataframes
	def get_timeline_avg_month(self, ds, args):
		print('--> get_timeline_avg_month, time step: avg month')
		#print(ds)
		df_stat = {}
		# get total area
		#sum_area = ds[ds['year'] == int(args['start_year'])]['area'].sum()
		# get area weighted means for each month grouped by year
		df_stat = ds.groupby(['year'])['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].mean()
		#print(df_stat)
		df_stat = df_stat.mean(axis=0)
		#print(df_stat[['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']].mean())
		df_stat = df_stat.reset_index()
		df_stat = df_stat.rename(columns={df_stat.columns[0]:'month',df_stat.columns[1]:'mean'})
		print('--> df_stat : ', df_stat)
		return df_stat

	## @brief Extract the frequeny distribution for selected group areas by time period
    #
    # Frequency 1: distribution over a time series for area weighted means for each time step
    #
    # Frequency 2: distribution over areas for arithmetic mean of time period, distribution take into account the area sizes
    #
    # @param ds A pandas DataFrame
    # @param args Command line arguments
    # @return df_stat A list of pandas dataframes
	def get_histogram(self, args, ds):

		print('--> extract_frequency_distribution, time step: year')

		df_stat = {}

		# ????????????????????????????????????????
		# get standard statistics
		# df = ds.iloc[:,2:].stack()
		# print('--> df : ' + str(df))
		# mean = np.array(df[:,:].values)

		df_temp = ds.iloc[:,2:].stack().reset_index()
		df_temp = df_temp.rename(columns={df_temp.columns[0]:'id',df_temp.columns[1]:'year', df_temp.columns[2]:'val'})
		df_temp = df_temp.groupby(['id'])['val'].mean()
		print(df_temp)
		# get standard statistics
		mean = df_temp.values
		#print('--> mean : ' + str(mean))
		#print('min', np.nanmin(mean),'max', np.nanmax(mean))

        # get parameter classes
		classes = self.get_parameter_classes(args)
		#print('--> classes : ' + str(classes))

		# set lower limit for bins
		if classes['lower_limit'].iloc[0] == None:
			if float(classes['upper_limit'].iloc[0]) > float(np.nanmin(mean)):
				classes['lower_limit'].iloc[0] = float(np.nanmin(mean))
			else:
				classes['lower_limit'].iloc[0] = float(classes['upper_limit'].iloc[0])
		# set upper limit for bins
		if classes['upper_limit'].iloc[-1] == None:
			if float(classes['lower_limit'].iloc[-1]) < float(np.nanmax(mean)):
				classes['upper_limit'].iloc[-1] = float(np.nanmax(mean))
			else:
				classes['upper_limit'].iloc[-1] = float(classes['lower_limit'].iloc[-1])

		# get bins for histogram function
		classes['bins'] = classes['upper_limit'].astype(float)
		bins = classes['lower_limit'].astype(float).tolist()
		#upper_bin = classes['upper_limit'].iloc[-1].astype(float)
		upper_bin = float(classes['upper_limit'].iloc[-1])
		bins.append(upper_bin)
		#print('--> bins : ' + str(bins))

		# calculate histogram
		histo, bins = np.histogram(mean, bins)
		
		classes = self.get_parameter_classes(args)
		df_stat = pd.DataFrame([ classes['idclass'], classes['lower_limit'], classes['upper_limit'], classes['red'], classes['green'], classes['blue']]).T
		df_stat['count'] = histo

		return df_stat

	## @brief Extract the frequeny distribution of difference values for selected group areas by time period
    #
    # @param ds A pandas DataFrame
    # @param args Command line arguments
    # @return df_stat A list of pandas dataframes
	def get_diff_histogram(self, args, ds1, ds2):

		df_stat = {}

		print('--> extract_diff_frequency_distribution, time step: year')
		# cancel function if dataset number of rows are diffent
		if ds1.shape[0] != ds2.shape[0]:
			sys.exit('ERROR: diffent number of rows of ds1 and ds2', ds1.shape, ds2.shape)

		# calculate mean for ds1
		df_temp1 = ds1.iloc[:,2:].stack().reset_index()
		df_temp1 = df_temp1.rename(columns={df_temp1.columns[0]:'id',df_temp1.columns[1]:'year', df_temp1.columns[2]:'val1'})
		df_temp1 = df_temp1.groupby(['id'])['val1'].mean().reset_index()

		# calculate mean for ds2
		df_temp2 = ds2.iloc[:,2:].stack().reset_index()
		df_temp2 = df_temp2.rename(columns={df_temp2.columns[0]:'id',df_temp2.columns[1]:'year', df_temp2.columns[2]:'val2'})
		df_temp2 = df_temp2.groupby(['id'])['val2'].mean().reset_index()

		# calculate difference
		df_join = pd.merge(df_temp1, df_temp2, how = 'inner', on = 'id')
		df_join['diff'] = df_join['val1'] - df_join['val2']
		#print(df_join['diff'].max(),df_join['diff'].min(), df_join['diff'].mean())

		# get diff parameter classes
		classes = self.get_diff_parameter_classes(args)
		classes = classes.sort_values(by=['idclass'], ascending=False)
		#print('--> classes : ' + str(classes))

		# set lower limit for bins
		if classes['lower_limit'].iloc[0] == None:
			if float(classes['upper_limit'].iloc[0]) > float(np.nanmin(df_join['diff'])):
				classes['lower_limit'].iloc[0] = float(np.nanmin(df_join['diff']))
			else:
				classes['lower_limit'].iloc[0] = float(classes['upper_limit'].iloc[0])
		# set upper limit for bins
		if classes['upper_limit'].iloc[-1] == None:
			if float(classes['lower_limit'].iloc[-1]) < float(np.nanmax(df_join['diff'])):
				classes['upper_limit'].iloc[-1] = float(np.nanmax(df_join['diff']))
			else:
				classes['upper_limit'].iloc[-1] = float(classes['lower_limit'].iloc[-1])

		# get bins for histogram function
		classes['bins'] = classes['upper_limit'].astype(float)
		bins = classes['lower_limit'].astype(float).tolist()
		#upper_bin = classes['upper_limit'].iloc[-1].astype(float)
		upper_bin = float(classes['upper_limit'].iloc[-1])
		bins.append(upper_bin)
		#print(classes)
		#print('--> bins : ' + str(bins))

		# calculate histogram
		histo, bins = np.histogram(df_join['diff'], bins)
		
		classes = self.get_diff_parameter_classes(args)
		df_stat = pd.DataFrame([ classes['idclass'], classes['lower_limit'], classes['upper_limit'], classes['red'], classes['green'], classes['blue']]).T
		df_stat['count'] = histo

		return df_stat

	## @brief Get parameter classes from database
    #
    # @param args
    # @returns df, pandas dataframe
	def get_parameter_classes(self, args):
		print('--> get parameter classes')
		db = pg()
		dbconf = db.dbConfig(self.conf['dbconfig'])
		dbcon = db.dbConnect()
		
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
		dbconf = db.dbConfig(self.conf['dbconfig'])
		dbcon = db.dbConnect()

		sql = 'SELECT count(*) AS count FROM sessions.class_description_diff WHERE idtab = '+ str(args['id_tab']) + ' AND idparam = ' + str(args['id_param'])
		res = db.tblSelect(sql)

		count = int(np.array(res[0][0]).item())
		print('--> count', count)
		
		if count > 0:
			sql = 'SELECT idclass, lower_limit, upper_limit, red, green, blue FROM sessions.class_description_diff where idtab = '+ str(args['id_tab']) + ' AND idparam = ' + str(args['id_param']) + ' order by idclass desc'

		else:
			sql = 'SELECT idclass, lower_limit, upper_limit, red, green, blue FROM viewer_data.class_description_diff where idparam = ' + str(args['id_param']) + ' order by idclass desc'
		
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

	## @brief Save data from h5 files to csv file
	#
	# @param ds A pandas DataFrame
	# @param args Command line arguments
	# @return status:json, status of save action
	def save_h5data_to_csv(self, ds, args):
		print("--> save h5 data to csv file")
		r = {"action" : "ERROR"}
		data = None
		print(args)
		# extract data by time period definition
		if args['time_period'] == 'month':
			r = self.save_h5data_monthly(ds, args)

		if args['time_period'] == 'year':
			r = self.save_h5data_yearly(ds, args)

		# save to csv
		if 'data' in r:
			if r['data'].size > 0:
				r = self.save_ds_to_csv(r['data'], args)
			else:
				sys.exit('ERROR: length of dataframe is zero')
		else:
			sys.exit(r['action'])

		return r['action']

	## @brief Save monthly data from h5 files to csv file
	#
	# @param ds A pandas DataFrame
	# @param args Command line arguments
	# @return status:json, status of save action
	def save_h5data_monthly(self, ds, args):
		print("--> save monthly data")
		r = {
			"action" : "ERROR: failed to extract h5 monthly data"
		}
		# get area names
		df_names = self.get_area_names(args)
		# new structure: idarea, idarea_data, area_size, year, month, value
		# first month: january
		df = pd.DataFrame(ds.iloc[:,0:4])
		df['idarea'] = args['id_area']
		df['month'] = 1
		df = df.rename(columns={df.columns[1]:'id_areadata', df.columns[2]:'area_size', df.columns[3]:'value'})
		# append area names
		df = pd.merge(df, df_names, how='left', on='id_areadata')
		# reorder columns
		columnsTitles = ['idarea', 'id_areadata', 'name', 'area_size', 'year', 'month', 'value']
		df = df.reindex(columns = columnsTitles)

		# following monthes
		for i in range(5,15):
			df_month = pd.DataFrame(ds.iloc[:,0:4])
			df_month['value'] = ds.iloc[:,i]
			df_month['idarea'] = args['id_area']
			df_month['month'] = i - 2
			df_month = df_month.rename(columns={df_month.columns[1]:'id_areadata', df_month.columns[2]:'area_size'})
			# append area names
			df_month = pd.merge(df_month, df_names, how='left', on='id_areadata')
			# reorder columns
			columnsTitles = ['idarea', 'id_areadata', 'name', 'area_size', 'year', 'month', 'value']
			df_month = df_month.reindex(columns = columnsTitles)
			df = df.append( df_month )
			if sys.version[:2] == '2.':
				df = df[df['value'].notnull()]
			if sys.version[:2] == '3.':
				df = df[df['value'].notna()]

		r = {"action" : "SUCCESS"}
		r['data'] = df

		return r

	## @brief Save yearly data from h5 files to csv file
	#
	# @param ds A pandas DataFrame
	# @param args Command line arguments
	# @return status:json, status of save action
	def save_h5data_yearly(self, ds, args):
		print("--> save yearly data")
		r = {
			"action" : "ERROR: failed to extract h5 yearly data"
		}

		# get area names
		df_names = self.get_area_names(args)
		# new structure: idarea, idarea_data, area_name, area_size, year, value
		# first year
		df = pd.DataFrame(ds.iloc[:,0:3])
		df['idarea'] = args['id_area']
		df['year'] = args['start_year']
		df = df.rename(columns={df.columns[0]:'id_areadata', df.columns[1]:'area_size', df.columns[2]:'value'})
		# append area names
		df = pd.merge(df, df_names, how='left', on='id_areadata')
		# reorder columns
		columnsTitles = ['idarea', 'id_areadata', 'name', 'area_size', 'year', 'value']
		df = df.reindex(columns = columnsTitles)
		row, col = df.shape
		# following years
		for i in range(5,col+2):
			df_year = pd.DataFrame(ds.iloc[:,0:3])
			print('--> df_year : ' + str(df_year))
			df_year['value'] = ds.iloc[:,i]
			df_year['idarea'] = args['id_area']
			df_year['year'] = ds.columns[i]
			df_year = df_year.rename(columns={df_year.columns[0]:'id_areadata', df_year.columns[1]:'area_size'})
			# append area names
			df_year = pd.merge(df_year, df_names, how='left', on='id_areadata')
			# reorder columns
			columnsTitles = ['idarea', 'id_areadata', 'name', 'area_size', 'year', 'value']
			df_year = df_year.reindex(columns = columnsTitles)
			df = df.append( df_year )

		r = {"action" : "SUCCESS"}
		r['data'] = df
		return r

	## @brief Save data from pandas dataframe to csv file
	#
	# @param ds A pandas DataFrame
	# @param args Command line arguments
	# @return status:json, status of save action
	def save_ds_to_csv(self, ds, args):
		print("--> save extracted data to csv file")
		r = {
			"action" : "failed to save data to csv file",
			"fpath" : None
		}
		# get file name
		fn = str(args['id_param']) + '_' + str(args['id_area']) + '_' + str(args['id_scenario']) + '_' + args['time_period']
		# get file path
		path = args['proj_dir'] + '/csv_export/'
		if not os.path.exists(path):
			os.makedirs(path)
		# save to csv
		print("export file path: ", path, fn)
		try:
			ds.to_csv(path+fn+'.csv', index=False)
		except Exception as e:
			sys.exit(str(e))

		#with zipfile.ZipFile(path+fn+'.zip', 'w') as zf:
		#	zf.write(path+fn+'.csv', arcname=fn+'.csv')
		#os.remove(path+fn+'.csv')
		r = {
			"action" : "file saved sucessfully",
			"fpath" : path+fn
		}
		#except:
		#	r = {
		#		"action" : "failed to save data to csv file",
		#		"fpath" : path+fn
		#	}
		return r

	## @brief Get names of area units from database
	#
	# @param args
	# @returns df, pandas dataframe
	def get_area_names(self, args):
		print('--> get area names for id_areadata')
		db = pg()
		dbconf = db.dbConfig(self.conf['dbconfig'])
		dbcon = db.dbConnect()
		sql = 'select idarea_data, description_text from spatial.area_data where idarea = ' + str(args['id_area'])
		print(sql)
		res = db.tblSelect(sql)
		db.dbClose()
		if res[1] > 0:
			names = np.array(res[0])
			df_names = pd.DataFrame([names[:,0],names[:,1]]).T
			df_names = df_names.rename(columns={0:'id_areadata', 1:'name'})
			df_names['id_areadata'] = df_names['id_areadata'].astype(float)
		else:
			sys.exit('ERROR: no rows found')

		return df_names
