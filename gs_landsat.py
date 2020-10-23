""" Functions for managing and downloading Landsat imagery sourced from Google.

Functions in this script make use of a spatialite database of Landsat products
derived from the Google:

https://cloud.google.com/storage/docs/public-datasets/landsat
https://storage.googleapis.com/gcp-public-data-landsat/index.csv.gz

Pre-requisites:
	Assumes that Landsat imagery are stored within a folder specified by the 
	environment variable $L0lib.

	gunzip available on path.

Note: an alternative approach might be to run an SQL BigQuery on the Google
Cloud Platform, but at time of writing (Jan 2020) I haven't bothered to get
the relevant permissions with Google for this to work. (AJT) 

Andrew Tedstone, Nov 2019 - Jan 2020

"""

import spatialite
import pandas as pd
import geopandas as gpd
import os
import requests
import subprocess


"""
# This example query does not use the spatial index, making it very slow:

sql = 'select SCENE_ID, SPACECRAFT_ID, DATE_ACQUIRED, WRS_PATH, WRS_ROW, 
CLOUD_COVER, DATA_TYPE, BASE_URL, Hex(ST_AsBinary(geom)) as geom from 
SpatialIndex where  ST_Intersects(BuildMBR(-47.1,67.0,-46.98,66.96), geom) 
and spacecraft_id=="LANDSAT_1" and EAST_LON > WEST_LON'

# Method of reading into a Pandas (not GeoPandas) dataframe:
df = pd.read_sql_query(sql, db, parse_dates=['DATE_ACQUIRED','SENSING_TIME'])

Queries can use a minimum bounding rectangle:
https://groups.google.com/forum/#!topic/spatialite-users/0FXMx2xgHPk 
ST_Transform(BuildMBR(-47.1,67.0,-46.98,66.96),4326) )

"""



def get_product_save_path(product_id, sensor_id,
	path=os.environ['L0lib']):
	return os.path.join(path, sensor_id, product_id)



def check_product_available(product_id, collection, sensor, bands,
	check_bqa=False, check_mtl=False):
	"""
	Check whether complete landsat product is available locally.

	product_id : Landsat Product ID, or Scene ID if collection == 'PRE'
	collection : collection number - PRE, 1, 2
	bands : tuple of bands to check
	sensor : TM, MSS, etc
	check_bqa : boolean. If True check for *BQA.TIF file. Only for Collection 1 products.
	check_mtl : boolean. If True check for *MTL.TXT file.
	

	Returns:
	tuple (status, bands_status)
	where status is boolean, False if any part of product is missing
	bands_status is dict {band:status}, including MTL and BQA if requested.

	"""

	# Path to save file to
	save_path = get_product_save_path(product_id, sensor)

	# Only attempt to check BQA product for collections in which it is available.
	if check_bqa and collection == 1:
		bands = list(bands)
		bands.append('BQA')

	if check_mtl:
		bands.append('MTL')

	store = {}
	complete = True
	for b in bands:
		if b == 'MTL':
			band_save_path = os.path.join(save_path, (product_id + '_MTL.txt'))
		elif b == 'BQA':
			band_save_path = os.path.join(save_path, (product_id + '_BQA.TIF'))
		else:
			band_save_path = os.path.join(save_path, (product_id + '_B%s.TIF' %(b)))

		if os.path.exists(band_save_path):
			store[b] = True
		else:
			store[b] = False
			complete = False

		if sensor == 'ETM' and str(b).isnumeric():
			# Check for gap mask completeness
			gm_save_path = os.path.join(save_path, (product_id) + '_B%s_GM.TIF' %b)
			if os.path.exists(gm_save_path):
				store['GM%s' %b] = True
			else:
				store['GM%s' %b] = False
				complete = False
	
	return (complete, store)



def check_products_available(df, bands, check_bqa=True, check_mtl=True):
	""" Check entire dataframe of products - wrapper function.

	Inputs:
	df : dataframe of products to check
	bands : bands to products to check for
	check_bqa : boolean. If True, check for accompanying *BQA.TIF file.
	check_mtl : boolean. If True, check for accompanying *MTL.TXT file (only for Collection 1).

	Returns:
	Pandas dataframe, index product_id, one column per band, columns for each of
	BQA and MTL if requested.

	"""

	store = {}
	for ix, row in df.iterrows():
		product_status, bands_status = check_product_available(row.PRODUCT_ID, 
			row.COLLECTION_NUMBER, row.SENSOR_ID, bands,
			check_bqa=check_bqa, check_mtl=check_mtl)
		store[ix] = bands_status

	return pd.DataFrame.from_dict(store, orient='index')



def download_file(url, save_path, save_name=None):
	""" 

	"""

	with requests.Session() as s:
		#s.mount(base_url, requests.HTTPAdapter(max_retries=3))
		rfile = s.get(url, stream=True, timeout=(5,5))
		if not rfile.ok:
			raise IOError("Can't access %s" %url)

		if save_name is None:
			save_name = url.split('/')[-1]
		outfile = os.path.join(save_path, save_name)

		with open(outfile + '.part', 'wb') as fp:
			for chunk in rfile.iter_content(chunk_size=65536):
				if chunk:
					fp.write(chunk)

		os.rename(outfile + '.part', outfile)

	return



def download_product(product_id, sensor_id, collection, gs_path, bands, 
	verbose=False,
	gs_gs_access='gs://gcp-public-data-landsat/',
	gs_http_access='https://storage.googleapis.com/gcp-public-data-landsat/'):
	""" Handle the downloading of the requested bands of a Landsat product.

	By default, download only occurs if file does not already exist locally.

	bands must be the bands_status dictionary output by check_product_available

	Beyond normal bands (i.e. numerical, 1--6 or 1--11), this function also 
	accepts	the following special cases as 'bands':
		- 'BQA' - quality info geoTIFF
		- 'MTL' - metadata txt file
		- 'GM<b>' where <b> is the band number - ETM gap-fill mask


	"""
	
	# Convert URL from GS to HTTP
	http_path = gs_http_access + gs_path.split(gs_gs_access)[1]
	# Add trailing scene_ID onto which to add the band identifier and file ending
	http_path_im = http_path + '/' + gs_path.split('/')[-1]

	# Path to save file to
	save_path = get_product_save_path(product_id, sensor_id)

	# Make save directory
	try:
		os.makedirs(save_path)
	except FileExistsError:
		pass

	for b in bands:

		gm = False

		# Only proceed to download file if it is not already available.
		if bands[b] is True:
			continue

		if b == 'MTL':
			url = http_path_im + '_MTL.txt'
			#band_save_name = os.path.join(save_path, (product_id + '_MTL.txt'))
			band_save_name = product_id + '_MTL.txt'
			verify = False
		elif b == 'BQA':
			url = http_path_im + '_BQA.TIF'
			band_save_name = product_id + '_BQA.TIF'
			verify = False
		elif isinstance(b, str):
			if b[0:2] == 'GM': # ETM+ gap masks
				if b[2] == 6:
					b_gm = '%s_VCID_1' %b[2]
				else:
					b_gm = b[2]
				
				url = os.path.join(http_path, 'gap_mask', 
					gs_path.split('/')[-1] + '_GM_B%s.TIF.gz' %b_gm)

				band_save_name = product_id + '_B%s_GM.TIF.gz' %b_gm
				gm = True
				verify = False
		else:
			url = http_path_im + '_B%s.TIF' %(b)
			band_save_name = product_id + '_B%s.TIF' %(b)
			verify = True

		if verbose:
			print(url)

		try:
			download_file(url, save_path, save_name=band_save_name)
		except IOError:
			if verify is True:
				# Only delete scene if bands don't exist - ignore BQA and MTL files.
				print('Download of complete scene failed, deleting already-downloaded files.')
				os.rmdir(save_path)
			else:
				print('Warning: %s does not exist.' %b)
				continue

		if gm:
			# Gunzip the file
			# Generate individual QA GeoTIFF file
			cmd = 'gunzip %s' %os.path.join(save_path, band_save_name)
			subprocess.check_output(cmd, shell=True)

	return



def download_products(df_down, bands, show_progress=True, verbose=False):
	n = 1
	ntot = len(df_down)
	for ix, row in df_down.iterrows():
		print('%s/%s %s' %(n, ntot, row.PRODUCT_ID))

		product_status, bands_status = check_product_available(row.PRODUCT_ID, 
			row.COLLECTION_NUMBER, row.SENSOR_ID, bands)
		if not product_status:
			download_product(row.PRODUCT_ID, row.SENSOR_ID, 
				row.COLLECTION_NUMBER, row.BASE_URL, bands_status, verbose=verbose)
		n += 1

	print('Downloading finished.')



def open_database(db_path):
	""" Open connection to spatialite database """
	return spatialite.connect(db_path)



def execute_query(db, sql, geom_col=None):
	""" Execute an SQL query on the landsat database.

	Products with no product_id set are given scene_id as their product_id,
	in these cases the collection number will be PRE.

	"""

	if geom_col is not None:
		df_full = gpd.GeoDataFrame.from_postgis(sql, db, geom_col=geom_col, 
			parse_dates=['DATE_ACQUIRED'])
	else:
		df_full = pd.read_sql(sql, db, parse_dates=['DATE_ACQUIRED'])
	df_full.columns = [col.upper() for col in df_full.columns]
	df_full.loc[df_full.PRODUCT_ID == '', 'PRODUCT_ID'] = df_full.SCENE_ID
	df_full.index = df_full.PRODUCT_ID
	return df_full



def filter_collection_2020(df):
	return df[(df.COLLECTION_NUMBER == 2020)]



def filter_collection_1(df):
	"""	Filter dataframe to return only Collection 1 data """
	return df[(df.COLLECTION_NUMBER == 1)]



def filter_collection_pre(df):
	"""Find data only available as PRE. 

	Only works if both Collection 1 and PRE data have been requested in initial
	SQL query!

	Inputs:
		df : pd.DataFrame of contents of query to Landsat database.
	"""
	
	# First do counts to see if duplicated
	counts = df.SCENE_ID.groupby(df.SCENE_ID).count()
	# Apply counts back to main dataframe
	df = df.join(counts, rsuffix='_COUNT')
	prepro_only = df[(df.SCENE_ID_COUNT == 1) & (df.COLLECTION_NUMBER == 'PRE')]
	prepro_only = prepro_only.drop(labels='SCENE_ID_COUNT', axis=1)
	
	return prepro_only	



		

