""" Functions for managing and downloading Landsat imagery sourced from Google.

Functions in this script make use of a spatialite database of Landsat products
derived from the Google:

https://cloud.google.com/storage/docs/public-datasets/landsat
https://storage.googleapis.com/gcp-public-data-landsat/index.csv.gz

Pre-requisites:
	Assumes that Landsat imagery are stored within a folder specified by the 
	environment variable $L0data.



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


"""
# This example query does not use the spatial index, making it very slow:
sql = 'select SCENE_ID, SPACECRAFT_ID, DATE_ACQUIRED, WRS_PATH, WRS_ROW, CLOUD_COVER, DATA_TYPE, BASE_URL, Hex(ST_AsBinary(geom)) as geom from SpatialIndex where  ST_Intersects(BuildMBR(-47.1,67.0,-46.98,66.96), geom) and spacecraft_id=="LANDSAT_1" and EAST_LON > WEST_LON'

# Method of reading into a Pandas (not GeoPandas) dataframe:
df = pd.read_sql_query(sql, db, parse_dates=['DATE_ACQUIRED','SENSING_TIME'])

Queries can use a minimum bounding rectangle:
https://groups.google.com/forum/#!topic/spatialite-users/0FXMx2xgHPk 
ST_Transform(BuildMBR(-47.1,67.0,-46.98,66.96),4326) )

"""



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
		outfile = save_path + save_name

		with open(outfile + '.part', 'wb') as fp:
			for chunk in rfile.iter_content(chunk_size=65536):
				if chunk:
					fp.write(chunk)

		os.rename(outfile + '.part', outfile)

	return



def get_product_save_path(product_id, collection):
	return os.path.join(os.environ['L0data'], sensor_id, product_id)



def check_product_available(product_id, collection, bands):
	"""
	Check whether complete landsat product is available locally.

	product_id : Landsat Product ID, or Scene ID if collection == 'PRE'
	collection : collection number - PRE, 1, 2
	bands : tuple of bands to check

	The MTL.txt file is always checked for.
	If collecion type is 1 then the BQA band will also be checked for.

	Returns:
	tuple (status, bands_status)
	where status is boolean, False if any part of product is missing
	bands_status is dict {band:status}, including MTL and BQA.

	"""

	# Only attempt to check BQA product for collections in which it is available.
	if collection == 1:
		bands = list(bands)
		bands.append('BQA')

	bands.append('MTL')

	store = {}
	complete = True
	for b in bands:
		if b == 'MTL':
			url = http_path + '_MTL.txt'
			band_save_path = os.path.join(save_path, (product_id + '_MTL.txt'))
		else:
			url = http_path + '_%s.TIF' %(b)
			band_save_path = os.path.join(save_path, (product_id + '_%s.TIF' %(b)))

		if os.path.exists(band_save_path):
			store[b] = True
		else:
			store[b] = False
			complete = False

	return (complete, store)



def download_product(product_id, sensor_id, collection, gs_path, bands, 
	verbose=False,
	gs_gs_access='gs://gcp-public-data-landsat/',
	gs_http_access='https://storage.googleapis.com/gcp-public-data-landsat/'):
	""" Handle the downloading of the requested bands of a Landsat product.

	By default, download only occurs if file does not already exist locally.

	"""
	
	# Convert URl from GS to HTTP
	http_path = gs_http_access + gs_path.split(gs_gs_access)[1]
	# Add trailing scene_ID onto which to add the band identifier and file ending
	http_path = http_path + '/' + gs_path.split('/')[-1]

	# Path to save file to
	save_path = get_product_save_path(product_id, collection)

	# Make save directory
	try:
		os.makedirs(save_path)
	except FileExistsError:
		pass

	for b, status in bands:

		# Only proceed to download file if it is not already available.
		if status is True:
			continue

		if b == 'MTL':
			url = http_path + '_MTL.txt'
			band_save_path = os.path.join(save_path, (product_id + '_MTL.txt'))
		else:
			url = http_path + '_%s.TIF' %(b)
			band_save_path = os.path.join(save_path, (product_id + '_%s.TIF' %(b)))
		
		if verbose:
			print(url)

		try:
			download_file(url, band_save_path, save_name='')
		except IOError:
			print('Download of complete scene failed, deleting already-downloaded files.')
			os.rmdir(save_path)

	return



def download_products(df_down, show_progress=True):
	n = 1
	for ix, row in df_down.iterrows():
		print('%s/%s %s' %(n, ntot, row.SCENE_ID))

		product_status, bands_status = check_product_available(pid, row.COLLECTION_NUMBER, download_bands)
		if not product_status:
			download_product(pid, row.SENSOR_ID, row.COLLECTION_NUMBER, row.BASE_URL, bands_status, verbose=True)
		n += 1

	print('Downloading finished.')


def open_database(db_path):
	return spatialite.connect(db_path)



def execute_query(db, sql):
	""" Execute an SQL query on the landsat database.

	Products with no product_id set are given scene_id as their product_id,
	in these cases the collection number will be PRE.

	"""

	df_full = gpd.GeoDataFrame.from_postgis(sql, db, geom_col='geom', parse_dates=['DATE_ACQUIRED'])
	df_full[df_full.PRODUCT_ID == ''] = df_full.SCENE_ID
	return df_full



def filter_collection_1(df):
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

	return prepro_only	



		

