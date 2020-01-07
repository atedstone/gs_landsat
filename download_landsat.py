"""
This is a relatively simple example of how to use the library.

Primary use of library is as plug-in to other projects.

Queries can use a minimum bounding rectangle:
https://groups.google.com/forum/#!topic/spatialite-users/0FXMx2xgHPk 
ST_Transform(BuildMBR(-47.1,67.0,-46.98,66.96),4326) )

"""

import gs_landsat

download_bands = ('B1','B2','B3','B4','B5')

db = gs_landsat.open_database(os.environ['L0data'] + 'landsat_gs.sqlite')

sql2 = "select DATE_ACQUIRED, SENSOR_ID, DATA_TYPE, SCENE_ID, PRODUCT_ID, BASE_URL, WRS_PATH, WRS_ROW, CLOUD_COVER, COLLECTION_NUMBER, Hex(ST_AsBinary(geom)) as geom from landsat where EAST_LON > WEST_LON and SPACECRAFT_ID='LANDSAT_5' and (rowid in (select rowid from SpatialIndex where ((f_table_name = 'landsat') and (f_geometry_column = 'geom') and ( search_frame=MakePoint(-47.0,67.0)) )))"
df_full = gs_landsat.execute_query(db, sql2)


# Select a single scene for download.
df = df_full[df_full.CLOUD_COVER < 20]
df_down = df[df.SCENE_ID == 'LT50050131991221KIS00']



gs_landsat.download_products(df_down)