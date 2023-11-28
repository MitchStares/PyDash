# Databricks notebook source
# MAGIC %md
# MAGIC # NSW Fire History
# MAGIC The following script processes the NPWSFireHistory layer taken from nsw seed portal. 
# MAGIC This script is designed mostly as a one-time process rather than a continual scheduled

# COMMAND ----------

# MAGIC %md
# MAGIC ## Packages and Setup

# COMMAND ----------

# MAGIC %pip install fiona shapely pyproj rtree #these need to be first, otherwise geopandas will install the wrong version of rtree (not sure why)
# MAGIC %pip install openpyxl
# MAGIC %pip install geopandas
# MAGIC %pip install folium matplotlib mapclassify
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

import pandas as pd
import json
import requests
import geopandas as gpd
from datetime import date
today = date.today()
from pyspark.sql.types import *
from pyspark.sql.functions import *
import folium
import mosaic as mos
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Extraction & Transformations

# COMMAND ----------

# ## Pulling LGA Spatial table from snowflake. The snowflake settings make it a little clunky. 

# serviceUser = "dac-snowflake-ResiliencePaaS@dac.nsw.gov.au"
# servicePassword = "@ResNSWDE2023"
# options = {
# "sfUrl": "https://va76158.australia-east.privatelink.snowflakecomputing.com",
# "sfUser": serviceUser,
# "sfPassword": servicePassword,
# "sfDatabase": "RAW_RESILIENCE",
# "sfSchema": "STAGING",
# "sfWarehouse": "RESILIENCE_GENERAL_WH",
# "sfRole": "RESILIENCE_ADMIN"
# }
# lgas = spark.read.format("snowflake").options(**options).option("dbtable", 'RAW_LGA_SPATIAL').load()

# display(lgas)


# ##Comparison - Reading from databricks delta table, same format as snowflake table
# # lgas = spark.read.table('hive_metastore.default.lgapoly')


# COMMAND ----------

## Basic ETL for shapefile to delta and snowflake. No need to run again unless data gets updated. 

# firehistory = gpd.read_file("/dbfs/mnt/raw-resilience-paas/shapefiles/bushfire/fire_npwsfirehistory.zip", layer = "NPWSFireHistory")
# firehistory['StartDate'] = pd.to_datetime(firehistory['StartDate'])
# firehistory['EndDate'] = pd.to_datetime(firehistory['EndDate'])
# firehistory['wkt'] = firehistory.geometry.to_wkt()
# firehistory['wkb'] = firehistory.geometry.to_wkb()
# firehistory = pd.DataFrame(firehistory.drop(columns='geometry'))
# firehistory[['FireSeason', 'FireType']] = firehistory['Label'].str.split(' ', n=1, expand=True)
# firehistory['DateUpdated'] = pd.to_datetime(today)

# firehistory_sdf = spark.createDataFrame(firehistory)
# display(firehistory_sdf)
# # firehistory.dtypes


# COMMAND ----------

# # Write the data to a table.
# table_name = "npwsfirehistory"
# firehistory_sdf.write.format("delta").mode("append").saveAsTable(table_name)

# ##Snowflake
# firehistory_sdf.write.format("snowflake").options(**options).option("dbtable", 'RAW_NPWSFIREHISTORY').mode("APPEND").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data preparation
# MAGIC Here we are using spark to read in the layers we have stored in the databricks table storage. We can just as easily use spark to read from snowflake if we have data in there. 
# MAGIC The only real prep we need to do is limit our data to our focal region and any other major filters (like the commented out fire season and firetype filters). This is best done here on load using spark as spark is very efficient at this. 

# COMMAND ----------

#until pandas udfs are implemented in spark, the geometry step of this will be commented out. 

#  .filter((col('FireSeason') == '2019-20') & (col('FireType') == 'Wildfire'))\
#.withColumn('geometry', mos.st_geomfromwkt(col('wkt')))
fires = spark.read.table('hive_metastore.default.npwsfirehistory')\
    .display()

# COMMAND ----------

#After running this a few times, turns out I don't really need mosaic to do the geomfromwkb conversion since I do that in geopandas anyway. 
#until pandas udfs are implemented in spark, the geometry step of this will be commented out. 

#.withColumn('geom',mos.st_geomfromwkb(col('WKB')))\
lga = spark.read.table('hive_metastore.default.lgapoly')\
    .filter(col('STE_CODE16')==1)\
    .display()

# COMMAND ----------

#Convert the dataframes to geopandas dataframes. Ideally this is eliminated and its all done in spark using UDFs (User defined functions)
lgaSpatial = gpd.GeoDataFrame(lga, crs="EPSG:4283",geometry=gpd.GeoSeries.from_wkb(lga.WKB))
firesSpatial = gpd.GeoDataFrame(fires, crs = "EPSG:4283", geometry=gpd.GeoSeries.from_wkt(fires.wkt))

# COMMAND ----------

#Simply Folium/Leaflet map of a fire season
## Run code to produce interactive leaflet map of a fire season

# m = folium.Map(location = ['-33.8688', '151.2093'],zoom_start=8, tiles="CartoDB positron")
# fires1920 = firesSpatial.query('Label == "2019-20 Wildfire"')
# for _, r in fires1920.iterrows():
#     # Without simplifying the representation of each fire,
#     # the map might not be displayed
#     sim_geo = gpd.GeoSeries(r["geometry"]).simplify(tolerance=0.001)
#     geo_j = sim_geo.to_json()
#     geo_j = folium.GeoJson(data=geo_j, style_function=lambda x: {"fillColor": "orange"})
#     folium.Popup("Fire name: " + str(r["FireName"]) + " <br>" + "Burnt Area (ha): " + str(r['AreaHa'])).add_to(geo_j)
#     geo_j.add_to(m)
# m

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fire history per LGA
# MAGIC The following code is designed to run across the fire history layer and extract how many fire polygons and how much area burnt within each LGA per fire season. 
# MAGIC This is done by looping through the fire history dataset, filtering each loop iteration to a single fire season using the Label column. 
# MAGIC From here, we use the overlay function from `geopandas` to intersect both layers producing a dataframe containing a single row for each fire and lga combination. **When a fire polygon occurs over multiple lgas, we will get duplicate rows so we can expect some significant blowout of area burnt for large fires.** This appeared to be the case as the existing areaha column was being duplicated per lga for fires crossing boundaries. To rectify this, we use the geopandas functions for calculating the area of a polygon. However, the layer needs to be reprojected into both a coordinate reference system that uses meters rather than degrees of latitude and longitude, but also an equal area projection around your area of interest that maintains appropriate distances between things rather than distortions common to projections like mercator. 
# MAGIC
# MAGIC Then, we run a simple groupby and aggregate to grab the number of fires and sum of area in hectares
# MAGIC
# MAGIC This could be converted to a pandas udf for usage in spark but need to work on those. 

# COMMAND ----------

##This map example demonstrates this issue with our blowout of hectares. While the overlay code works well in clipping the polygons to an LGA, each fire carries its existing areaha calculation with it. Meaning when we do a sum, we are multiplying any large fire Ha by the amount of lgas it falls over. 
#Simply put, we need to do the area calculation in the loop.

fires2019 = firesSpatial[(firesSpatial.FireSeason == '2019-20')]
fires2019.groupby(['FireSeason', 'FireType']).agg({'AreaHa':'sum'})

blueMountains = lgaSpatial[(lgaSpatial.LGA_CODE_2019 == 10900)]
fireOverlay = gpd.overlay(blueMountains, fires2019, how = 'intersection')
# display(fireOverlay)

m = folium.Map(location = ['-33.8688', '151.2093'],zoom_start=8, tiles="CartoDB positron")

#Overlayed Fires
sim_geo = gpd.GeoSeries(fireOverlay["geometry"]).simplify(tolerance=0.001)
geo_j = sim_geo.to_json()
geo_j = folium.GeoJson(data=geo_j, style_function=lambda x: {"fillColor": "orange"}, name='ClippedFires - 2019/20')
# folium.Popup("Fire name: " + str(r["FireName"]) + " <br>" + "Burnt Area (ha): " + str(r['AreaHa'])).add_to(geo_j)
geo_j.add_to(m)

#LGA boundary
bm_geo = gpd.GeoSeries(blueMountains['geometry']).simplify(tolerance=0.001)
bm_geo = bm_geo.to_json()
bm_geo = folium.GeoJson(data = bm_geo, style_function=lambda x: {"fillColor": "gray"}, name = 'LGA')    
bm_geo.add_to(m)


fires_geo = gpd.GeoSeries(fires2019['geometry']).simplify(tolerance=0.001)
fires_geo = fires_geo.to_json()
fires_geo = folium.GeoJson(data=fires_geo, style_function=lambda x: {"fillColor": "red"}, name='AllFires - 2019/20')
fires_geo.add_to(m)

folium.LayerControl().add_to(m)

m



# COMMAND ----------

## Area calculations
blueMountains = lgaSpatial[(lgaSpatial.LGA_CODE_2019 == 10900)]
#1431.1442 Square kms = 143,114.42ha as per data

#Without reprojecting
# blueMountains['AreaCalc'] = blueMountains['geometry'].area/10000 #
# print(blueMountains['AreaCalc'])
#0.000014ha. Very clearly wrong. 

blueMountains= blueMountains.to_crs("+proj=cea +lat_0=-33.8688 +lon_0=151.2093 +units=m") #issue was around projections. Need to project to a cylindrical equal area projection centred on a point of interest (sydney in my case but could do the dead centre of NSW)
blueMountains['AreaCalc'] = blueMountains['geometry'].area/10000
#143,114.264289 as per the calc after reprojection
display(blueMountains)


# COMMAND ----------

#Main logic for LGA intersection
firelist = []
for label in firesSpatial.Label.unique(): #loop through unique fire seasons
    fireyear = firesSpatial[(firesSpatial.Label == label)] #filter dataframe to current iterations fire season
    lga_fire_intersect = gpd.overlay(lgaSpatial, fireyear, how = "intersection", keep_geom_type=False) #intersect with lga layer

    lga_fire_intersect = lga_fire_intersect.to_crs("+proj=cea +lat_0=-33.8688 +lon_0=151.2093 +units=m") #reproject to cyclindrical equal area projection centred on sydney. 
    #Improvement: use string format replacement to insert the centroid of the whole layer to the projection lat lon centre points
    lga_fire_intersect['LGAArea'] = lga_fire_intersect['geometry'].area/ 10000 #calculating the new area for the fire inside the lga and converting from square meters to hectares

    lga_fire_stats = lga_fire_intersect.groupby(['LGA_NAME19','LGA_CODE_2019','FireSeason', 'FireType'])\
        .agg({'FireNo':'size', 'LGAArea': 'sum'})\
        .reset_index()\
        .rename(columns={'FireNo':"FIRECOUNT",'LGAArea':'AREAHA', 'FireSeason':'FIRESEASON', "FireType":'FIRETYPE'})
    firelist.append(lga_fire_stats) #append to list
lga_fires = pd.concat(firelist,ignore_index=True) #concatinate list into a single dataframe


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
import geopandas as gpd
import pandas as pd

# Assuming you have a Spark session already created
# spark = SparkSession.builder.appName("example").getOrCreate()

fires = spark.read.table('hive_metastore.default.npwsfirehistory')
lga = spark.read.table('hive_metastore.default.lgapoly')\
    .filter(col('STE_CODE16')==1)\

# Assuming you have a Spark session already created
# spark = SparkSession.builder.appName("example").getOrCreate()

@pandas_udf("struct<LGA_NAME: string, LGA_CODE: string, FIRESEASON: string, FIRETYPE: string, FIRECOUNT: bigint, AREAHA: double>", PandasUDFType.GROUPED_MAP)
def intersect_and_aggregate(key, data, *args):
    data_wkt_col, lga_name_col, lga_code_col, fire_season_col, fire_type_col, fire_count_col, lga_wkt = args[:-1]
    lgaspatial_gdf = args[-1]

    data['geometry'] = data[wkt_col].apply(wkt.loads)  # Convert WKT column to Shapely geometries
    gdf = gpd.GeoDataFrame(data, geometry=geometry)

    lgaspatial_gdf['geometry'] = lgaspatial_gdf[lga_wkt].apply(wkt.loads)  # Convert WKT column in lgaspatial to Shapely geometries
    lgaspatial_gdf = gpd.GeoDataFrame(lgaspatial_gdf, geometry = geometry )

    firelist = []
    for label in gdf[fire_season_col].unique():
        fireyear = gdf[gdf[fire_season_col] == label]
        lga_fire_intersect = gpd.overlay(lgaspatial_gdf, fireyear, how="intersection", keep_geom_type=False)
        lga_fire_intersect = lga_fire_intersect.to_crs("+proj=cea +lat_0=-33.8688 +lon_0=151.2093 +units=m")
        lga_fire_intersect['LGAArea'] = lga_fire_intersect[geometry].area / 10000

        lga_fire_stats = lga_fire_intersect.groupby([lga_name_col, lga_code_col, fire_season_col, fire_type_col]) \
            .agg({fire_count_col: 'size', 'LGAArea': 'sum'}) \
            .reset_index() \
            .rename(columns={fire_count_col: 'FIRECOUNT', 'LGAArea': 'AREAHA', fire_season_col: 'FIRESEASON', fire_type_col: 'FIRETYPE'})
        firelist.append(lga_fire_stats)

    lga_fires = pd.concat(firelist, ignore_index=True)
    return lga_fires

# Assuming you have two Spark DataFrames: lgaSpatialDF and firesSpatialDF
# lgaSpatialDF and firesSpatialDF should have the same column names as the ones used in the geopandas operations

result_df = fires.groupby("FireSeason").apply(
    lambda key, data: intersect_and_aggregate(key, data, "wkt", "LGA_NAME19", "LGA_CODE_2019", "FireSeason", "FireType", "FireNo", 'WKT', lga)
)
result_df.show()


# COMMAND ----------

lga_fires = spark.createDataFrame(lga_fires)
display(lga_fires)

# COMMAND ----------

# # Write the data to a table.
# table_name = "lga_firehistory"
# lga_fires.write.format("delta").mode("overwrite").saveAsTable(table_name)

# serviceUser = "dac-snowflake-ResiliencePaaS@dac.nsw.gov.au"
# servicePassword = "@ResNSWDE2023"
# options = {
# "sfUrl": "https://va76158.australia-east.privatelink.snowflakecomputing.com",
# "sfUser": serviceUser,
# "sfPassword": servicePassword,
# "sfDatabase": "RAW_RESILIENCE",
# "sfSchema": "STAGING",
# "sfWarehouse": "RESILIENCE_GENERAL_WH",
# "sfRole": "RESILIENCE_ADMIN"
# }
# sdf.write.format("snowflake").options(**options).option("dbtable", 'RAW_FIRESNEARME').mode("APPEND").save()
# # The above write function could be added to the end of the read function to allow for a read and write to occur within the same spark function. But we won't so we can check things before writing to snowflake. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Landuse classifications
# MAGIC Reading and transforming the NSW Landuse 2017 v1.2 layer from the Seed Database - https://datasets.seed.nsw.gov.au/dataset/nsw-landuse-2017-v1p2-f0ed  
# MAGIC
# MAGIC Key points of consideration:  
# MAGIC - Calculate area (ha) of each landuse type per LGA  
# MAGIC   - Consider comparing against ABS MeshBlock Landuse Areas
# MAGIC - Intersect fires per season
# MAGIC   - Calculate area (ha) burnt per classification type
# MAGIC   - Calculate % burnt
# MAGIC

# COMMAND ----------

landuse = mos.read().format('multi_read_ogr')\
    .option("driverName", "ESRI Shapefile")\
    .option("vsizip", "true")\
    .option("layerName", "NSWLanduse2017v1p2")\
    .option("asWKB", "true")\
    .option('inferSchema', 'true')\
    .load("dbfs:/mnt/raw-resilience-paas/shapefiles/NSWLanduse2017v1p2.zip")\
    .toPandas()
    # .display()
    # .withColumn('wkt', mos.st_astext('SHAPE'))\
    # .withColumn('wkb', mos.st_aswkb('wkt'))\
    # .drop('SHAPE','SHAPE_srid','SHAPE_Length','SHAPE_Area', 'wkt')

landuseSpatial =  gpd.GeoDataFrame(landuse, crs="EPSG:4283",geometry=gpd.GeoSeries.from_wkb(landuse.geom_0))



# COMMAND ----------

#Main logic for LGA intersection with landuse
landuselist = []
for luType in landuseSpatial.Secondary.unique(): #loop through unique fire seasons
    landuseType = landuseSpatial[(landuseSpatial.Secondary == luType)] #filter dataframe to current iterations fire season
    landuse_lga_intersect = gpd.overlay(lgaSpatial, landuseType, how = "intersection", keep_geom_type=False) #intersect with lga layer

    landuse_lga_intersect = landuse_lga_intersect.to_crs("+proj=cea +lat_0=-33.8688 +lon_0=151.2093 +units=m") #reproject to cyclindrical equal area projection centred on sydney. 
    #Improvement: use string format replacement to insert the centroid of the whole layer to the projection lat lon centre points
    landuse_lga_intersect['LGAArea'] = landuse_lga_intersect['geometry'].area/ 10000 #calculating the new area for the fire inside the lga and converting from square meters to hectares

    lga_landuse_stats = landuse_lga_intersect.groupby(['LGA_NAME19','LGA_CODE_2019','Secondary'])\
        .agg({'LGAArea': 'sum'})\
        .reset_index()\
        .rename(columns={'LGAArea':'AREAHA', 'Secondary':'LANDUSETYPE'})
    landuselist.append(lga_landuse_stats) #append to list
lga_landuse = pd.concat(landuselist,ignore_index=True) #concatinate list into a single dataframe


# COMMAND ----------

display(lga_landuse)

# COMMAND ----------



# COMMAND ----------

## Basic logic for calculating the number of fires each LGA has experienced
## Going to use ST_Intersect functions in spark on both the delta tables

# @pandas_udf('float')
# def count_fires(wkt)

#               
#     polys = gpd.GeoSeries.from_wkt(wkt)
#     # indices = h3.polyfill(geo_json_geom, resolution, True)
#     # h3_list = list(indices)
#     return polys.area

# fireslga = lga.withColumn('firecount', count(mos.st_intersects(col('WKT'),fires.select(col('wkt')))==True))\
#     .display()


# fireslga = lga.join(fires, col('geom')==col('geometry'))\
    # .groupBy(col('LGA_NAME19'))\
    # .agg(mos.st_intersection_aggregate(col('geom'),col('geometry'))).display()


# COMMAND ----------


