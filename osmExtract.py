import osmnx as ox
import geopandas as gpd
import os

def osmFeatures(key = 'amenities', value = 'hospitals'):
    place_name = 'NSW, Australia'
    gdf = ox.geometries_from_place(place_name, {key : value})
    return gdf

hospitals = osmFeatures()
hospitals.to_csv('assets/hospitals.csv')
