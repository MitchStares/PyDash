import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
npws = gpd.read_file("assets/tenure_npws_allmanagedland.zip")
#npws.info()
npws.set_index("NAME_SHORT")

fig = px.choropleth_mapbox(geojson = npws.geometry, 
              locations=npws.index,
              color = npws['TYPE'],
              mapbox_style="carto-positron")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()