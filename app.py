import dash
from dash import dcc, html, dash_table
import pandas as pd
import plotly.express as px
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__)

# Load your data into a Pandas dataframe
df = pd.read_csv("https://raw.githubusercontent.com/MitchStares/Spatial-Analysis-in-R/master/Euc_sieberii_illawarra.csv")

# Create a Mapbox map
map_figure = px.scatter_mapbox(df, lat="Latitude", lon="Longitude", zoom=6)
map_figure.update_layout(mapbox_style="open-street-map")


# Define the data table
data_table = dash_table.DataTable(
    id="data_table",
    columns=[{"name": col, "id": col} for col in df.columns],
    data=df.iloc[:10].to_dict("records"),
    fixed_rows={"headers": True, "data": 0},
    style_cell={"width": "100px"},
    style_table={"overflowX": "scroll"},
    page_current=0,
    page_size=10
)

# Define a callback to show or hide the data table
@app.callback(
    dash.dependencies.Output("data_table-container", "className"),
    [dash.dependencies.Input("show_data_table_button", "n_clicks")]
)
def show_data_table(n_clicks):
    if n_clicks is None:
        return "hidden"
    elif n_clicks % 2 == 0:
        return "hidden"
    else:
        return ""


# Define the layout for the dashboard
app.layout = html.Div(children=[
    html.Div(children=[
        dcc.Graph(id="map", figure=map_figure),
        dcc.Store(id="map_data", data=df.to_dict("records"))
    ], style={"display": "inline-block", "width": "50%"}),
    html.Div(children=[
        html.Button("Show data table", id="show_data_table_button"),
        html.Div(
            id="data_table-container",
            children=[data_table],
            className="hidden",
            style={"display": "none"} if "hidden" in "hidden" else {"display": "inline-block"}
        )
    ], style={"display": "inline-block", "width": "50%"})
])
if __name__ == "__main__":
    app.run_server(debug=True)