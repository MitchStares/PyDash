import dash
from dash import dcc, html, dash_table
import pandas as pd
import plotly.express as px
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__)

# Load your data into a Pandas dataframe
df = pd.read_csv("https://raw.githubusercontent.com/MitchStares/Spatial-Analysis-in-R/master/Euc_sieberii_illawarra.csv")
df = df.loc[:,['Latitude','Longitude','Scientific Name - original', 'Local Government Areas 2011']]
# Create a Mapbox map
map_figure = px.scatter_mapbox(df, lat="Latitude", lon="Longitude", zoom=6)
map_figure.update_layout(mapbox_style="open-street-map", 
                         margin=dict(t=0, b=0, l=0, r=0),
                         autosize = True,
                         hovermode = 'closest')

# Define the data table
data_table = dash_table.DataTable(
    id="data_table",
    columns=[{"name": col, "id": col} for col in df.columns],
    data=df.iloc[:10].to_dict("records"),
    sort_action = 'native',
    selected_rows=[],
    page_action='native',
    fixed_rows={"headers": True, "data": 0},
    style_cell={"width": "100px"},
    style_table={"overflowX": "scroll"},
    page_current=0,
    page_size=10
)

@app.callback(
    dash.dependencies.Output('data_table', 'data'),
    [dash.dependencies.Input('map', 'selectedData')])
def update_data_table(selectedData):
    if selectedData is None:
        return df.to_dict('records')
    else:
        selected_indices = [point['pointIndex'] for point in selectedData['points']]
        filtered_df = df.iloc[selected_indices]
        return filtered_df.to_dict('records')


# @app.callback(
#     dash.dependencies.Output("data_table", "data"),
#     [dash.dependencies.Input("map", "clickData"),
#      dash.dependencies.Input("map", "selectedData"),
#      dash.dependencies.Input("reset_button", "n_clicks")],
#     [dash.dependencies.State("map_data", "data")]
# )
# def update_data_table(clickData, selectedData, n_clicks, map_data):
#     if n_clicks is not None:
#         return map_data
#     elif clickData is not None and clickData["points"]:
#         selected_points = clickData["points"]
#         selected_indexes = [point["pointIndex"] for point in selected_points]
#         selected_data = [map_data[index] for index in selected_indexes]
#         return selected_data
#     elif selectedData is not None:
#         selected_points = selectedData["points"]
#         selected_indexes = [point["pointIndex"] for point in selected_points]
#         selected_data = [map_data[index] for index in selected_indexes]
#         return selected_data
#     else:
#         return map_data



# Define a callback to show or hide the data table
@app.callback(
    dash.dependencies.Output("data_table-container", "style"),
    [dash.dependencies.Input("show_data_table_button", "n_clicks")]
)
def show_data_table(n_clicks):
    if n_clicks is None:
        return {"display": "none"}
    elif n_clicks % 2 == 0:
        return {"display": "none"}
    else:
        return {"display": "inline-block"}

@app.callback(
    dash.dependencies.Output('selected-points', 'children'),
    [dash.dependencies.Input('map', 'selectedData')]
)
def update_selected_points(selectedData):
    if selectedData is None:
        return 0
    else: 
        return len(selectedData['points'])

# Define the layout for the dashboard
app.layout = html.Div(style={
        "display": "flex",
        "flexDirection": "row",
        "height": "100vh"
    },children=[
    html.Div(style={
                "width": "50vw",
                "height": "100vh",
                "padding": "0"
            },children=[
        dcc.Graph(id="map", figure=map_figure, config={"displayModeBar":True,"displaylogo":False, 'modeBarButtonsToRemove':['zoomIn','zoomOut','resetViewMapbox','toImage']}),
        dcc.Store(id="map_data", data=df.to_dict("records"))
    ]),
    html.Div(style={
                "width": "50vw",
                "height": "100vh",
                "overflowY": "auto",
                "padding": "0"
            },children=[
        # html.Button("Reset", id="reset_button", n_clicks=0),
        html.Button("Show data table", id="show_data_table_button", n_clicks = 0),
        html.Div(
            id="data_table-container",
            children=[data_table],
            style={"display": "none"}
        ), html.Div([html.Span("Selected Points: ", className = 'text-icon'), 
                    html.Span(id='selected-points', className='text-icon')], className = 'text-box')
    ])
])
if __name__ == "__main__":
    app.run_server(debug=True)