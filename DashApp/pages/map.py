import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objs as go
import requests
import pandas as pd
import numpy as np
import datetime
from app import app

SOURCES = ['USGS', 'EMSC']

column1 = dbc.Col(
    [
        dcc.Markdown(
            """

            ## Recent Earthquakes
            The map at the right shows the recent earthquakes. Larger dots are
            larger earthquakes.

            Use the drop down menu what period of time you would like
            to see earthquakes for.

            You can adjust for minimum magnitude using the slider below

            """
        ),
        html.Div([
            dcc.Dropdown(
                id='source',
                options=[
                    {'label': 'USGS', 'value': 'USGS'},
                    {'label': 'EMSC', 'value': 'EMSC'},
                    {'label': 'BOTH', 'value': 'BOTH'}
                ],
                value='USGS'
            ),

            dcc.Dropdown(
                id='timeFrame',
                options=[
                    {'label': 'Last Quake', 'value': 'Quake'},
                    {'label': 'Last Hour', 'value': '/hour'},
                    {'label': 'Last Day', 'value': '/day'},
                    {'label': 'Last Week', 'value': '/week'},
                    {'label': 'Last Month', 'value': '/month'}
                ],
                value='Quake'
            ),
            html.Div(id='menuItems')
        ]),
        html.Div([
            dcc.Markdown("\n\n\n\n"),
            dcc.Slider(
                id='magnitude',
                min=0,
                max=10,
                step=.5,
                value=5.5
            ),
            dcc.Markdown(id='sliderOutput')
        ])
    ],
    md=2,
)

# fig = go.Figure()


@app.callback(
    dash.dependencies.Output('sliderOutput', 'children'),
    [dash.dependencies.Input('magnitude', 'value')])
def display_min_mag(mag_num):
    return f"Minimum Magnitude: {mag_num}"


@app.callback(
    dash.dependencies.Output('wheretheDataGoes', 'children'),
    [dash.dependencies.Input('timeFrame', 'value'),
     dash.dependencies.Input('magnitude', 'value'),
     dash.dependencies.Input('source', 'value')])
def update_output(value, mag, source):
    if source != 'BOTH':
        print('running with one source')
        return single_source(value, mag, source)
    else:
        print('running with two sources')
        return dual_source(value, mag)


def dual_source(value, mag):
    if value == 'Quake':
        return dual_last(mag)
    else:
        df = pd.DataFrame()
        for source in SOURCES:
            api_url = f'https://quake-ds-staging.herokuapp.com/last/{source}/{value}/{float(mag)}'
            data = requests.get(api_url)
            if data.json()['num_quakes'] != 0:
                df = df.append(data.json()['message'])
    if df.shape[0] > 0:
        title = f'Quakes over {mag} from Both USGS and EMSC in the last {value.strip("/")}'
        df['color'] = df['Oceanic'].apply(lambda x: 'yellow' if x != x else 'blue')
        data, layout = loaded_fig(df)
    else:
        title = f'No Quakes over {mag} from either USGS or EMSC in the last {value.strip("/")}'
        data, layout = empty_fig()

    fig = go.Figure(data=data, layout=layout)
    fig.update_layout(mapbox_style='stamen-terrain', height=700, title=title)
    return dcc.Graph(figure=fig)


def dual_last(mag):
    quakes = []
    for i, source in enumerate(SOURCES):
        api_url = f'https://quake-ds-staging.herokuapp.com/lastQuake/{source}/{float(mag)}'
        data = requests.get(api_url)
        if data.json()['num_quakes'] > 0:
            quakes.append(data.json()['message'])

    if len(quakes) > 0:
        title = f'Last Quake over {mag}'
        data = quakes[0] if quakes[0]['time'] > quakes[1]['time'] else quakes[1]
        df = pd.DataFrame(data, index=[0])
        df['color'] = df['Oceanic'].apply(lambda x: 'yellow' if x != x else 'blue')
        data, layout = loaded_fig(df)
    else:
        title = f'No Quakes Over {mag} in either USGS or EMSC'
        data, layout = empty_fig()

    fig = go.Figure(data=data, layout=layout)
    fig.update_layout(mapbox_style='stamen-terrain', height=700, title=title)
    return dcc.Graph(figure=fig)


def single_source(value, mag, source):
    if value == 'Quake':
        api_url = f'https://quake-ds-staging.herokuapp.com/last{value}/{source}/{float(mag)}'
    else:
        api_url = f'https://quake-ds-staging.herokuapp.com/last/{source}/{value}/{float(mag)}'
    data = requests.get(api_url)
    if data.json()['num_quakes'] != 0:
        df = pd.DataFrame(data.json()['message']) if value != 'Quake' else \
            pd.DataFrame(data.json()['message'], index=[0])
        df['color'] = 'blue' if source == 'USGS' else 'yellow'
        data, layout = loaded_fig(df)
        if value == 'Quake':
            title = f'Last Quake over {mag} in {source}'
        else:
            title = f"Quakes over {mag} in the last {value.strip('last/')} in {source}"

    else:
        data, layout = empty_fig()
        if value == 'Quake':
            title = f'No Quakes over {mag} to display in {source}'
        else:
            title = f"No Quakes over {mag} in the last {value.strip('last/')} to display in {source}"

    fig = go.Figure(data=data, layout=layout)
    fig.update_layout(mapbox_style='stamen-terrain', height=700, title=title)
    return dcc.Graph(figure=fig)


def loaded_fig(df):
    df['lat'] = df['lat'].apply(lambda x: str(x))
    df['lon'] = df['lon'].apply(lambda x: str(x))
    data = [
        go.Scattermapbox(
            lat=df['lat'],
            lon=df['lon'],
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=df['mag'] + 9,
                color=df['color'],
            ),

            text=[f"""place: {x['place']}<br>UTC time: {datetime.datetime.fromtimestamp(x['time']/1000.0)}<br>mag: {x['mag']}"""
                  for _, x in df.iterrows()],
            hoverinfo='text'
        )
    ]

    layout = go.Layout(
        autosize=True,
        hovermode='closest',
        mapbox=go.layout.Mapbox(
            bearing=0,
            center=go.layout.mapbox.Center(
                lat=0,
                lon=0
            ),
            pitch=0,
            zoom=.5
        ),
    )
    return data, layout


def empty_fig():
    data = [
        go.Scattermapbox(
            lat=[0],
            lon=[0],
            mode='text',
            textposition='middle center'
        )
    ]

    layout = go.Layout(
        autosize=True,
        mapbox=go.layout.Mapbox(
            bearing=0,
            center=go.layout.mapbox.Center(
                lat=0,
                lon=0
            ),
            pitch=0,
            zoom=.5
        ),
    )
    return data, layout


column2 = dbc.Col([html.Div(
    id='wheretheDataGoes')
])

layout = dbc.Row([column1, column2], style={'margin-top': 100, 'height': 1000})
