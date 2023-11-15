from dash import Dash, dcc, html, callback, Input, Output, ctx
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
import plotly.io as pio
import data_manip as dm
import pandas as pd
import json
from datetime import datetime, timedelta
import locale
from functools import cmp_to_key
import logging
import logging_config
from typing import Iterable

logging_config.config()
logger = logging.getLogger(__name__)

locale.setlocale(locale.LC_ALL, 'cs_CZ')


def sorted_locale(x: Iterable[str]) -> list[str]:
    """
    Locale-aware sorted() function.

    Args:
        x: data to be sorted

    Returns:
        Data sorted according to current locale.
    """
    return sorted(x, key=cmp_to_key(locale.strcoll))


# find the platform-specific code for day of month without leading zero
try:
    x = datetime.today().strftime('%#d')
    day_of_month_code = '%#d'
except ValueError:
    day_of_month_code = '%-d'


def hex_to_rgb(color: str, alpha: float = None) -> str:
    """
    Translates a hex color representation to rgb (rgba when alpha is given).

    Examples:
        >>> hex_to_rgb('#ff00ff')
        'rgb(255, 0, 255)'

        >>> hex_to_rgb('#0000ff', .5)
        'rgba(0, 0, 255, 0.5)'

    Args:
        color: color in hex format ('#aabb00')
        alpha: transparency (0-1)

    Returns:
        rgb/rgba string representation of the color.
    """

    # hex to comma-separated integers, e.g., '#ff00ff' -> '255, 0, 255'
    rgb = ', '.join([str(int(color[i:i + 2], 16)) for i in [1, 3, 5]])
    if alpha is None:
        return f'rgb({rgb})'
    else:
        return f'rgba({rgb}, {str(alpha)})'


def date_marks(start: datetime, end: datetime) -> tuple[pd.DatetimeIndex, list[str]]:
    """
    Creates marks for a range slider.

    Args:
        start: start date
        end: end date

    Returns:

    """
    # find the highest frequency with less than 10 timepoints
    freqs = ['D', '2D', '3D', 'W', '2W', 'MS', '2MS', '3MS', '6MS', 'YS', '2YS']
    for freq in freqs:
        timepoints = pd.date_range(start, end, freq=freq)
        if len(timepoints) < 8:
            break
    else:
        timepoints = pd.date_range(start, end, periods=2)

    # keep only first occurrence of each year
    years = list(map(str, timepoints.year))
    yr = ''
    for i in range(len(years)):
        if years[i] == yr:
            years[i] = ''
        else:
            yr = years[i]
    days = timepoints.strftime(f'{day_of_month_code}. %b')
    labels = [f'{d} {y}' for y, d in zip(years, days)]

    return timepoints, labels


trace_colors = colors = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2',
    '#7f7f7f', '#bcbd22', '#17becf', '#9edae5', '#c5b0d5', '#ff9896', '#c49c94',
    '#f7b6d2', '#aec7e8', '#ffbb78', '#98df8a']
date_picker_format = 'Y-MM-DD'
topo_colorscale = [(0, '#3c855e'), (.4, '#ffd39e'), (1, '#88594d')]

station_names_translator = dm.get_station_name_translator()

stations_data = dm.get_stations_data(station_names_translator)
# create options for the stations dropdown menu
stations_list = sorted_locale(stations_data['name'])

daily_data = dm.get_daily_precipitation(station_names_translator)
date_range = daily_data['date'].min(), daily_data['date'].max()
# date_range = datetime.fromisoformat('2020-01-01'), datetime.fromisoformat('2025-01-01')
dates_cut, slider_m = date_marks(*date_range)
slider_at = (dates_cut - date_range[0]).days
slider_len = (date_range[1] - date_range[0]).days

#region Scattermapbox definition
fig_map = go.Figure()

# data shared by both Scattermapbox traces
station_markers = dict(
    lat=stations_data['lat'],
    lon=stations_data['lon'],
    mode='markers',
    showlegend=False
)

# bottom markers layer -- selected stations
fig_map.add_trace(
    go.Scattermapbox(
        name="selected_stations",
        lat=[],
        lon=[],
        marker=dict(
            size=40
        ),
        showlegend=False
    )
)

# middle markers layer -- inner marker border
fig_map.add_trace(
    go.Scattermapbox(
        **station_markers,
        marker=dict(
            size=20,
            color='white'
        )
    )
)

# upper markers layer -- inside color
fig_map.add_trace(
    go.Scattermapbox(
        **station_markers,
        marker=dict(
            size=16,
            color=stations_data['elevation'],
            showscale=True,
            colorscale=topo_colorscale,
            cmin=20,
            cmax=1603,
            colorbar=dict(
                title="Nadmořská<br>výška (m)",
                lenmode="pixels", len=200,
                xanchor='left', x=0,
                yanchor="bottom", y=0,
                bgcolor='rgba(255, 255, 255, .7)'
            )
        ),
        hovertemplate='<b>%{customdata[0]}</b><br>'
                      '%{customdata[1]} m n. m.<extra></extra>',
        customdata=stations_data.loc[:,
                   ['name', 'elevation', 'type']]
    )
)

fig_map.update_layout(
    mapbox=dict(
        style='open-street-map',
        center=dict(
            lat=stations_data['lat'].mean(),
            lon=stations_data['lon'].mean()
        ),
        zoom=6.5
    ),
    margin=dict(r=0, t=0, l=0, b=0),
    coloraxis_colorbar=dict(
        title="Elevation (m)",
        xanchor='left', x=.5,
        yanchor="bottom", y=.5,
        bgcolor='rgba(255, 255, 255, .7)'
    ),
    autosize=True,
)
#endregion


def blank_fig():
    fig = go.Figure(go.Scatter(
        x=[1], y=[1],
        text='<i>vyberte stanici</i>',
        mode='text',
        hoverinfo='skip'
    ))
    fig.update_layout(template=None,
                      font=dict(
                          size=18,
                          color='#aaaaaa'
                      ))
    fig.update_xaxes(showgrid=False, showticklabels=False, zeroline=False)
    fig.update_yaxes(showgrid=False, showticklabels=False, zeroline=False)

    return fig


def get_radio_options(disabled: bool = False):
    return [
        {'value': 'sum', 'label': 'Suma srážek (mm)', 'disabled': disabled},
        {'value': 'mean', 'label': 'Denní průměr srážek (mm)', 'disabled': disabled},
        {'value': 'var', 'label': 'Variabilita', 'disabled': disabled}
    ]


pio.templates.default = 'plotly_white'
app = Dash(__name__)
app.title = 'Srážky v ČR'

app.layout = html.Div([
    html.Div([  # left container
        html.Div([  # map
            dcc.Graph(
                id='map',
                figure=fig_map,
                style={'height': '100%'}
            )],
            style={'position': 'absolute',
                   'width': '100%',
                   'height': '100%'}
        ),
        html.Div([  # stations dropdown
            dcc.Dropdown(
                id='dropdown-stations',
                options=stations_list,
                multi=True,
                value=[],
                placeholder='Vyberte stanice zde nebo kliknutím v mapě',
                style={'marginBottom': '5px'}
            )],
            style={'position': 'absolute',
                   'width': '100%'}
        )],
        id='left-pane'),
    html.Div([  # right container
        html.Div([  # date slider and pickers
            html.Div([
                dcc.DatePickerSingle(
                    id='date-picker-from',
                    min_date_allowed=date_range[0],
                    max_date_allowed=date_range[1],
                    date=date_range[0],
                    display_format=date_picker_format
                )
            ]),
            html.Div([
                dcc.RangeSlider(
                    id='date-slider',
                    min=0,
                    max=slider_len,
                    step=1,
                    value=[0, slider_len],
                    marks=dict(zip(slider_at, slider_m))
                )],
                style={'width': '93%',
                       'marginTop': '8px'}
            ),
            html.Div([
                dcc.DatePickerSingle(
                    id='date-picker-to',
                    min_date_allowed=date_range[0],
                    max_date_allowed=date_range[1],
                    date=date_range[1],
                    display_format=date_picker_format
                )],
            )],
            style={'display': 'flex',
                   'justifyContent': 'space-between'}
        ),
        html.H2('Denní úhrn srážek (mm)'),
        html.Div([
            dcc.Graph(
                id='scatterplot',
                figure=blank_fig(),
                config=dict(locale='cs',
                            displayModeBar=False),
                style={'height': '100%'}
            )],
            style={'flex': 1}
        ),
        html.H2('Sumarizace za vybrané období'),
        html.Div([
            dcc.RadioItems(get_radio_options(disabled=True),
                value='sum',
                id='summary-radios',
                inline=True,
                labelStyle={'marginLeft': '20px',
                            'fontSize': 'small'})
            ],
            style={'display': 'flex',
                   'justifyContent': 'flex-end'}
        ),
        html.Div([
            dcc.Graph(
                id='barplot',
                figure=blank_fig(),
                config=dict(locale='cs',
                            displayModeBar=False),
                style={'height': '100%'}
            )],
            style={'flex': 1}
        )
    ],
        id='right-pane'
        ),
    # list of available colors
    dcc.Store(
        id='colors-available',
        data=json.dumps(trace_colors)),
    # dictionary of already used colors with station names as keys
    dcc.Store(
        id='colors-displayed',
        data='{}')
],
    id='main'
)


@callback(
    Output('date-picker-from', 'date'),
    Output('date-picker-to', 'date'),
    Output('date-slider', 'value'),
    Input('date-picker-from', 'date'),
    Input('date-picker-to', 'date'),
    Input('date-slider', 'value'))
def sync_slider_picker_dates(date_picker_from, date_picker_to, slider_value):

    if ctx.triggered_id == 'date-slider':
        date_picker_from = date_range[0] + timedelta(days=slider_value[0])
        date_picker_to = date_range[0] + timedelta(days=slider_value[1])

    elif ctx.triggered_id == 'date-picker-from':
        slider_value[0] = (datetime.fromisoformat(date_picker_from) - date_range[0]).days
    else:
        slider_value[1] = (datetime.fromisoformat(date_picker_to) - date_range[0]).days

    return date_picker_from, date_picker_to, slider_value


@callback(
    Output('colors-displayed', 'data'),
    Output('colors-available', 'data'),
    Input('colors-displayed', 'data'),
    Input('colors-available', 'data'),
    Input('dropdown-stations', 'value'))
def update_station_colors(displayed: str, available: str,
                          selected: list[str]) -> tuple[str, str]:
    """
    Updates the lists of displayed and available colors after a change in
    the stations dropdown menu.

    Args:
        displayed: colors of displayed stations
        available: available colors
        selected: stations selected in the dropdown

    Returns:
        Displayed colors.
        Available colors.
    """
    if selected is None:
        raise PreventUpdate

    available = json.loads(available)
    displayed = json.loads(displayed)
    new = [s for s in selected if s not in displayed]
    dropped = [s for s in displayed.keys() if s not in selected]

    if new:
        # add (station, color) pairs for new stations
        displayed |= dict(zip(new, available[:len(new)]))
        # remove assigned colors from available
        available = available[len(new):]

    if dropped:
        # return dropped stations' colors to available
        available = [displayed.pop(d) for d in dropped] + available

    return json.dumps(displayed), json.dumps(available)


@callback(
    Output('map', 'figure'),
    Input('colors-displayed', 'data'),
    Input('map', 'figure'))
def update_map_highlighted_stations(displayed: str, map: go.Figure) -> go.Figure:
    """
    Updates stations highlighted in the map according to 'colors-displayed'.

    Args:
        displayed: colors of displayed stations
        map: map figure

    Returns:
        Updated map figure.
    """
    displayed = json.loads(displayed)

    # highlight selected stations in the map
    latlon = stations_data.loc[displayed.keys(), ['lat', 'lon']]
    map = go.Figure(map)
    map.update_traces(
        lat=latlon['lat'],
        lon=latlon['lon'],
        marker=dict(
            color=list(displayed.values())
        ),
        selector=dict(name='selected_stations')
    )

    return map


@callback(
    Output('scatterplot', 'figure'),
    Output('barplot', 'figure'),
    Output('summary-radios', 'options'),
    Input('colors-displayed', 'data'),
    Input('date-slider', 'value'),
    Input('summary-radios', 'value'))
def draw_station_plots(displayed: str,
                       slider: list[int, int],
                       agg_fun: str) -> tuple[go.Figure, go.Figure]:
    """
    Draws plots for currently selected stations.

    Args:
        displayed: colors of displayed stations.
        slider: slider selected endpoints
        agg_fun: function used for aggregation by stations

    Returns:
        A tuple containing:
            Scatterplot figure,
            Barplot figure
    """
    displayed = json.loads(displayed)

    if not displayed:
        return blank_fig(), blank_fig(), get_radio_options(disabled=True)

    date_display_range = pd.date_range(
        date_range[0] + timedelta(days=slider[0]),
        date_range[0] + timedelta(days=slider[1]))
    df = daily_data.loc[
        daily_data.index.get_level_values('station_idx').isin(displayed.keys()) &
        daily_data.index.get_level_values('date_idx').isin(date_display_range)]

    # Build scatterplot
    scatter = go.Figure()

    # markers for values of 0 will not be shown
    df['m_size'] = [0 if a == 0 else 7 for a in df['amount']]

    stations = sorted_locale(displayed.keys())

    # add a line and marker traces for each station
    for station in stations:
        df_station = df.loc[df.index.get_level_values('station_idx') == station]
        fig_data = dict(
            x=df_station['date'],
            y=df_station['amount'])
        # lines
        scatter.add_trace(go.Scatter(
            **fig_data,
            mode='lines',
            name=station,
            line=dict(
                color=displayed[station]
            )))
        # markers
        scatter.add_trace(go.Scatter(**fig_data,
            mode='markers',
            marker=dict(
                size=df_station['m_size'],
                color=displayed[station]
            ),
            showlegend=False))
    scatter.update_xaxes(tickformat='%-d. %b\n%Y')  # unlike Windows, Dash uses '%-d'
    scatter.update_layout(margin=dict(l=50, r=50, b=50, t=0))

    # Build barplot

    agg = df.groupby('station', observed=True, as_index=True).agg({'amount': agg_fun})
    agg = agg.loc[stations]
    agg.reset_index(inplace=True)

    barplot = go.Figure()
    barplot.add_trace(go.Bar(
        x=agg['station'],
        y=agg['amount'],
        marker_color=[hex_to_rgb(displayed[s], alpha=.5) for s in agg['station']],
        marker_line_color=[displayed[s] for s in agg['station']],
        marker_line_width=3
    ))
    barplot.update_layout(margin=dict(l=50, r=50, b=50, t=0))
    # make bars narrower
    #if len(displayed) < 15:
    barplot.update_traces(width=.05 + (len(displayed)-1)*.05)

    return scatter, barplot, get_radio_options()


@callback(
    Output('dropdown-stations', 'value'),
    Output('map', 'clickData'),
    Input('dropdown-stations', 'value'),
    Input('map', 'clickData'))
def map_station_clicked(selected, click_map):
    """
    Updates selected stations after a station is clicked in the map.

    Args:
        selected: stations selected in the dropdown
        click_map: click data from 'map'

    Returns:
        Updated stations dropdown value.
        Cleared 'map's click data
    """

    # only react to clicks on 'map'
    if ctx.triggered_id != 'map':
        raise PreventUpdate

    # remove clicked station from selected when present, add otherwise
    station = click_map['points'][0]['customdata'][0]
    try:
        selected.remove(station)
    except ValueError:
        selected.append(station)

    # sets 'map's 'clickData' to 'None' so that repeated clicks on the same
    #   feature are not ignored
    return selected, None


if __name__ == '__main__':
    app.run(debug=False)
