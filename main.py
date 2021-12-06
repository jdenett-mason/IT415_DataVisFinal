#import pandas, plotly, dash, dash_bootstrap and json
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from dash.dependencies import Input, Output, State
import json

#create the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
load_figure_template("bootstrap")

#import counties geocode json for maps and building a county list
geocode_counties = open('data/geocode-fl-counties-fips.json')
counties = json.load(geocode_counties)
geocode_counties.close()

#import most recent week of housing data from Redfin source
df = pd.read_csv('data/weekly_housing_market_data_with_fips-FL_counties_ending10-31-21.csv', dtype={"fips": str})

#import entire list of housing data from Redfin source
df1 = pd.read_csv('data/weekly_housing_market_data_with_fips-FL_counties.csv', dtype={"fips": str})

#remove columns from df1 to create df2
df2 = df1.drop(columns=['region_id','region_type_id','region_type','period_end','duration','total_homes_sold','total_homes_sold_yoy',
               'average_homes_sold','average_homes_sold_yoy','total_homes_sold_with_price_drops','total_homes_sold_with_price_drops_yoy',
               'average_homes_sold_with_price_drops','average_homes_sold_with_price_drops_yoy','percent_homes_sold_with_price_drops',
               'percent_homes_sold_with_price_drops_yoy','median_sale_price_yoy','median_sale_ppsf','median_sale_ppsf_yoy',
               'median_days_to_close','median_days_to_close_yoy','price_drops','price_drops_yoy','percent_active_listings_with_price_drops',
               'percent_active_listings_with_price_drops_yoy','pending_sales','pending_sales_yoy','median_pending_sqft',
               'median_pending_sqft_yoy','off_market_in_two_weeks','off_market_in_two_weeks_yoy','off_market_in_one_week',
               'off_market_in_one_week_yoy','percent_off_market_in_two_weeks','percent_off_market_in_two_weeks_yoy','percent_off_market_in_one_week',
               'percent_off_market_in_one_week_yoy','total_new_listings','total_new_listings_yoy','average_new_listings',
               'average_new_listings_yoy','median_new_listing_price','median_new_listing_price_yoy','median_new_listing_ppsf',
               'median_new_listing_ppsf_yoy','inventory','inventory_yoy','total_active_listings','total_active_listings_yoy',
               'active_listings','active_listings_yoy','age_of_inventory','age_of_inventory_yoy','homes_delisted','homes_delisted_yoy',
               'percent_active_listings_delisted','percent_active_listings_delisted_yoy','median_active_list_price','median_active_list_price_yoy',
               'median_active_list_ppsf','median_active_list_ppsf_yoy','average_of_median_list_price_amount','average_of_median_list_price_amount_yoy',
               'average_of_median_offer_price_amount','average_of_median_offer_price_amount_yoy','avg_offer_to_list','avg_offer_to_list_yoy',
               'average_sale_to_list_ratio','average_sale_to_list_ratio_yoy','median_days_on_market','median_days_on_market_yoy',
               'pending_sales_to_sales_ratio','pending_sales_to_sales_ratio_yoy','months_of_supply','months_of_supply_yoy',
               'average_pending_sales_listing_updates','average_pending_sales_listing_updates_yoy','percent_total_price_drops_of_inventory',
               'percent_total_price_drops_of_inventory_yoy','percent_homes_sold_above_list','percent_homes_sold_above_list_yoy',
               'price_drop_percent_of_old_list_price','price_drop_percent_of_old_list_price_yoy','last_updated','average_adjustment_average_homes_delisted',
               'adjusted_average_homes_delisted','average_adjustment_average_homes_sold','adjusted_average_homes_sold',
               'average_adjustment_average_new_listings','adjusted_average_new_listings','average_adjustment_pending_sales',
               'adjusted_pending_sales','adjusted_average_homes_delisted_yoy','adjusted_average_homes_sold_yoy','adjusted_average_new_listings_yoy',
               'adjusted_pending_sales_yoy'])
#clean up df2 for datatypes, filter and group for for september and october average numbers and merge them into one dataframe
df2['period_begin'] = pd.to_datetime(df2['period_begin'], format='%Y-%m-%d')
df2['median_sale_price'] = pd.to_numeric(df2['median_sale_price'])
df2_sep = df2[(df2['period_begin'] >= '2021-09-01') & (df2['period_begin'] < '2021-10-01')].groupby(['fips','region_name'])['median_sale_price'].mean().to_frame('average_sale_price').reset_index()
df2_oct = df2[(df2['period_begin'] >= '2021-10-01') & (df2['period_begin'] < '2021-11-01')].groupby(['fips','region_name'])['median_sale_price'].mean().to_frame('average_sale_price').reset_index()
df2_merge = pd.merge(df2_sep, df2_oct, how='inner', on=['region_name', 'fips'])
df2_merge['average_price_mom'] = df2_merge.apply(lambda row: (row.average_sale_price_y - row.average_sale_price_x) / row.average_sale_price_x , axis=1)
df2_max = df2_merge.average_price_mom.max()
df2_min = df2_merge.average_price_mom.min()

#import data from Realtor.com dataset, clean up for datatypes, filter and group for for september and october sums and merge them into one dataframe
df3 = pd.read_csv('data/RDC_Inventory_Core_Metrics_Zip_History_FL.csv', usecols= ['month_date_yyyymm', 'county_fips', 'region_name', 'price_increased_count', 'price_reduced_count'])
df3['month_date_yyyymm'] = pd.to_datetime(df3['month_date_yyyymm'], format='%Y%m')
df3_fil = df3[(df3['month_date_yyyymm'] == '2021-10-01')]
df3_inc = df3_fil.drop(columns=['month_date_yyyymm', 'county_fips', 'price_reduced_count']).groupby(['region_name'])['price_increased_count'].sum().to_frame('total_increased_count').reset_index()
df3_red = df3_fil.drop(columns=['month_date_yyyymm', 'county_fips', 'price_increased_count']).groupby(['region_name'])['price_reduced_count'].sum().to_frame('total_reduced_count').reset_index()
df3_merge = pd.merge(df3_inc, df3_red, how='inner', on='region_name')

#import Apartment rental data from Zillow
df4 = pd.read_csv('data/Metro_ZORI_AllHomesPlusMultifamily_SSA_florida_transposed.csv', dtype={"fips": str})

#build county name dropdown component
county_names = list()
county_name_dropdown_options = list()
for county in counties['features']:
    county_names.append(county['properties']['NAME'])
county_names.sort()
for cn in county_names:
    county_name_dropdown_options.append({"label": cn, "value": cn})

county_dropdown = dcc.Dropdown(id="select_county", options=county_name_dropdown_options,
                              multi=True, placeholder="Select county")

#Build metro area list for average apartment rental prices
florida_metros=['Miami-Fort Lauderdale, FL','Tampa, FL','Orlando, FL','Jacksonville, FL', 'North Port-Sarasota-Bradenton, FL',
                'Fort Myers, FL','Lakeland, FL','Daytona Beach, FL','Melbourne, FL','Port St. Lucie, FL']

#Build Median Sale Price Chropleth map visualization
fig = px.choropleth(df, geojson=counties,
                       locations='fips', color='median_sale_price',
                       color_continuous_scale="Viridis",
                       range_color=(100000, 750000),
                       basemap_visible=False,
                       labels={'fips': 'FIPS code', 'median_sale_price':'Median Sales Price', 'region_name' : 'County'},
                       hover_data={'region_name', 'median_sale_price'},
                       center={'lat': 27.6648, 'lon': -81.5158},
                       fitbounds='geojson',
                       )

fig.update_layout(margin={"r":0,"t":50,"l":0,"b":50}, title="Weekly Average Home Price by County - Week of 31Oct21",
                  paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                  font_color="#fff", plot_bgcolor="#222222", geo_bgcolor="#222222")

#Build weekly homes shold line chart visualization
fig1 = px.line(df1, x='period_begin', y='total_homes_sold', color="region_name",
            title='Florida Weekly Homes Sold 2017-2021', labels={}, markers=False)

fig1.update_layout(paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                  font_color="#fff", plot_bgcolor="#222222")

#Build Weekly Average Sale Price lone chart visualization
fig2 = px.line(df1, x='period_begin', y='median_sale_price', markers=False, color="region_name",
            title='Florida Weekly Average Home Prices 2017-2021', labels={})

fig2.update_layout(paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                  font_color="#fff", plot_bgcolor="#222222")

#Build Average MOM Price Change Chropleth map visualization
fig3 = px.choropleth(df2_merge, geojson=counties,
                       locations='fips', color='average_price_mom',
                       color_continuous_scale="RdYlGn",
                       color_continuous_midpoint= 0,
                       range_color=(df2_min/3, df2_max/3),
                       basemap_visible=False,
                       labels={'average_price_mom':'Average MOM Price Change', 'region_name' :'County'},
                       hover_data={'region_name', 'average_price_mom'},
                       center={'lat': 27.6648, 'lon': -81.5158},
                       fitbounds='geojson',
                       )

fig3.update_layout(margin={"r":0,"t":50,"l":0,"b":50}, title="Month over Month Average Home Price Change by County Sep-Oct 2021",
                  paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                  font_color="#fff", plot_bgcolor="#222222", geo_bgcolor="#222222")

#Build Median Days on Market bar chart visualization
fig4 = px.bar(df, x = 'region_name', y = 'median_days_on_market', color = "region_name",
              title = 'Florida Weekly Homes Sold - Median Days on Market - Week of 31Oct21',
              labels = {'region_name': 'County', 'median_days_on_market': 'Median Days on Market'}, barmode='stack')

fig4.update_layout(paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                  font_color="#fff", plot_bgcolor="#222222")

#Build Median Days to Close bar chart visualization
fig5 = px.bar(df, x = 'region_name', y = 'median_days_to_close', color = "region_name",
              title = 'Florida Weekly Homes Sold - Median Days to Close - Week of 31Oct21',
              labels = {'region_name': 'County', 'median_days_to_close': 'Median Days to Close'}, barmode='stack')

fig5.update_layout(paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                  font_color="#fff", plot_bgcolor="#222222")

#Build Price Increase and Decrease Relative Bar Chart visualization
fig6 = go.Figure()
fig6.add_trace(go.Bar(x=df3_merge['region_name'], y=df3_merge['total_increased_count'], marker_color='Green', name='Price Increases'))
fig6.add_trace(go.Bar(x=df3_merge['region_name'], y=(df3_merge['total_reduced_count'] * -1), marker_color='Red', name='Price Reductions'))

fig6.update_layout(barmode = 'relative', title_text = 'Home Inventory - Price Increases / Decreases - Month of October 2021',
                   paper_bgcolor = "#222222", title_font_color = "#fff", legend_font_color = "#fff", legend_title_font_color = "#fff",
                   font_color = "#fff", plot_bgcolor = "#222222")

#Build Metro Average Rent Price line chart visualization
fig7 = px.line(df4, x='date', y= florida_metros, markers=False,
            title='Florida Metro Area Rent 2017-2021', labels={'date': 'Date', 'florida_metros': 'Metro'})

fig7.update_layout(margin={"r":0,"t":50,"l":0,"b":50}, yaxis_title="Dollars / Month", legend_title="Florida Metro Area",
                   paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                   font_color="#fff", plot_bgcolor="#222222")

#place the app container for html and bind the visualizations to a dashboard webpage
app.layout = dbc.Container(
    [
        html.H2("Florida Real Estate Markets", className="bg-primary text-white p-1"),
        dbc.Row(
            [
                dbc.Col([html.Div(county_dropdown)], width={'size': 1}, lg={'size':1}),
                dbc.Col(dcc.Graph(id='fig_graph', figure=fig), width={'size': 5}, lg={'size':5}),
                dbc.Col(dcc.Graph(id='fig1_graph', figure=fig1), width={'size': 6}, lg={'size':6}),
            ],
        ),
        dbc.Row(
            [
                dbc.Col( width={'size': 1}, lg={'size':1}),
                dbc.Col(dcc.Graph(id='fig2_graph', figure=fig2), width={'size': 5}, lg={'size':5}),
                dbc.Col(dcc.Graph(id='fig3_graph', figure=fig3), width={'size': 6}, lg={'size':6}),
            ],

        ),
        dbc.Row(
            [
                dbc.Col( width={'size': 1}, lg={'size':1}),
                dbc.Col(dcc.Graph(id='fig4_graph', figure=fig4), width={'size': 5}, lg={'size':5}),
                dbc.Col(dcc.Graph(id='fig5_graph', figure=fig5), width={'size': 6}, lg={'size':6}),
            ],

        ),
        dbc.Row(
            [
                dbc.Col( width={'size': 1}, lg={'size':1}),
                dbc.Col(dcc.Graph(id='fig6_graph', figure=fig6), width={'size': 5}, lg={'size':5}),
                dbc.Col(dcc.Graph(id='fig7_graph', figure=fig7), width={'size': 6}, lg={'size':6}),
            ],

        ),

    ],
    fluid=True,
)

##Build callbacks that update several of the visualizations when triggered by the county dropdown
@app.callback(
    Output('fig1_graph', 'figure'),
    Output('fig2_graph', 'figure'),
    Output('fig4_graph', 'figure'),
    Output('fig5_graph', 'figure'),
    Output('fig6_graph', 'figure'),
    [Input('select_county', 'value')])
def update_figure(county_name):
    dd_counties = list()

    if county_name is not None:
        for cn in county_name:
            dd_counties.append(cn + " County, FL")
    else:
        dd_counties = df1['region_name'].unique()

    bs1 = df1.region_name.isin(dd_counties)
    fdf1 = df1[bs1]
    bs = df.region_name.isin(dd_counties)
    fdf = df[bs]
    bs2 = df3_merge.region_name.isin(dd_counties)
    fdf2 = df3_merge[bs2]

    f1 = px.line(fdf1, x='period_begin', y='median_sale_price', markers=False, color="region_name",
                title='Florida Weekly Average Home Prices 2017-2021', labels={})
    f1.update_layout(paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                       font_color="#fff", plot_bgcolor="#222222", transition_duration=500)

    f = px.line(fdf1, x='period_begin', y='total_homes_sold', color="region_name",
            title='Florida Weekly Homes Sold 2017-2021', labels={}, markers=False,)
    f.update_layout(paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                       font_color="#fff", plot_bgcolor="#222222", transition_duration=500)

    f2 = px.bar(fdf, x='region_name', y='median_days_on_market', color="region_name",
                title='Florida Weekly Homes Sold 2017-2021 - Median Days on Market', labels={'region_name': 'County', 'median_days_on_market': 'Median Days on Market'}, barmode='stack')
    f2.update_layout(paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff",
                     legend_title_font_color="#fff",
                     font_color="#fff", plot_bgcolor="#222222", transition_duration=500)

    f3 = px.bar(fdf, x = 'region_name', y = 'median_days_to_close', color = "region_name",
            title = 'Florida Weekly Homes Sold 2017-2021 - Median Days to Close', labels = {'region_name': 'County', 'median_days_to_close': 'Median Days to Close'}, barmode='stack')
    f3.update_layout(paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                       font_color="#fff", plot_bgcolor="#222222", transition_duration=500)

    f4 = go.Figure()
    f4.add_trace(go.Bar(x=fdf2['region_name'], y=fdf2['total_increased_count'], marker_color='Green', name='Price Increases'))
    f4.add_trace(go.Bar(x=fdf2['region_name'], y=(fdf2['total_reduced_count'] * -1), marker_color='Red', name='Price Reductions'))
    f4.update_layout(barmode='relative',title_text='Home Inventory - Price Increases / Decreases - Month of October 2021',
                     paper_bgcolor="#222222", title_font_color="#fff", legend_font_color="#fff", legend_title_font_color="#fff",
                     font_color="#fff", plot_bgcolor="#222222", transition_duration=500)

    return f, f1, f2, f3, f4

if __name__ == "__main__":
    app.run_server(debug=True)