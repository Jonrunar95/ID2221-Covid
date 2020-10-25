import json
import os, sys
import requests
import urllib.parse
import datetime
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.interpolate import interp1d
#sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'Stream_Service'))
#import CovidAPI
# pip install cassandra-driver
from cassandra.cluster import Cluster
from flask import Flask, jsonify, Response
import pandas as pd
# pip install plotly
import plotly.express as px


app = Flask(__name__)

###
### Data service
###
class Service:
    def __init__(self):
        cluster = Cluster()
        self.session = cluster.connect("covid")
        #self.covidApi = CovidAPI.API()

    def query(self, statement, value):
        prep = self.session.prepare(statement)
        result = self.session.execute(prep, value)
        return result

    # Get info on single country
    # country: String with a capital letter
    def getCountryInfo(self, country):
        prep = self.session.prepare("SELECT * FROM countryinfo WHERE country=?")
        result = self.session.execute(prep, [country])
        return result

    # Get info on all available countries
    def getAllCountryInfo(self):
        result = self.session.execute("SELECT * from countryinfo")
        return result
    
    def getCountryCases(self, country, json=False):
        prep = self.session.prepare("SELECT * FROM countrycases WHERE country=?")
        if json:
            prep = self.session.prepare("SELECT JSON * FROM countrycases WHERE country=?")
        result = self.session.execute(prep, [country])
        return result

    def getAllProvinceCases(self):
        result = self.session.execute("SELECT * FROM ProvinceCases")
        return result

###
### Backend service
###
@app.route('/frontpage', methods=['GET'])
def frontpage():
    result = srv.getAllCountryInfo()
    countryJson = []
    for row in result:
        countryJson.append({"Country": row.country, "Date": row.date, "NewConfirmed": row.newconfirmed, "NewDeaths": row.newdeaths,
                                "TotalConfirmed": row.totalconfirmed, "TotalDeaths": row.totaldeaths, "Population": row.population,
                                "DeathRate": row.deathrate})
    return jsonify({"data": countryJson})

@app.route('/Country/<Country>', methods=['GET'])
def country(Country):
    
    info = srv.getCountryInfo("Iceland")
    population = info.one().population

    result = srv.getCountryCases(Country)
    countryJson = []
    for row in result:
        countryJson.append({"Date": row.date, "Active": row.active, "Confirmed": row.confirmed, "ConfirmedToday": row.confirmedtoday, "ConfirmedLast14": row.confirmedlast14, "Deaths": row.deaths,
                                "DeathsToday": row.deathstoday, "DeathsLast14": row.deathlast14, "InfectionRate": (row.confirmedlast14/population)*100000})
    return jsonify({"data": countryJson})


@app.route('/Country/<Country>/plot_DailyCases.png')
def plot_DailyCases_png(Country):
    fig = create_dailyCasesfigure(Country)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

@app.route('/Country/<Country>/plot_InfectionRate.png')
def plot_InfectionRate_png(Country):
    fig = create_infectionRatefigure(Country)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

@app.route('/Country/<Country>/plot_InfectionTrend.png')
def plot_InfectionTrend_png(Country):
    fig = create_infectionTrendfigure(Country)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

@app.route('/heatmap/<weeknumber>/<casetype>')
def createHeatmap(weeknumber, casetype):   
    weeknumber = int(weeknumber)

    # Get Data and create pandas dataframe
    data = srv.getAllProvinceCases()
    df = pd.DataFrame(list(data))
    
    # Convert columns to suitable type
    covert_dict = {'lat': float, 'lon': float, 'week': int}
    df1 = df.astype(covert_dict)

    minCase = 0
    maxCase = 0 

    # Use casetype values as reference for heatmap
    if casetype == "confirmed":
        minCase = min(df1.confirmed.values.tolist())
        maxCase = max(df1.confirmed.values.tolist())
    if casetype == "active":
        minCase = min(df1.active.values.tolist())
        maxCase = max(df1.active.values.tolist())
    if casetype == "death":
        minCase = min(df1.death.values.tolist())
        maxCase = max(df1.death.values.tolist())

    # Filter data by weeknumber
    df1 = df.loc[((df['week'] == weeknumber))]
    midp=None

    # Use casetype values as reference for heatmap
    if casetype == "confirmed":
        case_list = df1.confirmed.values.tolist()
        minCase = min(case_list)
        maxCase = max(case_list)
    if casetype == "active":
        midp = 0
        case_list = df1.active.values.tolist()
        #minCase = min(case_list)
        #maxCase = max(case_list)
    if casetype == "death":
        case_list = df1.death.values.tolist()
        minCase = min(case_list)
        maxCase = max(case_list)

    print(casetype, "", maxCase)

    # Interpolate according to highest case value
    #interp = interp1d([min(case_list), max(case_list)], [2,20])
    interp = interp1d([minCase, maxCase], [1,20])
    circleRad = interp(case_list)
    circleRad = 10

    # Create figure
    fig = px.density_mapbox(df1, z=casetype, lat='lat', lon='lon', radius=circleRad, zoom=0, mapbox_style='open-street-map', color_continuous_midpoint=midp)
    #fig.show()
    #fig.write_image("figTest.png")

    img_bytes = fig.to_image(format="png")
    
    return Response(img_bytes, mimetype='image/png')



##
## Plotting functions
##
def create_dailyCasesfigure(country):
    result = srv.getCountryCases(country)
    X = []
    y_confirmed = []
    y_active = []
    y_deaths = []
    for row in result:
        date = datetime.datetime.strptime(row.date, "%Y-%m-%d")
        X.append(date)
        y_confirmed.append(row.confirmed)
        y_active.append(row.active)
        y_deaths.append(row.deaths)

    dates = mdates.date2num(X)
    fig = Figure(figsize=(10,5))

    axis = fig.add_subplot(1, 1, 1)

    axis.plot_date(dates, y_confirmed, xdate=True, ls='-', marker=None, alpha=0)
    axis.fill_between(dates, 0, y_confirmed, label="Recovered Cases Ratio")
    axis.fill_between(dates, 0, [x+y for x,y in zip(y_deaths, y_active)], label="Death Cases Ratio", color='black')
    axis.fill_between(dates, 0, y_active, label="Active Cases Ratio")
    
    axis.margins(y=0, x=0) # Use this if we want to remove x axis padding also
    axis.legend(loc=2)
    axis.set_ylim(0, max(y_confirmed)*1.2)
    
    axis.set_ylabel('Cumulative Cases')
    return fig

##
## Plotting functions
##
def create_infectionRatefigure(country):
    result = srv.getCountryCases(country)

    info = srv.getCountryInfo("Iceland")
    population = info.one().population

    X = []
    y_rate = []
    for row in result:
        date = datetime.datetime.strptime(row.date, "%Y-%m-%d")
        X.append(date)
        y_rate.append((row.confirmedlast14/population)*100000)

    dates = mdates.date2num(X)
    fig = Figure(figsize=(10,5))

    axis = fig.add_subplot(1, 1, 1)
    axis.plot_date(dates, y_rate, xdate=True, ls='-', marker=None, label="Infection Rate per 100k")
    axis.set_ylabel('Infection Rate per 100k')

    # axis2 = axis.twinx()
    # color = 'tab:red'
    # axis2.plot_date(dates, y_deaths, xdate=True, ls='-', marker=None, label="Deaths", color=color, alpha=0.6)
    # #axis2.bar(dates, y_deaths, color=color, alpha=0.6)
    # axis2.tick_params(axis='y', labelcolor=color)
    # axis2.set_ylabel('Cumulative Deaths', color=color)
    # axis2.set_ylim(0, 2*max(y_deaths))

    axis.margins(y=0) # Use this if we want to remove x axis padding also
    #axis2.margins(y=0) 
    fig.tight_layout()
    return fig

def create_infectionTrendfigure(country):
    result = srv.getCountryCases(country)
    X = []
    y = []
    for row in result:
        date = datetime.datetime.strptime(row.date, "%Y-%m-%d")
        X.append(date)
        y.append(row.confirmedlast14)

    dates = mdates.date2num(X)
    fig = Figure(figsize=(10,5))
    axis = fig.add_subplot(1, 1, 1)
    axis.plot_date(dates, y, xdate=True, ls='-', marker=None)
    axis.margins(y=0) # Use this if we want to remove x axis padding also
    return fig



if __name__ == "__main__":
    srv = Service()
    app.run()

    #createHeatmap()
    #result = srv.getCountryCases("Iceland")
    #for r in result:
    #    print(r)
