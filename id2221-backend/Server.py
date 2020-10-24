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
import random
#sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'Stream_Service'))
#import CovidAPI
# pip install cassandra-driver
from cassandra.cluster import Cluster
from flask import Flask, jsonify, Response

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

###
### Backend service
###
@app.route('/frontpage', methods=['GET'])
def frontpage():
    result = srv.getAllCountryInfo()
    countryJson = []
    for row in result:
        countryJson.append({"Country": row.country, "Date:": row.date, "NewConfirmed": row.newconfirmed, "NewDeaths": row.newdeaths,
                                "TotalConfirmed": row.totalconfirmed, "TotalDeaths": row.totaldeaths, "Population": row.population,
                                "DeathRate:": row.deathrate})
    return jsonify({"data": countryJson})

@app.route('/Country/<Country>', methods=['GET'])
def country(Country):
    result = srv.getCountryCases(Country)
    countryJson = []
    for row in result:
        countryJson.append({"Date:": row.date, "Confirmed": row.confirmed, "ConfirmedLast14": row.confirmedlast14, "Deaths": row.deaths,
                                "DeathsLast14": row.deathlast14})
    return jsonify({"data": countryJson})


@app.route('/Country/<Country>/plot_DailyCases.png')
def plot_DailyCases_png(Country):
    fig = create_dailyCasesfigure(Country)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

@app.route('/Country/<Country>/plot_InfectionTrend.png')
def plot_InfectionTrend_png(Country):
    fig = create_infectionTrendfigure(Country)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

##
## Plotting functions
##
def create_dailyCasesfigure(country):
    result = srv.getCountryCases(country)
    X = []
    y_confirmed = []
    #y_deaths = []
    for row in result:
        date = datetime.datetime.strptime(row.date, "%Y-%m-%d")
        X.append(date)
        y_confirmed.append(row.confirmed)
        #y_deaths.append(row.deaths)

    dates = mdates.date2num(X)
    fig = Figure(figsize=(10,5))
    axis = fig.add_subplot(1, 1, 1)
    axis.plot_date(dates, y_confirmed, xdate=True, ls='-', marker=None, label="Daily Infections")
    #axis.plot_date(dates, y_deaths, xdate=True, ls='-', marker=None, label="Daily Deaths", color='black')
    axis.margins(y=0) # Use this if we want to remove x axis padding also
    #axis.legend()
    axis.set_ylabel('Daily infections')
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
    #result = srv.getCountryCases("Iceland")
    #for r in result:
    #    print(r)
