import json
import os, sys
import requests
import urllib.parse
import datetime
#sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'Stream_Service'))
#import CovidAPI
# pip install cassandra-driver
from cassandra.cluster import Cluster
from flask import Flask, jsonify

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
    
    def getCountryCases(self, country):
        prep = self.session.prepare("SELECT * FROM countrycases WHERE country=?")
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


if __name__ == "__main__":
    srv = Service()
    app.run()
