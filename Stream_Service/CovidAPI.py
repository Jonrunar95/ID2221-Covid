#!/usr/bin/python3

import http.client
import mimetypes
import json
from matplotlib import pyplot as plt
import numpy as np
import matplotlib.dates as mdates
import datetime

class API:
    # Class Initializer
    def __init__(self):
        self.conn = http.client.HTTPSConnection("api.covid19api.com")

    # Returns data from API as json object
    # URI: string
    def getData(self, URI):
        payload = ''
        headers = {'X-Access-Token': '8ee2b7a5-b8df-44c9-9eb2-2f12da1d5ed8' }
        URI = URI.encode('ascii', 'ignore').decode('ascii')
        self.conn.request("GET", URI, payload, headers)
        res = self.conn.getresponse()
        data = res.read()
        datajson = json.loads(data)
        return datajson

    # Returns available URI paths as json object
    def getAvailableUri(self):
        URI = ""
        return self.getData(URI)

    # Returns available country names as json object
    def getAllCountryNames(self):
        URI = "/countries"
        return self.getData(URI)

    # Returns all country cases from day one as json object
    # country: string
    def getDayOneCountry(self, country):
        URI = "/total/dayone/country/" + country
        return self.getData(URI)

    def getAllCountrySummaries(self):
        URI = "/summary"
        return self.getData(URI)["Countries"]

    # Returns all cases for all countries as a list of jsons
    def getAllCountryCases(self):
        print("Getting a lot of data from API...")
        countries = self.getAllCountryNames()
        countryCases = [] # List of json's for each country
        # For each country, fetch cases from day one
        for c in countries:
            if(c["Slug"] == "réunion" or c["Slug"] == "saint-barthélemy"): # These guys is problem, also empty anyways...
                continue
            countryCases.append(self.getDayOneCountry(c["Slug"]))

            # If API fetching is not successful
            try:
                if countryCases[-1]["success"] == False:
                    print(countryCases[-1]["message"])
            except:
                pass
        print("Data fetch completed!")
        return countryCases
    
    # Plot daily infections
    # country: string
    def plotCountry(self, country):
        data = self.getDayOneCountry(country)
        x = []
        y = []
        
        print("Plotting data for", country)
        for l in data:
            date = l["Date"].split("T")
            date = datetime.datetime.strptime(date[0], "%Y-%m-%d")
            x.append(date)
            y.append(l["Active"])
        
        dates = mdates.date2num(x)
        plt.plot_date(dates, y, xdate=True, ls='-', marker=None)
        plt.ymin=0
        plt.show()


if __name__ == "__main__":
    # Create API instance
    api = API()
    
    #data = api.getAllCountryCases()

    #data = api.getData("/summary")  

    #data = api.getDayOneCountry("usa")

    api.plotCountry("iceland")

    #data = api.getData("/all")

    #data = api.getAllCountrySummaries()

    #print(data)

    #for d in data:
    #   print(d["Slug"])


    #for d in data:
    #    e = d["Premium"]["CountryStats"]
    #    print(e)


    #print(data)

    # for d in data:
    #     if len(d) > 0:
    #         try:
    #             print(d[0])
    #         except:
    #             print(d)
        #print(d["Country"])



    
