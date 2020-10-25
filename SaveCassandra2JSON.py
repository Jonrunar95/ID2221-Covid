from cassandra.cluster import Cluster
import numpy as np

def getConfirmedCountryCases(country):
    prep = session.prepare("SELECT confirmed3dayaverage FROM countrycases WHERE country=?")
    result = session.execute(prep, [country])
    return result

def getAllCountryCases():
    result = session.execute("SELECT * from countrycases")
    return result

def getAllCountries():
    result = session.execute("SELECT Country from countryinfo")
    return result

cluster = Cluster()
session = cluster.connect("covid")

Countries = getAllCountries().all()

countries = []

for i in range(len(Countries)):
    countries.append(str(Countries[i][0]))

CountryCases = []

for i in range(len(countries)):
    print(countries[i])
    cases = getConfirmedCountryCases(countries[i]).all()
    arr = np.zeros((len(cases)))
    for j in range(len(cases)):
        arr[j] = cases[j][0]
    CountryCases.append(arr)

np.save("CountryCases.npy", CountryCases)
