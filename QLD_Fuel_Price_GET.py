import requests
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector

import time

start_time = time.time()

conn = mysql.connector.connect(host='xxxxxxxxx', database='QLD_Fuel_Prices', user='xxxxxxxxx', password = 'xxxxxxxxx')
cursor = conn.cursor()


engine = create_engine('mysql+mysqldb://xxxxxxxxx:xxxxxxxxx/QLD_Fuel_Prices?charset=utf8',
                       encoding='utf-8')

headers = {
    'Authorization': "FPDAPI SubscriberToken=xxxxxxxxx",
    'Content-Type': "application/json",
    'cache-control': "no-cache",
}

url_fuelpricesqld = "https://fppdirectapi-prod.fuelpricesqld.com.au/Subscriber/GetCountryBrands"
url_CountryGeographicRegions = "https://fppdirectapi-prod.fuelpricesqld.com.au/Subscriber/GetCountryGeographicRegions"
url_FullSiteDetails = "https://fppdirectapi-prod.fuelpricesqld.com.au/Subscriber/GetFullSiteDetails"
url_GetCountryFuelTypes = "https://fppdirectapi-prod.fuelpricesqld.com.au/Subscriber/GetCountryFuelTypes"
url_GetSitesPrices = "https://fppdirectapi-prod.fuelpricesqld.com.au/Price/GetSitesPrices"

querystring_1 = {"countryId": "21"}
querystring_2 = {"countryId": "21", "geoRegionLevel": "3", "geoRegionId": "1"}

response_fuelpricesqld = requests.request("GET", url_fuelpricesqld, headers=headers, params=querystring_1)
response_CountryGeographicRegions = requests.request("GET", url_CountryGeographicRegions, headers=headers, params=querystring_1)
response_FullSiteDetails = requests.request("GET", url_FullSiteDetails, headers=headers, params=querystring_2)
response_CountryFuelTypes = requests.request("GET", url_GetCountryFuelTypes, headers=headers, params=querystring_1)
response_SitesPrices = requests.request("GET", url_GetSitesPrices, headers=headers, params=querystring_2)

# ********************* Servo Brands *******************

j1 = response_fuelpricesqld.json()

data = pd.DataFrame(j1['Brands'])
df = pd.DataFrame(data, columns=['BrandId', 'Name'])
df.to_sql(con=engine, name='Brands', if_exists='replace', index=True, index_label='id')

# ********************* Country Geographic Regions *******************

j2 = response_CountryGeographicRegions.json()
data = pd.DataFrame(j2['GeographicRegions'])

df = pd.DataFrame(data, columns=['GeoRegionLevel', 'GeoRegionId', 'Name', 'Abbrev', 'GeoRegionParentId'])
df.to_sql(con=engine, name='GeographicRegions', if_exists='replace', index=True, index_label='id')

# ********************* Full Site Details *******************

j3 = response_FullSiteDetails.json()
data = pd.DataFrame(j3['S'])

df = pd.DataFrame(data, columns=['S', 'A', 'N', 'B', 'P', 'G1', 'G2', 'G3', 'G4', 'G5', 'Lat', 'Lng'])
df.to_sql(con=engine, name='S', if_exists='replace', index=False)

# ********************* Country Fuel Types *******************

j4 = response_CountryFuelTypes.json()

data = pd.DataFrame(j4['Fuels'])
df = pd.DataFrame(data, columns=['FuelId', 'Name'])
df.to_sql(con=engine, name='Fuels', if_exists='replace', index=True, index_label='id')

# ********************* Sites Prices *******************

j5 = response_SitesPrices.json()

data = pd.DataFrame(j5['SitePrices'])
df = pd.DataFrame(data, columns=['SiteId', 'FuelId', 'CollectionMethod', 'TransactionDateUtc', 'Price'])
df.to_sql(con=engine, name='SitePrices', if_exists='replace', index=True, index_label='id')

# ********************* Consolidated DB Table *******************

query_drop_table = ("""DROP TABLE IF EXISTS QLDFuelPrices""")
cursor.execute(query_drop_table)
conn.commit()

query_create = """CREATE TABLE QLDFuelPrices(_id int NOT NULL AUTO_INCREMENT PRIMARY KEY, SiteID int(20), FuelId int(20), 
                                                            BrandId int(20), GeoRegionId int(20), Name varchar (255), Address varchar(255), 
                                                            Suburb varchar(255), Lat varchar(255), Lng varchar(255), 
                                                            FuelType varchar(255), Brand varchar(255), FuelPrice int(10), Updated datetime )"""
cursor.execute(query_create)

query_SalesPrices = cursor.execute("""SELECT SiteId, FuelId, Price, TransactionDateUtc FROM SitePrices""")
data_SalesPrices = cursor.fetchall()

for rows in data_SalesPrices:
    cursor.execute('INSERT INTO QLDFuelPrices(SiteID, FuelId, FuelPrice, Updated) VALUES(%s,%s,%s,%s)', rows)


query2 = ("""UPDATE QLDFuelPrices AS t1
                            INNER JOIN S AS t2 on t1.SiteID = t2.S
                            SET t1.Name = t2.N""")
cursor.execute(query2)

query3 = ("""UPDATE QLDFuelPrices AS t1
                            INNER JOIN S AS t2 on t1.SiteID = t2.S
                            SET t1.Address = t2.A""")
cursor.execute(query3)

query4 = ("""UPDATE QLDFuelPrices AS t1
                            INNER JOIN S AS t2 on t1.SiteID = t2.S
                            SET t1.Lat = t2.Lat""")
cursor.execute(query4)

query4 = ("""UPDATE QLDFuelPrices AS t1
                            INNER JOIN S AS t2 on t1.SiteID = t2.S
                            SET t1.Lng = t2.Lng""")
cursor.execute(query4)

query6 = ("""UPDATE QLDFuelPrices AS t1
                            INNER JOIN Fuels AS t2 on t1.FuelId = t2.FuelId
                            SET t1.FuelType = t2.Name""")
cursor.execute(query6)

query7 = ("""UPDATE QLDFuelPrices AS t1
                            INNER JOIN S AS t2 on t1.SiteID = t2.S
                            SET t1.BrandId = t2.B""")
cursor.execute(query7)

query8 = ("""UPDATE QLDFuelPrices AS t1
                            INNER JOIN Brands AS t2 on t1.BrandId = t2.BrandId
                            SET t1.Brand = t2.Name""")
cursor.execute(query8)

query9 = ("""UPDATE QLDFuelPrices AS t1
                            INNER JOIN S AS t2 on t1.SiteID = t2.S
                            SET t1.GeoRegionId = t2.G1""")
cursor.execute(query9)

query10 = ("""UPDATE QLDFuelPrices AS t1
                            INNER JOIN GeographicRegions AS t2 on t1.GeoRegionId = t2.GeoRegionId
                            SET t1.Suburb = t2.Name""")
cursor.execute(query10)


conn.commit()
cursor.close()
conn.close()

print("--- %s seconds ---" % (time.time() - start_time))
