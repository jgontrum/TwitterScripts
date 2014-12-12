__author__ = 'johannes'
import ConfigParser
import sqlsoup
import re
import countries

cc = countries.CountryChecker('TM_WORLD_BORDERS-0.3.shp')
def isInTargetCounty (lat, lon):
    country = cc.getCountry(countries.Point(lat, lon))
    if not country == None:
        c = cc.getCountry(countries.Point(lat, lon)).iso
        return c
    return "XX"

# Regular expression to find coordinates
lonlatpattern = re.compile('\{latitude=(\d+\.\d+).*longitude=(\d+\.\d+)\}')
# <editor-fold desc="SQL Connect">
# Read in config:
config = ConfigParser.RawConfigParser()
config.read('/Users/johannes/Development/Tweets2SQL/config.properties')

dbHost = config.get('MySQL', 'mySQLHost')
dbUser = config.get('MySQL', 'mySQLUser')
dbPassword = config.get('MySQL', 'mySQLPassword')
dbTable = config.get('MySQL', 'mySQLTablePrefix')
dbTable = "DeChAtGEO"

# Connect to database
dbHost = dbHost[dbHost.find('://') + 3:]
db = sqlsoup.SQLSoup('mysql://' + dbUser + ":" + dbPassword + "@" + dbHost)
# </editor-fold>

rp = db.execute('select JSON, TweetID, Longitude, Latitude, German from DeChAtGEO LIMIT 10')

for json, tweetid, long, lat, grm in rp.fetchall():
    if long is None or lat is None or grm is None:
        matches = lonlatpattern.search(json)
        if matches != None:
            lat = float(matches.group(1))
            lon = float(matches.group(2))
            country = isInTargetCounty(lat, lon)
            valid = 0
            if country == "DE" or country == "AT" or country == "CH":
                valid = 1

            q = 'UPDATE ' + dbTable + " SET Longitude = " + str(lon) + ", Latitude = " + str(
                lat) + ", Country = " + country + ", German = " + str(valid) + " WHERE TweetID = " + str(tweetid)
            print q
            db.bind.execute(q)
        else:
            pass
            # delete row