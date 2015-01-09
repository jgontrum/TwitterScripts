__author__ = 'johannes'
import ConfigParser
import sqlsoup
import re
import countries
import sys
import math

# <editor-fold desc="Check if coordinates are in DE,CH,AT">
cc = countries.CountryChecker('filtered.shp')
def isInTargetCounty (lat, lon):
    country = cc.getCountry(countries.Point(lat, lon))
    if not country == None:
        c = cc.getCountry(countries.Point(lat, lon)).iso
        return c
    return "XX"
# </editor-fold>

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

# <editor-fold desc="Read DB content">
""" Read MySQL database in chunks """
max_rows = db.execute('SELECT COUNT(*) FROM DeChAtGEO').fetchall()[0][0]
current_top_row = 0
step = 10000
def getNextDBChunk():
    global current_top_row
    global step
    global max_rows
    if current_top_row > max_rows:
        print "Finished!"
        return None
    rp = db.execute('SELECT JSON, TweetID, Longitude, Latitude, LocationDE FROM DeChAtGEO ORDER BY CreatedAt LIMIT ' + str(current_top_row) + ',' + str(step))
    print "[DEBUG] Returning range from",current_top_row,"to",(current_top_row + step)
    current_top_row += step
    return rp
# </editor-fold>

# <editor-fold desc="Join queries">
""" Join the update queries """
max_allowed_package = db.execute("SHOW VARIABLES LIKE 'max_allowed_packet'").fetchall()[0][1]
limit = 0.2
update_cache = ""
def updateChanges(newQuery, final=False):
    global update_cache
    global limit
    global max_allowed_package
    update_cache += newQuery
    if sys.getsizeof(update_cache) > limit * max_allowed_package or final and (sys.getsizeof(update_cache) > 0) :
        print "[DEBUG] Making update query"
        db.bind.execute(update_cache)
        update_cache = ""
# </editor-fold>

# Main Loop!
counter = 0
while True:
    rp = getNextDBChunk()
    if rp == None:
        break
    for json, tweetid, long, lat, grm in rp.fetchall():
        counter += 1
        if counter % 2000 == 0:
            print str(int(math.ceil((float(counter) / max_rows) * 100))) + "%"
        if long is None or lat is None or grm is None:
            matches = lonlatpattern.search(json)
            if matches != None:
                lat = float(matches.group(1))
                lon = float(matches.group(2))
                country = isInTargetCounty(lat, lon)
                valid = 0
                if country == "DE" or country == "AT" or country == "CH":
                    valid = 1

                updateChanges('UPDATE ' + dbTable + " SET Longitude = " + str(lon) + ", Latitude = " + str(
                    lat) + ", Country = '" + country + "', LocationDE = " + str(valid) + " WHERE TweetID = " + str(tweetid) + ";")
            else:
                pass
                # delete row
updateChanges("",final=True)
