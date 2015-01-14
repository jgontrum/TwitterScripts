__author__ = 'johannes'
import ConfigParser
import re
import countries
import sys
import math
import MySQLdb

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
config.read('../Tweets2SQL/config.properties')

dbHostDB = config.get('MySQL', 'mySQLHost')
dbUser = config.get('MySQL', 'mySQLUser')
dbPassword = config.get('MySQL', 'mySQLPassword')
dbTable = config.get('MySQL', 'mySQLTablePrefix')
dbTable = "SchefflerGeoKorpus"
dbHost = dbHostDB[:dbHostDB.find("/")]
dbDB = dbHostDB[dbHostDB.find("/") + 1:]

# Connect to database
connection = MySQLdb.connect(dbHost, dbUser, dbPassword, dbDB)
connection.autocommit(False)
dbCursor = connection.cursor()

connectionInsert = MySQLdb.connect(dbHost, dbUser, dbPassword, dbDB)
connectionInsert.autocommit(True)
insertCursor = connectionInsert.cursor()
# </editor-fold>

# <editor-fold desc="Read DB content">
""" Read MySQL database in chunks """
dbCursor.execute('SELECT COUNT(*) FROM ' + dbTable)
max_rows = float(dbCursor.fetchone()[0])
current_top_row = 0
step_fetch = 50000


def getNextDBChunk ():
    global current_top_row
    global step_fetch
    global max_rows
    global dbCursor
    if current_top_row > max_rows:
        print "Finished!"
        return None
    dbCursor.execute(
        'SELECT TweetID, Longitude, Latitude, LocationDE FROM ' + dbTable + ' ORDER BY CreatedAt DESC LIMIT ' + str(
            current_top_row) + ',' + str(step_fetch))
    rp = dbCursor.fetchall()
    print "[DEBUG] Returning range from", current_top_row, "to", (current_top_row + step_fetch)
    current_top_row += step_fetch
    return rp

# </editor-fold>

# <editor-fold desc="Join queries">
""" Join the update queries """
update_counter = 0
step = 20000
locationDEMap = {}
countryMap = {}


def updateChanges (newQuery, final=False):
    global update_counter
    global step
    global insertCursor
    global locationDEMap
    global countryMap
    
    if not final:
        locationDEMap.setdefault(newQuery[1], []).append(newQuery[2])
        countryMap.setdefault(newQuery[0], []).append(newQuery[2])

    update_counter += 1
    if update_counter >= step or (final and update_counter > 0):
        print "[DEBUG] Making update query"
        for key, values in locationDEMap.items():
            insertValues = "(" + ",".join(values) + ")"
            insertCursor.execute("UPDATE " + dbTable + " SET LocationDE = " + key + " WHERE TweetID in " + insertValues)

        for key, values in countryMap.items():
            insertValues = "(" + ",".join(values) + ")"
            insertCursor.execute("UPDATE " + dbTable + " SET Country = '" + key + "' WHERE TweetID in " + insertValues)
        connectionInsert.commit()

        locationDEMap = {}
        countryMap = {}
        update_counter = 0

# </editor-fold>

# Main Loop!
counter = 0
while True:
    rp = getNextDBChunk()
    if rp == None:
        break
    for tweetid, long, lat, grm in rp:
        counter += 1
        if counter % 2000 == 0:
            print str(int(math.ceil((float(counter) / max_rows) * 100))) + "%"
        if grm == None:
            country = isInTargetCounty(lat, long)
            valid = 0
            if country == "DE" or country == "AT" or country == "CH":
                valid = 1
            updateChanges((country, str(valid), str(tweetid)))
updateChanges("", final=True)


