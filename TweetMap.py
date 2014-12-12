__author__ = 'johannes'
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import ConfigParser
import sqlsoup
import re

# Regular expression to find coordinates
lonlatpattern = re.compile('\{latitude=(\d+\.\d+).*longitude=(\d+\.\d+)\}')


# Read in config:
config = ConfigParser.RawConfigParser()
config.read('/Users/johannes/Development/Tweets2SQL/config.properties')

dbHost = config.get('MySQL', 'mySQLHost')
dbUser = config.get('MySQL', 'mySQLUser')
dbPassword = config.get('MySQL', 'mySQLPassword')
dbTable = config.get('MySQL', 'mySQLTablePrefix')
dbTable = "DeChAtGEO"

# Connect to database
dbHost = dbHost[dbHost.find('://')+3:]
db = sqlsoup.SQLSoup('mysql://' + dbUser + ":" + dbPassword + "@" + dbHost)

rp = db.execute('select JSON from ' + dbTable)


# Prepare Map
map = Basemap(projection='merc',
resolution='l',
              area_thresh=200,
              lat_0=51.16,  # center
              lon_0=10.44,  # center
              llcrnrlon=5.3,  # longitude of lower left hand corner of the desired map domain (degrees).
              llcrnrlat=45,  # latitude of lower left hand corner of the desired map domain (degrees).
              urcrnrlon=18,  # longitude of upper right hand corner of the desired map domain (degrees).
              urcrnrlat=56  # latitude of upper right hand corner of the desired map domain (degrees).
)

# draw coastlines, state and country boundaries, edge of map.
map.drawcoastlines(linewidth=0.25)
map.drawcountries(linewidth=0.25)
map.fillcontinents(color='snow', lake_color='lightcyan')
# draw the edge of the map projection region (the projection limb)
map.drawmapboundary(fill_color='lightblue')


# Iterate over rows
c = 0
coordinates = list()
for jsonrow in rp.fetchall():
    matches = lonlatpattern.search(jsonrow.items()[0][1])
    if matches != None:
        c += 1
        lat =  float(matches.group(1))
        lon =  float(matches.group(2))
        coordinates.append((lon, lat))
        map.plot(lon, lat, 'r,', latlon=True)

print c, " dots"
plt.show()
