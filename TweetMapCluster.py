__author__ = 'johannes'
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from sklearn.cluster import KMeans
import ConfigParser
import sqlsoup
import re

# Get data
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

rp = db.execute('select Longitude, Latitude from ' + dbTable + ' WHERE LocationDE = 1')

# <editor-fold desc="Fetch data">
coordinates = list()
for lon, lat in rp.fetchall():
    coordinates.append((lon, lat))
# </editor-fold>

# <editor-fold desc="Clustering">
# Create numpy array
data = np.asarray(coordinates, dtype=float)
kmeans = KMeans(n_clusters=14)
kmeans.fit(data)
# </editor-fold>

# <editor-fold desc="Prepare Basemap">
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
# </editor-fold>

# <editor-fold desc="Drawing tweets">
# Draw tweets
for lon, lat in coordinates:
    map.plot(lon, lat, 'r,', latlon=True)

# Draw centroids
# for lon, lat in kmeans.cluster_centers_:
#     map.plot(lon, lat, 'b.', latlon=True)
# </editor-fold>

print len(coordinates)
plt.show()