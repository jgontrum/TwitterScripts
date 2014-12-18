__author__ = 'johannes'
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from sklearn.cluster import KMeans
import ConfigParser
import sqlsoup
import re
from operator import itemgetter

# Get data
# Regular expression to find coordinates
lonlatpattern = re.compile('\{latitude=(\d+\.\d+).*longitude=(\d+\.\d+)\}')
# <editor-fold desc="SQL Connect">
# Read in config:
config = ConfigParser.RawConfigParser()
config.read('../config.properties')

dbHost = config.get('MySQL', 'mySQLHost')
dbUser = config.get('MySQL', 'mySQLUser')
dbPassword = config.get('MySQL', 'mySQLPassword')
dbTable = config.get('MySQL', 'mySQLTablePrefix')
dbTable = "DeChAtGEO"

# Connect to database
dbHost = dbHost[dbHost.find('://') + 3:]
db = sqlsoup.SQLSoup('mysql://' + dbUser + ":" + dbPassword + "@" + dbHost)
# </editor-fold>

rp = db.execute('select Longitude, Latitude, Text from ' + dbTable + ' WHERE LocationDE = 1')

tokendict = {}
tokenDistribution = {}
# <editor-fold desc="Fetch data">
coordinates = list()
for lon, lat, text in rp.fetchall():
    for token in text.split():
        if token in tokendict:
            loo, lao, coo = tokendict[token]
            loo += lon
            lao += lat
            coo += 1
            tokendict[token] = (loo, lao, coo)
            tokenDistribution[token].append((lon,lat))
        else:
            tokendict[token] = (lon, lat, 1)
            tokenDistribution[token] = [(lon,lat)]

    #coordinates.append((lon, lat))
# </editor-fold>

tokenToVariance = []
# Factor for making degrees of longitude roughly equal degrees of latitude in variance calculation
longitude_factor = 0.636

for token in tokenDistribution.keys():
    tokenList = tokenDistribution[token]
    count = len(tokenList)
    if count > 2:
        npList = np.asarray(tokenList, dtype=float)
        (meanx,meany) = tuple(np.mean(npList, axis=0))
        variance_num = 0
        for (pointx,pointy) in tokenList:
            variance_num += (pointx-meanx)**2 + (longitude_factor*(pointy-meany))**2
        variance = variance_num/count
        tokenToVariance.append((token,variance))
        if variance < 1:
            coordinates.append((meanx,meany))

# ## Print tokens by variance
# for token in sorted(tokenToVariance, key=itemgetter(1)):
#     print token

print "Num:" , len(coordinates)
# <editor-fold desc="Clustering">
# Create numpy array
data = np.asarray(coordinates, dtype=float)
kmeans = KMeans(n_clusters=5)
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
for lon, lat in kmeans.cluster_centers_:
    map.plot(lon, lat, 'b.', latlon=True)
# </editor-fold>

print len(coordinates)
plt.show()