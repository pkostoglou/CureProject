#!/home/panos/PycharmProjects/MachineLearning/venv/bin/python
#print "mpike"
import sys
from sklearn.cluster import AgglomerativeClustering

points = []
for line in sys.stdin:
  newLine = line.replace('(','').replace(')','').split(',')
  points.append((float(newLine[0]) , float(newLine[1])))
cluster = AgglomerativeClustering(n_clusters=3, affinity='euclidean', linkage='ward')  
cluster.fit_predict(points)
counter = 0
for c in cluster.labels_:
  print(str(points[counter][0])+","+str(points[counter][1])+","+str(c))
  counter += 1
