from pyspark import SparkContext, SparkConf
import ast, csv, StringIO

mapObj = open('categorical_map.txt', 'r')

mappings = []
for x in mapObj:
	mappings.append(ast.literal_eval(x))

cols = [1,2,3]
numcols = sum(map(len, mappings))

def transform(record):
	allfields = record.split(',')
	#print len(allfields)
	newcols = [0] * numcols

	beginIdx = 0
	step = 0
	for col in cols:
		newcols[mappings[beginIdx][allfields[col]] + step] = 1
		step += len(mappings[beginIdx])
		beginIdx += 1

	rawfields =[]
	rawfields.append(allfields[len(allfields) - 1])
	rawfields.append(allfields[0])
	for x in allfields[4:len(allfields) - 1]:
		rawfields.append(x)

	return  rawfields + newcols

#strn = '0,tcp,http,SF,215,45076,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,normal.'
#print transform(strn)

def writetocsv(rec):
	output = StringIO.StringIO()
	csv.writer(output).writerow(rec)
	return output.getvalue().strip()

conf = SparkConf().setMaster('local[*]').setAppName('KDDPyData')
sc = SparkContext(conf = conf)

baseRDD = sc.textFile('file:///C:/Users/dpatn/Desktop/Win_Share/projects/kdd_clustering/kddcup.data')
baseRDD.map(lambda x: transform(x)).map(lambda x: writetocsv(x)).saveAsTextFile('file:///C:/Users/dpatn/Desktop/Win_Share/projects/kdd_clustering/transformeddata')