import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn import preprocessing, metrics
from sklearn.cross_validation import cross_val_score, train_test_split, ShuffleSplit
from sklearn import metrics
import os, sys

os.chdir('C:/Users/dpatn/Desktop/Win_Share/projects/smartfly/datamodels')

header = [
'Uniqueflightid',
'Year',
'Month',
'Daynum',
'DOW',
'SDT',
'SAT',
'Airline',
'Flightnum',
'Tailnum',
'Pmodel',
'Seatconf',
'Depdelay',
'Originarpt',
'Destarpt',
'Disttraveld',
'Taxitimein',
'Taxitimeout',
'Cancelled',
'Cancelledcode'
]

seed = 42
trainrows = 7270237

"""

traindf = pd.read_csv('smartfly_historic.csv', names = header, low_memory = False, dtype = {'SDT': object, 'SAT': object})
testdf = pd.read_csv('smartfly_scheduled.csv', names = header, low_memory = False, dtype = {'SDT': object, 'SAT': object, 'Uniqueflightid': object})

traindf['target'] = 0
traindf.loc[traindf.Depdelay > 0, 'target'] = 1

traindf = traindf[traindf.Cancelled == 0]
traindf = traindf[traindf.SAT != '2096']

combdf = pd.concat((traindf, testdf), axis = 0)
combdf['Airline_fn'] = combdf['Airline'] + '_' + combdf['Flightnum'].map(str)


n = 0
timedic = {}
for hr in range(0, 25):
	for mn in ['0' + str(m) if m < 10 else str(m) for m in range(0, 60)]:
		timedic[str(int(str(hr) + mn))] = n
		n += 1

combdf['Deptime'] = combdf['SDT'].map(timedic)
combdf['Arrtime'] = combdf['SAT'].map(timedic)

combdf['Dephour'] = combdf['Deptime'].map(lambda x: int(x) / 60)
combdf['Arrhour'] = combdf['Arrtime'].map(lambda x: int(x) / 60)

catvars = [
'Airline',
'Flightnum',
'Tailnum',
'Pmodel',
'Seatconf',
'Originarpt',
'Destarpt',
'Airline_fn'
]

le = preprocessing.LabelEncoder()

for catvar in catvars:
	combdf[catvar] = le.fit_transform(combdf[catvar])

combdf.drop(['SDT', 'SAT', 'Depdelay', 'Taxitimein', 'Taxitimeout', 'Cancelled', 'Cancelledcode'], inplace = True, axis = 1)

colnames = combdf.columns.tolist()
combdf = combdf[colnames[11:12] + colnames[0:11] + colnames[12:]]

print combdf.columns.tolist()
combdf.to_csv('newdf.csv', index = False)

"""
combdf = pd.read_csv('newdf.csv')

trainy = combdf.iloc[:trainrows].target.values
testids = combdf.iloc[trainrows:, 0].values

combdf.drop(['target'], inplace = True, axis = 1)

traindf = combdf.iloc[:trainrows, 1:]
testdf = combdf.iloc[trainrows:, 1:]

trainX = traindf.values
testX = testdf.values


X_train, X_valid, y_train, y_valid = train_test_split(trainX, trainy, test_size = 0.3, random_state = seed)
rf = RandomForestClassifier(n_estimators = 20, min_samples_leaf = 10, n_jobs = -1, verbose = 1, random_state = seed)
rf.fit(X_train, y_train)

print metrics.roc_auc_score(y_valid, rf.predict_proba(X_valid)[:,1])


"""
kf = ShuffleSplit(n = trainy.shape[0], n_iter = 1, test_size = 0.2, random_state = seed)
for train_index, test_index in kf:
	rf.fit(trainX[train_index], trainy[train_index])
	pred = rf.predict_proba(trainX[test_index])[:,1]
	print metrics.roc_auc_score(trainy[test_index], pred)



rf = RandomForestClassifier(n_estimators = 200, min_samples_leaf = 10, n_jobs = -1, verbose = 1, random_state = seed)
rf.fit(trainX, trainy)

finaldf = pd.concat((pd.DataFrame(testids, columns = ['Ids']), pd.DataFrame(rf.predict_proba(testX)[:, 1], columns = ['predictions'])), axis = 1)
finaldf.sort_values(by = 'predictions', ascending = False, inplace = True)

finaldf.to_csv('newsol.csv', index = False, header = False)
"""