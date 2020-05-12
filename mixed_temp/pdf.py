import pandas as pd
import numpy as np
import os,sys
import json, datetime

from scipy.stats import chi2_contingency
from statsmodels.stats.weightstats import ztest

os.chdir('C:/Users/dpatn/Desktop/Win_Share/projects/famous/data')

def transformData(filename):
	weblogdata = []
	with open(filename,'r') as weblogfile:
		for line in weblogfile:
			newdata = []
			data = json.loads(line)
			try:
				query = data['query']
			except:
				query = 'NA'

			try:
				campaign = str(data['campaign'])
			except:
				campaign = 'NA'

			exprmnt1_2 = str(data['experiments'][0])
			exprmnt3_4 = str(data['experiments'][1])
			ts = int((datetime.datetime.strptime(data['tstamp'], "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime('1970-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")).total_seconds())
			newdata.append(data['visit_id'])
			newdata.append(data['uid'])
			newdata.append(query)
			newdata.append(campaign)
			newdata.append(data['action'])
			newdata.append(exprmnt1_2)
			newdata.append(exprmnt3_4)
			newdata.append(ts)
			newdata.append(data['tstamp'])
			weblogdata.append(newdata)

	df = pd.DataFrame(weblogdata, columns=['visit_id','uid','query','campaign','action','experiment1_2','experiment3_4', 'tstamp', 'acttime'])
	df.sort_values(by=['visit_id','uid','tstamp'], inplace=True)
	df.to_csv('new' + filename, index=False, header=False)
	return df

def getmaxadclicktime(spamdf):
	jndspamdf = pd.merge(spamdf,spamdf,on='visit_id')
	newspamdf = jndspamdf[(jndspamdf['action_x']=='landed') & (jndspamdf['action_y']=='adclick')]
	newspamdf['timediff'] = newspamdf['tstamp_y'] - newspamdf['tstamp_x']
	return int(newspamdf.groupby('visit_id').agg({'timediff':np.min}).max())

def getbotids(webdf, maxadclicktime):
	jndwebdf = pd.merge(webdf,webdf,on='visit_id')
	newwebdf = jndwebdf[(jndwebdf['action_x']=='landed') & (jndwebdf['action_y']=='adclick')]
	newwebdf['timediff'] = newwebdf['tstamp_y'] - newwebdf['tstamp_x']
	newwebdf = newwebdf[newwebdf['timediff'] <= maxadclicktime]
	adclickids = set(newwebdf['uid_x'].values)
	legitids = set(webdf[(webdf['action']=='order') | (webdf['action']=='signup')]['uid'].values)
	return list(adclickids - legitids)

"""

# Transform Data

transformData('web.log')
transformData('spam.log')

# Problem 1 - Find bots and filter web.log

webdf = pd.read_csv('newweb.log',names=['visit_id','uid','query','campaign','action','experiment1_2','experiment3_4', 'tstamp', 'acttime'])
spamdf = pd.read_csv('newspam.log',names=['visit_id','uid','query','campaign','action','experiment1_2','experiment3_4', 'tstamp', 'acttime'])

maxadclicktime = getmaxadclicktime(spamdf)
print 'Bots click ads within: ' + str(maxadclicktime) + ' seconds'
botlist = getbotids(webdf, maxadclicktime)
print 'Number of Bots ids in Web.log: ' + str(len(botlist))

# Generate clean Web Log

newwebdf = webdf[~webdf.uid.isin(botlist)]
newwebdf.to_csv('cleanweb.log', index=False, header=False)
webdf = pd.read_csv('cleanweb.log',names=['visit_id','uid','query','campaign','action','experiment1_2','experiment3_4', 'tstamp', 'acttime'])
webdf.fillna(method='ffill',inplace=True)
webdf.to_csv('cleanweb.log', index=False, header=False)

# Problem 2 - 0.010022915283

print float(len(webdf[webdf.action == 'adclick'].index)) / len(webdf['visit_id'].unique())

# Problem 3.1

unquserscounts = webdf.groupby(['query','campaign']).agg({'uid':pd.Series.nunique}).reset_index()

newwebdf = webdf[webdf.action=='order']
ordervstids = newwebdf['visit_id'].unique()
newwebdf = webdf[webdf.visit_id.isin(ordervstids)]
ordercounts = newwebdf.groupby(['query','campaign']).agg({'visit_id':pd.Series.nunique}).reset_index()

mergeddf = pd.merge(ordercounts,unquserscounts)
mergeddf['mean_orders'] = mergeddf['visit_id'] / mergeddf['uid']
print mergeddf.values[mergeddf.mean_orders.argmax(),0:2]

"""

# Problem 3.2

webdf = pd.read_csv('cleanweb.log',names=['visit_id','uid','query','campaign','action','experiment1_2','experiment3_4', 'tstamp', 'acttime'])

"""
std_1 = webdf.groupby(['query','campaign','uid']).action.count().reset_index()
std_1['action']=0

std_2 = webdf[webdf.action=='order'].groupby(['query','campaign','uid']).action.count().reset_index()

std_3 = pd.merge(left=std_1,right=std_2, how='left', on=['query','campaign','uid'])
std_3.action_y.fillna(0,inplace=True)

print std_3.groupby(['query','campaign']).action_y.std().reset_index()

"""
# Problem 4

def getcontigencyframe(df, col):
	totusers = df.groupby([col]).agg({'uid':pd.Series.nunique}).reset_index()
	sgnupusers = df[df['action'] == 'signup'].groupby([col]).agg({'uid':pd.Series.nunique}).reset_index()
	newdf = pd.merge(sgnupusers, totusers, on=[col])
	newdf['ntsignup'] = newdf['uid_y'] - newdf['uid_x']
	newdf['signup_rate'] = newdf['uid_x'] / newdf['uid_y']
	return newdf

print getcontigencyframe(webdf, 'experiment1_2')['signup_rate'].values
print getcontigencyframe(webdf, 'experiment3_4')['signup_rate'].values

contgncytbl1_2 = getcontigencyframe(webdf, 'experiment1_2')[['uid_x','ntsignup']].values
contgncytbl3_4 = getcontigencyframe(webdf, 'experiment3_4')[['uid_x','ntsignup']].values

g1, p1, dof1, expctd1 = chi2_contingency(contgncytbl1_2, lambda_="log-likelihood")
g2, p2, dof2, expctd2 = chi2_contingency(contgncytbl3_4, lambda_="log-likelihood")

print '\n'
print 'Confidence for exp1 vs exp2', p1
print 'Confidence for exp3 vs exp4', p2

webdf['acttime'] = pd.to_datetime(webdf.acttime)
webdf['day'] = [d.day for d in webdf.acttime]
webdf.sort_values(['acttime', 'visit_id'], inplace=True)

for x in range(15,31):
	subset = webdf[webdf['day'] <= x]
	contigencytbl = getcontigencyframe(subset, 'experiment1_2')[['uid_x','ntsignup']].values
	g, p, dof2, expctd = chi2_contingency(contigencytbl, lambda_="log-likelihood")
	print 'Day ' + str(x - 14) + ': p-value = ' + str(1 - p)

# Problem 5

print webdf.groupby(['experiment1_2']).agg({'visit_id':pd.Series.nunique}).reset_index()
webdf_revenueaction = webdf[webdf['action'] != 'landed']
print webdf_revenueaction.groupby(['experiment1_2','action']).agg({'visit_id':pd.Series.nunique}).reset_index()

print webdf.groupby(['experiment3_4']).agg({'visit_id':pd.Series.nunique}).reset_index()
webdf_revenueaction = webdf[webdf['action'] != 'landed']
print webdf_revenueaction.groupby(['experiment3_4','action']).agg({'visit_id':pd.Series.nunique}).reset_index()

webdf['exp_3_landed'] = np.where((webdf.action=='landed')&(webdf.experiment3_4==3),1,0)
webdf['exp_4_landed'] = np.where((webdf.action=='landed')&(webdf.experiment3_4==4),1,0)

webdf['exp_3_earned'] = np.where((webdf.action=='signup')&(webdf.experiment3_4==3),0.4, 
                      np.where((webdf.action=='order')&(webdf.experiment3_4==3),4,
                               np.where((webdf.action=='adclick')&(webdf.experiment3_4==3),0.1, 0)))
webdf['exp_4_earned'] = np.where((webdf.action=='signup')&(webdf.experiment3_4==4),0.4, 
                      np.where((webdf.action=='order')&(webdf.experiment3_4==4),4,
                               np.where((webdf.action=='adclick')&(webdf.experiment3_4==4),0.1, 0)))

webdf['exp_3_landed_cumu'] = webdf['exp_3_landed'].cumsum()
webdf['exp_4_landed_cumu'] = webdf['exp_4_landed'].cumsum()

webdf['exp_3_earned_cumu'] = webdf['exp_3_earned'].cumsum()
webdf['exp_4_earned_cumu'] = webdf['exp_4_earned'].cumsum()

webdf['mean_rev_exp_3'] = webdf['exp_3_earned_cumu']/webdf['exp_3_landed_cumu']
webdf['mean_rev_exp_4'] = webdf['exp_4_earned_cumu']/webdf['exp_4_landed_cumu']

res =[]
for i in range(0, webdf.shape[0], 1000):
    try:
        res.append([webdf.acttime.iloc[i],webdf.mean_rev_exp_3.iloc[i],webdf.mean_rev_exp_4.iloc[i]]+
                 list(ztest(webdf.exp_3_earned.iloc[:i],webdf.exp_4_earned.iloc[:i])))
    except:
        1==1

zt = pd.DataFrame(res, columns=['time','exp_3','exp_4','z','p'])
zt['day'] = [k.day - 14 for k in zt.time]

print 'Day ' + str(zt[zt.p < 0.01].day.min())
