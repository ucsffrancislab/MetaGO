#filename select group_specific features
#!/usr/bin/env python
'''
created on 2016/12/23
@author:fulei
'''
import os
import sys
import optparse
import math
from scipy.stats import chi2
from scipy.stats import ttest_ind , ttest_ind_from_stats
from scipy.special import stdtr
from math import fabs
import random
import numpy as np
from sklearn.linear_model import LogisticRegression
import scipy
from scipy import stats
from sklearn import metrics
from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel

prog_base = os.path.split(sys.argv[0])[1]

parser = optparse.OptionParser()
parser.add_option("-f", "--UnionFile", action = "store", type = "string", dest="Unionfile",
				 help="the list which include the files need to be unioned")
parser.add_option("-r", "--TupleNumber", action = "store", type = "string", dest="tupleNumber",
				 help="the number of tuple in each sample")
parser.add_option("-m", "--Hnumber", action = "store", type = "int", dest="healthy",
				 help="the sample include Healthy")
parser.add_option("-n", "--Pnumber", action = "store", type = "int", dest="patient",
				 help="the sample include patient")
parser.add_option("-c", "--c_thresh", action = "store", type = "float", dest="c_value", default=0.01,
				 help="the chi2 threshold of theta")
parser.add_option("-t", "--ASS_thresh", action = "store", type = "float", dest="ass_theta",default=0.8,
				 help="the threshold of ASS")
parser.add_option("-x", "--Wicxon_thresh", action = "store", type = "float", dest="wicxon_theta",default=0.05,
				 help="the threshold of wicxon_test")
parser.add_option("-l", "--LR_thresh", action = "store", type = "float", dest="logicalRegress_theta",default=0.8,
				 help="the threshold of LogicalRegress_test")
parser.add_option("-w", "--filterWay", action = "store", type = "string", dest="filter_way",
				 help="the way of filtering")
parser.add_option("-u", "--saveUnionFile", action = "store", type = "string", dest="saveUnion", default="N",
				 help="select if need save the unioned file")
parser.add_option("-s", "--saveFilterSparse", action = "store", type = "string", dest="saveSparse", default="N",
				 help="select if need save the Union files which have been filter sparse features")

(options,args)=parser.parse_args()

if(options.Unionfile is None or
   options.tupleNumber is None or
   options.healthy is None or
   options.patient is None or
   options.filter_way is None):
		print( prog_base+":error: Missing the required command-line argument." )
		parser.print_help()
		sys.exit(0)

file_in=options.Unionfile
file_tuple_number=options.tupleNumber
number_h=options.healthy
number_p=options.patient
ass_p=options.ass_theta
c_value=options.c_value
wicxon_p=options.wicxon_theta
LR_p=options.logicalRegress_theta
functions=options.filter_way
Union_save=options.saveUnion
Sparse_save=options.saveSparse

tuple_number=[]
f=open(file_in,'r')
f1=open(file_tuple_number,'r')
tuple_file=f.readlines()
first_file=tuple_file[0]
sample_name=first_file.split(".")
label=sample_name[0][-1:]
tuple_number_list=f1.readlines()

for lines in tuple_number_list:
		tuple_number.append(float(lines))
#########################################
# Data initialization prepare for union #
#########################################
para1=[]
para2=[]
for i in range(1,number_h+number_p+1):
	para1.append('a'+str(i))
	para2.append('b'+str(i))
print(para1)
print(para2)
wordList=['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
k=int((number_h+number_p)/26)
B=[]
listB=[]
for i in wordList[0:k+1]:
	for j in wordList:
		listB.append(i+j)
B=listB[0:number_h+number_p]
print(B)



cores=os.sysconf("SC_NPROCESSORS_ONLN")
sc = SparkContext("local["+str(cores)+"]", "First Spark App")
#sc = SparkContext("local[10]", "First Spark App")


###################################
# creat RDDs for each sample file #
###################################
for i in range (0,len(tuple_file)):
	para1[i]=sc.textFile(tuple_file[i][0:-1]).map(lambda x : (x.encode("ascii", "ignore").split(" "))).persist(StorageLevel.MEMORY_AND_DISK_SER)
	para2[i]=para1[i].map(lambda x :(x[0],B[i]+x[1])).persist(StorageLevel.MEMORY_AND_DISK_SER)

c0=para2[0]
if(i!=0):
	for j in range(1,i+1):
		c0=c0.union(para2[j])
res=c0.map(lambda x :(x[0],[x[1]])).reduceByKey(lambda a,b:sorted(a+b))

#######################################
# Union samples into a feature matrix #
#######################################

def Myfunc_Union(s):
	REFER=B[0:len(tuple_file)]
	j=0
	C=[]
	for i in range(0,len(REFER)):
		if(s[1][j].startswith(REFER[i])):
			C.append(s[1][j][2:])
			j+=1
			if(j>=len(s[1])):
				for m in range(i+1,len(REFER)):
					C.append(0)
				break
		else:C.append(0)
	Line=(s[0]+"*"+str(C))
	line=Line.replace("(","").replace(")","").replace("[","").replace("]","").replace("'","").replace(" ","")
	return line

tuple_forward=res.map(Myfunc_Union).persist(StorageLevel.MEMORY_AND_DISK_SER)

if Union_save=="Y":
	tuple_forward_out=tuple_forward.map(lambda s : s.replace('*','\t').replace(',','\t')).persist(StorageLevel.MEMORY_AND_DISK_SER)
	tuple_forward_out.saveAsTextFile("tuple_union")

#################################################################################
# the function that filter the features that includes 80% 0 in frequency vectors#
#################################################################################

def filter80(s):
	#filter 80%#
	s=s.split("*")
	tuple_value=s[1].split(",")
	group1=tuple_value[:number_h]
	group2=tuple_value[number_h:]
	a=group1.count("0")
	b=group2.count("0")
	if(float(a)/float(len(group1)) >= 0.8 and float(b)/float(len(group2)) >= 0.8):
		return None
	else:
		return s

res_select_80=tuple_forward.filter(lambda s : filter80(s)!=None).persist(StorageLevel.MEMORY_AND_DISK_SER)

if Sparse_save=="Y":
	res_select_80_out=res_select_80.map(lambda s : s.replace('*','\t').replace(',','\t')).persist(StorageLevel.MEMORY_AND_DISK_SER)
	res_select_80_out.saveAsTextFile("filter_sparse")

###############################################################################
# the function that filter the features with T_test and chi2_test or with ASS #
###############################################################################

def filtering(s):
	tuple_list=s.split("*")
	value=tuple_list[1].split(",")
	values=[]
	for i in range(0,len(value)):
		k=float(value[i])/(tuple_number[i]/1000000)
		k=round(k,4)
		values.append(k)
	group1=values[0:number_h]
	group2=values[number_h:]
	c0=group1.count(0.0)
	c1=number_h-c0
	p0=group2.count(0.0)
	p1=number_p-p0
	#chi2_test
	if (functions=='chi2-test'):
		#chi2_test
	   # cc=0.0
		global K2Line
		global cc
		totall=float(number_h+number_p)
		x1=float((p0+p1)/totall*(p1+c1))
		x2=float((p0+p1)/totall*(p0+c0))
		x3=float((c0+c1)/totall*(p1+c1))
		x4=float((c0+c1)/totall*(p0+c0))
		'''
		theorotical value
		'''
		if(x1!=0.0 and x2!=0.0 and x3!=0.0 and x4!=0.0):
			kk=(fabs(p1-x1)-0.5)**2/x1+(fabs(p0-x2)-0.5)**2/x2+(fabs(c1-x3)-0.5)**2/x3+(fabs(c0-x4)-0.5)**2/x4
		else:
			line=("*"+tuple_list[0]+'#'+str(values)+'tp'+str(p))
			return line
		kp=float(chi2.sf(kk,1))
		'''
		output the chi2 value
		'''
		if(kp<=c_value):
			logicalValue=[]
			for i in range(0,len(values)):
				if values[i]==0.0:
					logicalValue.append(0)
				else:
					logicalValue.append(1)
			K2Line=("??"+tuple_list[0]+'#'+str(logicalValue)+'\tkp:'+str(kp))
		else:
			(Zvalue,Pvalue)=stats.ranksums(group1,group2)
			if (Pvalue <= wicxon_p):
				train_Y=[]
				for i in range(0,number_h):
					train_Y.append('H')
				for i in range(0,number_p):
					train_Y.append('P')
				train_X=np.array(values)
				train_X=train_X.reshape(len(train_X),1)
				model = LogisticRegression( C = 1e9 )
				model.fit(train_X, train_Y)
				expected = train_Y
				predicted = model.predict(train_X)
				ConfusionMtrix=metrics.confusion_matrix(expected, predicted)
				cc=0.5*(float(ConfusionMtrix[0,0])/float(number_h)+float(ConfusionMtrix[1,1])/float(number_p))
				if (cc >= LR_p):
					K2Line=("!!"+str(tuple_list[0])+'#'+str(values)+' kp:'+str(kp)+' Wilcoxon_Pvalue:'+str(Pvalue)+' Regress_ASS:'+str(cc))
				else:
					K2Line=s
			else:
				K2Line=s

		Lines=K2Line.replace("(","").replace(")","").replace(","," ").replace("[","").replace("]","").replace("'","").replace('  ',' ').replace(' ','\t').replace('#','\t')
		return Lines

	#ASS test
	elif (functions=='ASS'):
		ASS1=0.5*(float(c0)/number_h+float(p1)/number_p)
		ASS2=0.5*(float(c1)/number_h+float(p0)/number_p)
		ASS=max(ASS1,ASS2)
		if (ASS1>ASS2):
			SampleLabel='P'
		else:
			SampleLabel='H'
		#aa=0.0
		global AUCline
		global aa
		if (ASS >= ass_p):
			logicalValue=[]
			for i in range(0,len(values)):
				if values[i]==0.0:
					logicalValue.append(0)
				else:
					logicalValue.append(1)
			AUCline=("??"+tuple_list[0]+'#'+str(logicalValue)+'\tASS:'+str(ASS)+'\tLabel:'+SampleLabel)
		else:
			(Zvalue,Pvalue)=stats.ranksums(group1,group2)
			if (Pvalue <= wicxon_p):
				train_Y=[]
				for i in range(0,number_h):
					train_Y.append('H')
				for i in range(0,number_p):
					train_Y.append('P')
				train_X=np.array(values)
				train_X=train_X.reshape(len(train_X),1)
				model = LogisticRegression( C = 1e9 )
				model.fit(train_X, train_Y)
				expected = train_Y
				predicted = model.predict(train_X)
				ConfusionMtrix=metrics.confusion_matrix(expected, predicted)
				aa=0.5*(float(ConfusionMtrix[0,0])/float(number_h)+float(ConfusionMtrix[1,1])/float(number_p))


				# replaced tabs with 8 spaces
				mean_AA=np.array(group1).mean()
				mean_BB=np.array(group2).mean()
				if mean_AA>mean_BB:
						LR_label='H'
				else:
						LR_label='P'



				if (aa >= LR_p):
					AUCline=("!!"+str(tuple_list[0])+'#'+str(values)+' Wilcoxon_Pvalue:'+str(Pvalue)+' Regress_ASS:'+str(aa)+' Label:'+LR_label)
				else:
					AUCline=s
			else:
				AUCline=s
		Lines=AUCline.replace("(","").replace(")","").replace(","," ").replace("[","").replace("]","").replace("'","").replace('  ',' ').replace(' ','\t').replace('#','\t')
		return Lines

if functions=='chi2-test':
	res_select_80.map(filtering).filter(lambda s : '??' in s).map(lambda s : s.replace("??","")).saveAsTextFile("Chi2_filtered_down")
	res_select_80.map(filtering).filter(lambda s : '!!' in s).map(lambda s : s.replace("!!","")).saveAsTextFile("WR_filtered_down")
elif functions=='ASS':
	res_select_80.map(filtering).filter(lambda s : '??' in s).map(lambda s : s.replace("??","")).saveAsTextFile("ASS_filtered_down")
	res_select_80.map(filtering).filter(lambda s : '!!' in s).map(lambda s : s.replace("!!","")).saveAsTextFile("WR_filtered_down")

print(label+"yes~~~~~~~~~~~~~~~~~~~~~~")
f.close()
f1.close()
