#filename unionandfilter
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
from math import fabs
from scipy.stats import ttest_ind , ttest_ind_from_stats
from scipy.special import stdtr
import random
import numpy as np
from pyspark import SparkContext
from sklearn import metrics
from sklearn.linear_model import LogisticRegression
import scipy
from scipy import stats
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
parser.add_option("-t", "--AUC_thresh", action = "store", type = "float", dest="auc_theta",default=0.8,
                 help="the threshold of AUC")
parser.add_option("-x", "--Wicxon_thresh", action = "store", type = "float", dest="wicxon_theta",default=0.05,
                 help="the threshold of wicxon_test")
parser.add_option("-l", "--LR_thresh", action = "store", type = "float", dest="logicalRegress_theta",default=0.8,
                 help="the threshold of LogicalRegress_test")
parser.add_option("-w", "--filterWay", action = "store", type = "string", dest="filter_way",
                 help="the way of filtering")


(options,args)=parser.parse_args()

if(options.Unionfile is None or
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
auc_p=options.auc_theta
c_value=options.c_value
wicxon_p=options.wicxon_theta
LR_p=options.logicalRegress_theta
functions=options.filter_way


#function that filter tuple with T_test and chi2_test or AUC test

tuple_number=[]
f_number=open(file_tuple_number,'r')
tuple_number_list=f_number.readlines()
for lines in tuple_number_list:
        tuple_number.append(float(lines))

print(tuple_number)

f_number.close()


sc = SparkContext("local[10]", "First Spark App")
res_select_80=sc.textFile(file_in,use_unicode=False).persist(StorageLevel.MEMORY_AND_DISK_SER)

def filtering(s):
    tuple_list=s.split('\t')
    value=tuple_list[1:]
    values=[]
    logicalValue=[]
    for i in range(0,len(value)):
        if value[i]=='0':
            logicalValue.append(0)
        else:
            logicalValue.append(1)
        #print(value)
    for i in range(0,len(value)):
        k=float(value[i])/(tuple_number[i]/1000000)
        k=round(k,4)
        values.append(k)
    group1=values[:number_h]
    group2=values[number_h:]
    c0=group1.count(0.0)
    c1=number_h-c0
    p0=group2.count(0.0)
    p1=number_p-p0

    if (functions=='chi2-test'):
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

    #AUC test
    elif (functions=='ASS'):
        ASS1=0.5*(float(c0)/number_h+float(p1)/number_p)
        ASS2=0.5*(float(c1)/number_h+float(p0)/number_p)
        ASS=max(ASS1,ASS2)
        if (ASS1>ASS2):
            SampleLabel='P'
        else:
            SampleLabel='H'
        global AUCline
        global aa
        if (ASS >= auc_p):
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

print("yes~~~~~~~~~~~~~~~~~~~~~~")
