import os,sys
import optparse
import string
import copy
import time

prog_base=os.path.split(sys.argv[0])[1]

parser=optparse.OptionParser()
parser.add_option("-s", "--fileSize", action = "store", type = "int", dest="splitedNumber",
	help="the totall number of splied")
parser.add_option("-f", "--SortFiles", action = "store", type = "string", dest="sortFile",
	help="the file need to be sorted")
parser.add_option("-p","--outputPath",action="store",type="string",dest="fileOutputPath",default='./',
	help="the folder to save the filtered tuple files")
(options,args)=parser.parse_args()
if(options.splitedNumber is None or options.sortFile is None):
	print( prog_base+":error: Missing the required command-line argument." )
	parser.print_help()
	sys.exit(0)

FileSpliedNumber=options.splitedNumber
fileName=options.sortFile
outputPath=options.fileOutputPath

if outputPath is None:
	outputPath='./'
#fileName='HV06_Ra-R_U_Ss_sebaceous_sorted.txt'
List4=['A','C','G','T']
List8=[['AA','AC'],['AG','AT'],['CA','CC'],['CG','CT'],['GA','GC'],['GG','GT'],['TA','TC'],['TG','TT']]
List16=['AA','AC','AG','AT','CA','CC','CG','CT','GA','GC','GG','GT','TA','TC','TG','TT']
if FileSpliedNumber==4:
	baseList=List4
elif FileSpliedNumber==8:
	baseList=List8
else:
	baseList=List16
filename=fileName.split('.')[0]
print(baseList)
#sortedList=[]
#totalCount=0.0

def writeFile(i):
	j=1
	for base in baseList:
		#tupleList=[]
		inputFile=open(fileName,"r")

		outputFile=open(outputPath+filename+'_'+str(j)+'.txt',"w")
		while True:
			line=inputFile.readline()
			if line:
				if line[0:i]==base:
					outputFile.write(line)
				else:
					continue
			else:
				break
		inputFile.close()
		print(base+' is done~')
		j=j+1

def writeFilesplit8(i):
	j=1
	for base in baseList:
		#tupleList=[]
		inputFile=open(fileName,"r")

		outputFile=open(outputPath+filename+'_'+str(j)+'.txt',"w")
		while True:
			line=inputFile.readline()
			if line:
				if line[0:i]==base[0] or line[0:i]==base[1]:
					outputFile.write(line)
				else:
					continue
			else:
				break
		inputFile.close()
		print(str(base)+' is done~')
		j=j+1

if baseList==List4:
	writeFile(1)
elif baseList==List8:
	writeFilesplit8(2)
else:
	writeFile(2)

