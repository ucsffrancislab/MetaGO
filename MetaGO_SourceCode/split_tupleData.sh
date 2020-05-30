#!/usr/bin/env bash

fileList=`cat $1`
#for item in $fileList
#do
#echo $item
#python sample_normal.py  -f $item -p ./
#rm $item -rf
#done
#fileSize=`du -sh $fileList`
mkdir temporary_files/
mv $fileList temporary_files/
#mv split_tupleData.py temporary_files/
#fileSize=`du -sh $temporary_files`
#echo  $fileSize

pieces=$2

echo  $pieces
mkdir splited_file
cd temporary_files/
echo $PWD
for item2 in *.txt ; do
	#echo $item2
	#fileSize=`du -s $item2`
	#echo $fileSize
	echo "python ../../split_tupleData.py -s $pieces -f $item2 -p ../splited_file/"
	#python split_tupleData.py -s $pieces -f $item2 -p ../splited_file/
	#rm $item2 -rf
done | parallel

#mv *.txt *.py ../
mv *.txt ../
cd ../

