#!/usr/bin/env bash

ARGS=`getopt -a -o I:F:N:M:K:m:P:C:A:X:L:W:O:USZh -l inputData:,fileList:,n1:,n2:,kMer:,min:,Piece:,K2test:,ASS:,WilcoxonTest:,LogicalRegress:,filterFuction:,outputPath:,Union,sparse,cleanUp,help -- "$@"`  
[ $? -ne 0 ] && usage  
#set -- "${ARGS}"  
eval set -- "${ARGS}" 
 
while true ; do  
	case "$1" in
		-I|--inputData)
			inputData="$2" #input data,raw data or union matrix
			shift
			;;
		-F|--fileList)   #inputFile List which includes all sampleFiles' absolute path name
			fileList="$2" 
			shift  
			;;  
		-N|--n1)  
			n1="$2"  #The totall samples in group1
			shift  
			;;  
		-M|--n2)  
			n2="$2"  #The totall samples in group2
			shift  
			;;  
		-K|--kMer)  
			kMer="$2"  #The length of tuple
			shift  
			;;  
		-m|--min)  
			min="$2" #The minin frequency of tuple
			shift  
			;;
		-P|--Piece):
			Piece="$2" #The totall pieces of every tupleFile splited into
			shift
			;;  
		-C|--K2test)  
			K2test="$2" #The chi2 test p value threshold
			;;  
		-A|--ASS)   
			ASS="$2" #The AUC test AUC threshold
			;;  
		-X|--WilcoxonTest)
			WilcoxonTest="$2"
			;;
		-L|--LogicalRegress)
			LogicalRegress="$2"
			;;
		-W|--filterFuction)   
			filterFuction="$2" #The filter function TK or AUC
			;;  
		-O|--outputPath)  
			outputPath="$2" #Output files' absolute path name
			;;  
		-U|--Union)  
			UNION="Y" #Whether save Unioned files
			;;
		-S|--sparse)
			SPARSE="Y" #Whether save sparaseed Unioned files
			;;
		-Z|--cleanUp)
			CLEANUP="Y" #Whether clean up all intermediate documents
			;;  
		-h|--help)  
			usage  
			;;  
		--)  
			shift  
			break 
			;;  
	esac  
	shift  
done 


####################################
# initialization prepare fileLists #
####################################
echo $fileList $n1 $n2 $kMer $min $Piece $Ttest $K2test $ASS $filterFuction $outputPath $UNION 'aaa' $SPARSE $CLEANUP
InputData=$inputData
FileList=$fileList
N1=$n1
N2=$n2
KMER=$kMer
MIN=$min
WAY=$filterFuction
OUT=$outputPath
PICECE=$Piece
K2_theta=$K2test
ASS_theta=$ASS
Wilcoxon_theta=$WilcoxonTest
LR_theta=$LogicalRegress
saveUnion=$UNION
saveFilter80=$SPARSE
Clean=$CLEANUP



echo $InputData

if [ "$InputData" != 'RAW' -a "$InputData" != 'MATRIX' ]; then
	echo " ERRO!! Please choose a correct inputdata, you can choose 'RAW' or 'MATRIX' "
	exit
fi

if [ -n "$FileList" -a "$InputData" = 'MATRIX' ]||[ -n "$KMER" -a "$InputData" = 'MATRIX' ]||[ -n "$MIN" -a "$InputData" = 'MATRIX' ]||[ -n "$saveUnion" -a "$InputData" = 'MATRIX' ]||[ -n "$Clean" -a "$InputData" = 'MATRIX' ]; then
	echo "ERRO!! You should choose a correct parameter! For the inputdata you have chosen 'MATRIX', plese don't choose fileList kMer min and Union parameter!"
	exit
fi

if [ -n "$K2test" -a "$filterFuction" = 'ASS' ]||[ -n "$ASS" -a "$filterFuction" = 'chi2-test' ]; then
	echo "ERRO! You should choose a right parameter! If you choose 'ASS' as the filterFuction, you can not choose K2test, and if you choose 'chi2-test' as the filterFuction, you can not choose ASS" 
	exit
fi

if [ "$InputData" = 'RAW' ]; then
	if [ -z $FileList ]||[ -z $N1 ]||[ -z $N2 ]||[ -z $KMER ]; then
		echo "ERRO!!Lack of parameter!!"
		exit
	fi
else
	if [ -z $N1 ]||[ -z $N2 ]; then 
		echo "ERRO!!Lack of parameter!!"
		exit
	fi
fi  

if [ "$filterFuction" != 'ASS' -a  "$filterFuction" != 'chi2-test' ]; then
	echo " ERRO!! Please choose a correct funtion for filtering, you can shoose 'chi2-test' or 'ASS' "
	exit
fi

if [ "$Piece" != 16 -a "$Piece" != 8 -a "$Piece" != 4 -a "$Piece" != 1 ]; then
	echo "ERRO! You can only choose 16 8 4 or 1(no split) for -P "
	exit
fi

if [ "$InputData" = 'RAW' ]; then
	if [ -z $MIN ]; then
		MIN=2
	fi
	if [ -z $KMER ]; then
		KMER=40
	fi
fi

if [ -z $PICECE ]; then
	PICECE=1
fi

if [ -z $SPATSE_theta ]; then
	SPATSE_theta=0.8
fi

if [ -z $WAY ]; then
	WAY='ASS'
fi

if [ -z $K2_theta ]; then
	K2_theta=0.01
fi

if [ -z $ASS_theta ]; then
	ASS_theta=0.8
fi

if [ -z $Wilcoxon_theta ]; then
	Wilcoxon_theta=0.01
fi

if [ -z $LR_theta ]; then
	LR_theta=0.8
fi

if [ "$InputData" = 'RAW' ]; then
	if [ -z $saveUnion ]; then
		saveUnion="N"
	fi
fi

if [ -z $saveFilter80 ]; then
	saveFilter80='N'
fi

if [ -z $OUT ]; then
	OUT='./'
fi

if [ "$InputData" = 'RAW' ]; then
	if [ -z $Clean ]; then
		Clean='N'
	fi
fi

echo $PICECE $K2_theta $ASS_theta $saveUnion $saveFilter80 $Clean




if [ "$InputData" = 'RAW' ]; then
	#################
	# get tupleFile #
	#################

	head -n $n1 $FileList > group1File.txt
	tail -n $n2 $FileList > group2File.txt

	mkdir G1_tupleFile G2_tupleFile

	nb_cores=$( getconf _NPROCESSORS_ONLN )
	echo "Using ${nb_cores} for dsk"

	for g in 1 2 ; do

		for iterm in $( cat group${g}File.txt ); do

			#	example
			#	iterm=/mnt/ssd0/MetaGO_S3_20200407_Schizophrenia/Control-SD14-unmapped.fasta.gz

			sampleName=`echo $iterm|awk -F "/" '{print $NF}'|awk -F"." '{print $1}'`
			fileType=`echo $iterm|awk -F "/" '{print $NF}'|awk -F"." '{print $2}'`
			fileType2=`echo $iterm|awk -F "/" '{print $NF}'|awk -F"." '{print $3}'`
			echo $sampleName
			echo $fileType

			if [[ "$fileType" = 'sra' ]]; then
				fastq-dump --split-spot $iterm --fasta --gzip
				dskbase=$sampleName.fasta.gz
			else
				dskbase=$iterm
			fi    
			dsk -nb-cores ${nb_cores} -file ${dskbase} -kmer-size $KMER -abundance-min $MIN -out ${dskbase}.h5
			dsk2ascii -nb-cores ${nb_cores} -file ${dskbase}.h5 -out G${g}_tupleFile/$sampleName"_k_"$KMER".txt"
			rm ${dskbase}.h5

		done
	done	#	for g in 1 2 ; do	

	rm group1File.txt group2File.txt


	if  [[ "$fileType" = 'sra' ]]; then
		mkdir fastaFile
		mv *.gz fastaFile/ 
		if [[ "$Clean" = "Y" ]]; then
			rm -r fastaFile/
		else
			mv fastaFile/ $OUT
		fi
	fi

	#####################
	# Count tupleNumber #
	#####################


	#	parallelize this with parallel
	#	I think that the order of this file matters so don't muck it up.
	#	for f in G?_tupleFile/*.txt ; do
	#		echo "awk '{sum+=$2};END{print sum}' $f > ${f}.sum.txt"
	#	done | parallel
	#	for i in 1 2 ; do
	#		cd G${i}_tupleFile/
	#		ls *.txt > ../Group${i}FileList.txt
	#		for iterm in $( cat ../Group${i}FileList.txt ); do
	#			###awk '{sum+=$2};END{print sum}' $iterm >> ../group1TupleNumber.txt
	#			cat ${iterm}.sum.txt >> ../group${i}TupleNumber.txt
	#		done
	#		cd ../
	#	done

	cd G1_tupleFile/
	ls *.txt > ../Group1FileList.txt
	for iterm in $( cat ../Group1FileList.txt ); do
		awk '{sum+=$2};END{print sum}' $iterm >> ../group1TupleNumber.txt
	done
	cd ../

	cd G2_tupleFile/
	ls *.txt > ../Group2FileList.txt
	for iterm in $( cat ../Group2FileList.txt ); do
		awk '{sum+=$2};END{print sum}' $iterm >> ../group2TupleNumber.txt
	done
	cd ../

	cat group1TupleNumber.txt group2TupleNumber.txt > TupleNumber.txt
	rm group1TupleNumber.txt group2TupleNumber.txt


	###################
	# split tupleFile #
	###################
	if [[ "$PICECE" -ne 1 ]]; then
		mv split_tupleData.py G1_tupleFile/
		mv split_tupleData.sh G1_tupleFile/
		cd G1_tupleFile/
		bash split_tupleData.sh ../Group1FileList.txt $PICECE 
		mv split_tupleData.py ../G2_tupleFile/
		mv split_tupleData.sh ../G2_tupleFile/
		cd splited_file/
		for k in $( seq 1 $PICECE); do
			ls *_$k.txt > ../../Group1FileList_$k.txt
		done
		mv * ../../
		cd ../
		rm -r splited_file/
		rm -r temporary_files/

		cd ../G2_tupleFile/
		bash split_tupleData.sh ../Group2FileList.txt $PICECE
		mv split_tupleData.py ../
		mv split_tupleData.sh ../
		cd splited_file/
		for k in $( seq 1 $PICECE); do
			ls *_$k.txt > ../../Group2FileList_$k.txt
		done
		mv * ../../
		cd ../
		rm -r splited_file/
		rm -r temporary_files/
		cd ../

		if [[ "$Clean" = "Y" ]]; then 
			rm -r G1_tupleFile/ G2_tupleFile/
		else
			mv G1_tupleFile/ $OUT
			mv G2_tupleFile/ $OUT
		fi
	fi

fi

if [[ "$PICECE" -ne 1 ]]; then
	for k in $( seq 1 $PICECE); do
		if [ "$InputData" = 'RAW' ]; then
			cat Group1FileList_$k.txt Group2FileList_$k.txt > TupleFileList_$k.txt
			spark-submit --properties-file ./spark.conf sparkUnionFilter.py -f TupleFileList_$k.txt -r TupleNumber.txt -m $N1 -n $N2 -c $K2_theta -t $ASS_theta -x $Wilcoxon_theta -l $LR_theta -w $WAY -u $saveUnion -s $saveFilter80

			if [[ "$saveUnion" = "Y" ]]; then
				mv tuple_union tuple_union_$k
				mv tuple_union_$k $OUT
			fi

			if [[ "$saveFilter80" = "Y" ]]; then 
				mv filter_sparse filter_sparse_$k
				mv filter_sparse_$k $OUT
			fi

		else
			spark-submit --properties-file ./spark.conf sparkFilterOnly.py -f $OUT/filter_sparse_$k/ -r $OUT/TupleNumber.txt -m $N1 -n $N2 -c $K2_theta -t $ASS_theta -x $Wilcoxon_theta -l $LR_theta -w $WAY
		fi

		if [[ "$WAY" = "ASS" ]]; then
			mv ASS_filtered_down ASS_filtered_down_$k
			mv ASS_filtered_down_$k $OUT
			mv WR_filtered_down WR_filtered_down_$k
			mv WR_filtered_down_$k $OUT
		else
			mv Chi2_filtered_down Chi2_filtered_down_$k
			mv Chi2_filtered_down_$k $OUT
			mv WR_filtered_down WR_filtered_down_$k
			mv WR_filtered_down_$k $OUT
		fi
	done

else
	if [ "$InputData" = 'RAW' ]; then
		cat Group1FileList.txt Group2FileList.txt > TupleFileList.txt
		cd G1_tupleFile
		mv * ../
		cd ../G2_tupleFile
		mv * ../
		cd ../
		spark-submit --properties-file ./spark.conf sparkUnionFilter.py -f TupleFileList.txt -r TupleNumber.txt -m $N1 -n $N2 -c $K2_theta -t $ASS_theta -x $Wilcoxon_theta -l $LR_theta -w $WAY -u $saveUnion -s $saveFilter80
		rm TupleFileList.txt

		if [[ "$saveUnion" = "Y" ]]; then
			mv tuple_union/ $OUT
		fi
       
		if [[ "$saveFilter80" = "Y" ]]; then
			mv filter_sparse/ $OUT
		fi
	else
		spark-submit --properties-file ./spark.conf sparkFilterOnly.py -f $OUT/filter_sparse/ -r $OUT/TupleNumber.txt -m $N1 -n $N2 -c $K2_theta -t $ASS_theta -x $Wilcoxon_theta -l $LR_theta -w $WAY
	fi

	if [[ "$WAY" = "ASS" ]]; then
		mv ASS_filtered_down/ $OUT
		mv WR_filtered_down/ $OUT
	else
		mv Chi2_filtered_down/ $OUT
		mv WR_filtered_down/ $OUT
	fi

fi
if [ "$InputData" = 'RAW' ]; then
	mv TupleNumber.txt $OUT
fi

############################################
# move all intermediate files in documents #
############################################

if [ "$InputData" = 'RAW' ]; then
	if [[ "$PICECE" -ne 1 ]]; then
		mkdir Group1splitedFile/ Group2splitedFile/
		for k in $( seq 1 $PICECE); do
			mv `cat Group1FileList_$k.txt` Group1splitedFile/
			mv `cat Group2FileList_$k.txt` Group2splitedFile/
			mv Group1FileList_$k.txt Group1splitedFile/
			mv Group2FileList_$k.txt Group2splitedFile/
			#mv Group1FileList.txt Group1splitedFile/
			#mv Group2FileList.txt Group2splitedFile/
			rm TupleFileList_$k.txt
		done
		mv Group1FileList.txt Group1splitedFile/
		mv Group2FileList.txt Group2splitedFile/
	else
		mv `cat Group1FileList.txt` G1_tupleFile/
		mv `cat Group2FileList.txt` G2_tupleFile/
		mv Group1FileList.txt G1_tupleFile/
		mv Group2FileList.txt G2_tupleFile/
	fi

	if [[ "$PICECE" -ne 1 ]]; then 
		if [[ "$Clean" = "Y" ]]; then
			rm -r Group1splitedFile/ Group2splitedFile/
		else
			mv Group1splitedFile/ $OUT
			mv Group2splitedFile/ $OUT
		fi
	else
		if [[ "$Clean" = "Y" ]]; then 
			rm -r G1_tupleFile/ G2_tupleFile/
		else
			mv G1_tupleFile/ $OUT
			mv G2_tupleFile/ $OUT
		fi
	fi
fi
