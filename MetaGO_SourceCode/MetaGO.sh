#!/usr/bin/env bash

set -e  #       exit if any command fails
#set -u  #       Error on usage of unset variables ( too many given the script style )
set -o pipefail

# I don't see the advantage of use PIECE

SCRIPT=$( readlink -f $0 )
#SCRIPTDIR=$( dirname $SCRIPT )
#	when submitting a script to the queue IT IS COPIED
#	this dir will not include other things that were in the same dir
SCRIPTDIR=$HOME/github/ucsffrancislab/MetaGO/MetaGO_SourceCode


ARGS=`getopt -a -o I:F:N:M:K:m:P:C:A:X:L:W:O:USZh -l inputData:,fileList:,n1:,n2:,kMer:,min:,Piece:,K2test:,ASS:,WilcoxonTest:,LogicalRegress:,filterFuction:,outputPath:,Union,sparse,cleanUp,help -- "$@"`
[ $? -ne 0 ] && usage
#set -- "${ARGS}"
eval set -- "${ARGS}"

echo "Filenames parsed on .'s SAMPLENAME.FILETYPE.FILETYPE2"
echo "Where FILETYPE is fasta and FILETYPE2 would be gz, if present"
echo "DO NOT USE ANY OTHER .'s"

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

#	What is the 'aaa' for?
#	Ttest is never used. Can I remove?
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
	echo " ERROR!! Please choose a correct inputdata, you can choose 'RAW' or 'MATRIX' "
	exit
fi

if [ -n "$FileList" -a "$InputData" = 'MATRIX' ]||[ -n "$KMER" -a "$InputData" = 'MATRIX' ]||[ -n "$MIN" -a "$InputData" = 'MATRIX' ]||[ -n "$saveUnion" -a "$InputData" = 'MATRIX' ]||[ -n "$Clean" -a "$InputData" = 'MATRIX' ]; then
	echo "ERROR!! You should choose a correct parameter! For the inputdata you have chosen 'MATRIX', plese don't choose fileList kMer min and Union parameter!"
	exit
fi

if [ -n "$K2test" -a "$filterFuction" = 'ASS' ]||[ -n "$ASS" -a "$filterFuction" = 'chi2-test' ]; then
	echo "ERROR! You should choose a right parameter! If you choose 'ASS' as the filterFuction, you can not choose K2test, and if you choose 'chi2-test' as the filterFuction, you can not choose ASS"
	exit
fi

if [ "$InputData" = 'RAW' ]; then
	if [ -z $FileList ]||[ -z $N1 ]||[ -z $N2 ]||[ -z $KMER ]; then
		echo "ERROR!!Lack of parameter!!"
		exit
	fi
else
	if [ -z $N1 ]||[ -z $N2 ]; then
		echo "ERROR!!Lack of parameter!!"
		exit
	fi
fi

if [ "$filterFuction" != 'ASS' -a  "$filterFuction" != 'chi2-test' ]; then
	echo " ERROR!! Please choose a correct funtion for filtering, you can shoose 'chi2-test' or 'ASS' "
	exit
fi

if [ "$Piece" != 16 -a "$Piece" != 8 -a "$Piece" != 4 -a "$Piece" != 1 ]; then
	echo "ERROR! You can only choose 16 8 4 or 1(no split) for -P "
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


nb_cores=$( getconf _NPROCESSORS_ONLN )
echo
echo "Using ${nb_cores} for parallelization"
echo

#grep MemTotal /proc/meminfo
#MemTotal:       125816292 kB
#100G
#mem=$( awk '( $1 == "MemTotal:" ){split((0.8*($2))/1000000,a,".");print(a[1]"G")}' /proc/meminfo )
#mem=$( awk '( $1 == "MemTotal:" ){split(($2/1000000)-15,a,".");print(a[1]"G")}' /proc/meminfo )
# for full n38 server using 504GB, need less than this
#mem=$( awk '( $1 == "MemTotal:" ){split(($2/1024000)-15,a,".");print(a[1]"G")}' /proc/meminfo )
mem=$( awk '( $1 == "MemTotal:" ){split(0.9*($2/1024000)-15,a,".");print(a[1]"G")}' /proc/meminfo )

# this should be converted into a command line option
#	something is still crossing the line on a cluster node even when this is set very low

echo
echo "Using ${mem} memory"
echo

#Put
#--driver-cores NUM
#--driver-memory MEM
#on the spark-submit command line and remove from conf file?

if [ "$InputData" = 'RAW' ]; then
	#################
	# get tupleFile #
	#################

	if [ ! -f ${OUT}/group1File.txt ] ; then
		head -n $n1 $FileList > ${OUT}/group1File.txt
	fi

	if [ ! -f ${OUT}/group2File.txt ] ; then
		tail -n $n2 $FileList > ${OUT}/group2File.txt
	fi

	mkdir -p ${OUT}/G1_tupleFile ${OUT}/G2_tupleFile

	for g in 1 2 ; do

		for iterm in $( cat ${OUT}/group${g}File.txt ); do
			echo "iterm :${iterm}:"

			#	example
			#	iterm=/mnt/ssd0/MetaGO_S3_20200407_Schizophrenia/Control-SD14-unmapped.fasta.gz

			sampleName=$( echo $iterm|awk -F "/" '{print $NF}'|awk -F"." '{print $1}' )
			fileType=$( echo $iterm|awk -F "/" '{print $NF}'|awk -F"." '{print $2}' )
			fileType2=$( echo $iterm|awk -F "/" '{print $NF}'|awk -F"." '{print $3}' )
			echo $sampleName
			echo $fileType
			echo $fileType2

			if [[ "$fileType" = 'sra' ]]; then
				fastq-dump --split-spot $iterm --fasta --gzip
				dskbase=$sampleName.fasta.gz
				#	sampleName does not include the path ( I don't use sra so can't test at the moment )
			else
				#	iterm does (or can include the full path)
				dskbase=$iterm
			fi
			echo "dskbase :${dskbase}:"

			h5=${OUT}/$( basename ${dskbase} ).h5
			dskascii=${OUT}/G${g}_tupleFile/${sampleName}_k_${KMER}.txt
			if [ ! -f ${dskascii} ] ; then
				if [ ! -f ${h5} ] ; then
					echo "Writing h5:${h5}:"
					echo "dsk -nb-cores ${nb_cores} -file ${dskbase} -kmer-size $KMER -abundance-min $MIN -out ${h5}"
					dsk -nb-cores ${nb_cores} -file ${dskbase} -kmer-size $KMER -abundance-min $MIN -out ${h5}
				fi
				echo "Writing out:${dskascii}:"
				echo "dsk2ascii -nb-cores ${nb_cores} -file ${h5} -out ${dskascii}"
				dsk2ascii -nb-cores ${nb_cores} -file ${h5} -out ${dskascii}
				rm ${h5}
			fi

		done
	done	#	for g in 1 2 ; do

	#	keep for now
	#rm ${OUT}/group1File.txt ${OUT}/group2File.txt

	if  [[ "$fileType" = 'sra' ]]; then
		mkdir ${OUT}/fastaFile
		mv ${OUT}/*.fasta.gz ${OUT}/fastaFile/
		if [[ "$Clean" = "Y" ]]; then
			rm -r ${OUT}/fastaFile/
		#else
		#	mv fastaFile/ $OUT
		fi
	fi

	#####################
	# Count tupleNumber #
	#####################


	#	parallelize this with parallel
	#	I think that the order of this file matters so don't muck it up.
	echo "Summing"
	echo "${OUT}/G?_tupleFile/*.txt"
	for f in ${OUT}/G?_tupleFile/*.txt ; do
		#	MUST ESCAPE THE DOLLAR SIGN!!!
		if [ ! -f ${f}.sum ] ; then
			echo "awk '{sum+=\$2};END{print sum}' $f > ${f}.sum"
		fi
	done | parallel
	echo "Summed"

	echo "Concatenating sums"
	for i in 1 2 ; do
		cd ${OUT}/G${i}_tupleFile/
		echo -n > ${OUT}/group${i}TupleNumber.txt
		# will be in different order than initial file list ?? Does this matter?
		if [ -n "$( ls ${OUT}/G${i}_tupleFile/*.txt )" ] ; then
			ls ${OUT}/G${i}_tupleFile/*.txt > ${OUT}/Group${i}FileList.txt
			for iterm in $( cat ${OUT}/Group${i}FileList.txt ); do
				cat ${iterm}.sum >> ${OUT}/group${i}TupleNumber.txt
			done
		else
			touch ${OUT}/Group${i}FileList.txt
		fi
		cd ${OUT}
	done
	echo "Concatenated"

	cat ${OUT}/group1TupleNumber.txt ${OUT}/group2TupleNumber.txt > ${OUT}/TupleNumber.txt
	#	keep for now
	#rm ${OUT}/group1TupleNumber.txt ${OUT}/group2TupleNumber.txt


	###################
	# split tupleFile #
	###################
	if [[ "$PICECE" -ne 1 ]]; then

		for i in 1 2 ; do
			cd ${OUT}/G${i}_tupleFile/
			bash ${SCRIPTDIR}/split_tupleData.sh ${OUT}/Group${i}FileList.txt $PICECE
			cd splited_file/
			for k in $( seq 1 $PICECE); do
				ls *_$k.txt > ../../Group${i}FileList_$k.txt
			done
			mv * ../../
			cd ../
			#	remove empty dirs
			#rm -r splited_file/
			#rm -r temporary_files/
			rmdir splited_file/
			rmdir temporary_files/
			cd ..
		done

		#	keep for now
#		if [[ "$Clean" = "Y" ]]; then
#			rm -r ${OUT}/G1_tupleFile/ ${OUT}/G2_tupleFile/
#		else
#	Should be there
#			mv G1_tupleFile/ $OUT
#			mv G2_tupleFile/ $OUT
#		fi
	fi

fi

spark_submit="spark-submit --properties-file ${SCRIPTDIR}/spark.conf --driver-cores ${nb_cores} --driver-memory ${mem}"
spark_common="-m $N1 -n $N2 -c $K2_theta -t $ASS_theta -x $Wilcoxon_theta -l $LR_theta -w $WAY"
sparkUnionFilter="${spark_submit} ${SCRIPTDIR}/sparkUnionFilter.py -r ${OUT}/TupleNumber.txt ${spark_common} -u $saveUnion -s $saveFilter80"

sparkFilterOnly="${spark_submit} ${SCRIPTDIR}/sparkFilterOnly.py -r $OUT/TupleNumber.txt ${spark_common}"

if [[ "$PICECE" -ne 1 ]]; then
	echo "Merging in multple pieces"
	for k in $( seq 1 $PICECE); do
		if [ "$InputData" = 'RAW' ]; then
			cat ${OUT}/Group1FileList_$k.txt ${OUT}/Group2FileList_$k.txt > ${OUT}/TupleFileList_$k.txt
			${sparkUnionFilter} -f ${OUT}/TupleFileList_$k.txt

			if [[ "$saveUnion" = "Y" ]]; then
				mv tuple_union tuple_union_$k
				#mv tuple_union_$k $OUT
			fi

			if [[ "$saveFilter80" = "Y" ]]; then
				mv filter_sparse filter_sparse_$k
				#mv filter_sparse_$k $OUT
			fi

		else
			${sparkFilterOnly} -f $OUT/filter_sparse_$k/
		fi

		if [[ "$WAY" = "ASS" ]]; then
			mv ASS_filtered_down ASS_filtered_down_$k
			#mv ASS_filtered_down_$k $OUT
			mv WR_filtered_down WR_filtered_down_$k
			#mv WR_filtered_down_$k $OUT
		else
			mv Chi2_filtered_down Chi2_filtered_down_$k
			#mv Chi2_filtered_down_$k $OUT
			mv WR_filtered_down WR_filtered_down_$k
			#mv WR_filtered_down_$k $OUT
		fi
	done

else
	if [ "$InputData" = 'RAW' ]; then
		echo "Merging in one piece"
		cat ${OUT}/Group1FileList.txt ${OUT}/Group2FileList.txt > ${OUT}/TupleFileList.txt
# WHY KEEP MOVING FILES AROUND. Keep files in the G?_tupleFile
#		cd G1_tupleFile
#		mv * ../
#		cd ../G2_tupleFile
#		mv * ../
#		cd ../
		if [ ! -d ${OUT}/tuple_union ] ; then
			echo "Running union filter"
			echo ${sparkUnionFilter} -f ${OUT}/TupleFileList.txt
			${sparkUnionFilter} -f ${OUT}/TupleFileList.txt
			echo "Done running union filter"
		fi
		#	keep for now
#		rm TupleFileList.txt

		#	both should be in $OUT already
		#if [[ "$saveUnion" = "Y" ]]; then
		#	mv tuple_union/ $OUT
		#fi
		#if [[ "$saveFilter80" = "Y" ]]; then
		#	mv filter_sparse/ $OUT
		#fi
	else
		${sparkFilterOnly} -f $OUT/filter_sparse/
	fi

	#	Hopefully in $OUT already
	#if [[ "$WAY" = "ASS" ]]; then
	#	mv ASS_filtered_down/ $OUT
	#	mv WR_filtered_down/ $OUT
	#else
	#	mv Chi2_filtered_down/ $OUT
	#	mv WR_filtered_down/ $OUT
	#fi

fi


# already in OUT/
#if [ "$InputData" = 'RAW' ]; then
#	mv TupleNumber.txt $OUT
#fi

############################################
# move all intermediate files in documents #
############################################

#	Why? They should be created and kept in the appropriate spot
#
#	if [ "$InputData" = 'RAW' ]; then
#		if [[ "$PICECE" -ne 1 ]]; then
#			mkdir Group1splitedFile/ Group2splitedFile/
#			for k in $( seq 1 $PICECE); do
#				mv `cat Group1FileList_$k.txt` Group1splitedFile/
#				mv `cat Group2FileList_$k.txt` Group2splitedFile/
#				mv Group1FileList_$k.txt Group1splitedFile/
#				mv Group2FileList_$k.txt Group2splitedFile/
#				#mv Group1FileList.txt Group1splitedFile/
#				#mv Group2FileList.txt Group2splitedFile/
#				rm TupleFileList_$k.txt
#			done
#			mv Group1FileList.txt Group1splitedFile/
#			mv Group2FileList.txt Group2splitedFile/
#		else
#			mv `cat Group1FileList.txt` G1_tupleFile/
#			mv `cat Group2FileList.txt` G2_tupleFile/
#			mv Group1FileList.txt G1_tupleFile/
#			mv Group2FileList.txt G2_tupleFile/
#		fi
#
#		if [[ "$PICECE" -ne 1 ]]; then
#			if [[ "$Clean" = "Y" ]]; then
#				rm -r Group1splitedFile/ Group2splitedFile/
#			else
#				mv Group1splitedFile/ $OUT
#				mv Group2splitedFile/ $OUT
#			fi
#		else
#			if [[ "$Clean" = "Y" ]]; then
#				rm -r G1_tupleFile/ G2_tupleFile/
#			#	already there
#			#else
#			#	mv G1_tupleFile/ $OUT
#			#	mv G2_tupleFile/ $OUT
#			fi
#		fi
#	fi
