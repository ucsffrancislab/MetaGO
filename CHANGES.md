
#	Upcoming Changes

Upgrade to the latest version of dsk (2.3.3) to control the number of cores

https://github.com/GATB/dsk/releases
https://github.com/GATB/dsk/releases/download/v2.3.3/dsk-v2.3.3-bin-Linux.tar.gz

Upgrade to python3
Needed to add parentheses to a couple print statements
Needed to replace some tabs with spaces


Can't actually get MetaGO to run with python3.
Hadoop or spark are not happy.
Downgraded to python 2.7 for the moment. Still working.





##	20201013

Using more absolute paths so can run in data dir, not in software dir

Stop deleting files and moving them around

Note that upgrading pyspark above 3 will require changes in the python scripts.
MEMORY_AND_DISK_SER = StorageLevel(True, True, False, False, 1)

I'm working with pip install --upgrade --user pyspark==2.4.5

Define spark.local.dir for large datasets. /tmp was too small. /scratch has 2TB.






##	20200602

Make number of cores used by Spark dynamic.
In MetaGO.bash?
Options to or from within the python script?
In spark.conf?

Should do this in just one place and that's probably the main MetaGO.bash script.

nb_cores=$( getconf _NPROCESSORS_ONLN )

grep MemTotal /proc/meminfo
MemTotal:       125816292 kB
100G

mem=$( awk '( $1 == "MemTotal:" ){split((0.8*($2))/1000000,a,".");print(a[1]"G")}' /proc/meminfo )


Put 
--driver-cores NUM
--driver-memory MEM 
on the spark-submit command line and remove from conf file?



parallelize some loops with parallel?



