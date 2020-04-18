#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

#
# Get the job cache directory
#
. ./job-cache-env.sh

#
# Get the classpath
#
. ./ingest-libs.sh

if [[ -z $DATAWAVE_INGEST_HOME ]]; then
  export DATAWAVE_INGEST_HOME=$THIS_DIR/../..
fi

#
# Capture only the ingest config files
#
declare -a INGEST_CONFIG
i=0
for f in ../../config/*-config.xml; do
  INGEST_CONFIG[i++]=`basename $f`
done

#
# Transform the classpath into a comma-separated list also
#
LIBJARS=`echo $CLASSPATH | sed 's/:/,/g'`

#
# Ingest parameters
#
DATE=`date "+%Y%m%d%H%M%S"`
WORKDIR=${BASE_WORK_DIR}/${DATE}-$$/
# using the hash partitioner to reduce latency....
PART_ARG=
TIMEOUT=600000
INPUT_FILES=$1
REDUCERS=$2
EXTRA_OPTS=${@:3}
# Don't change this option unless you know what you're doing. There's a specific format to the field. See AccumuloOutputFormat for details.
BATCHWRITER_OPTS="-AccumuloOutputFormat.WriteOpts.BatchWriterConfig=    11#maxMemory=100000000,maxWriteThreads=8"
MAPRED_OPTS="-mapreduce.job.reduces=$REDUCERS -mapreduce.task.io.sort.mb=${LIVE_CHILD_IO_SORT_MB} -mapreduce.task.io.sort.factor=100 -bulk.ingest.mapper.threads=0 -bulk.ingest.mapper.workqueue.size=10000 -io.file.buffer.size=1048576 -dfs.bytes-per-checksum=4096 -io.sort.record.percent=.10 -mapreduce.map.sort.spill.percent=.50 -mapreduce.map.output.compress=${LIVE_MAP_OUTPUT_COMPRESS} -mapreduce.map.output.compress.codec=${LIVE_MAP_OUTPUT_COMPRESSION_CODEC} -mapreduce.output.fileoutputformat.compress.type=${LIVE_MAP_OUTPUT_COMPRESSION_TYPE} $PART_ARG -mapreduce.task.timeout=$TIMEOUT -markerFileReducePercentage 0.33 -context.writer.max.cache.size=2500 -mapreduce.job.queuename=liveIngestQueue $MAPRED_INGEST_OPTS"

if [[ "$LIVE_CHILD_MAP_MAX_MEMORY_MB" == "" ]]; then
   LIVE_CHILD_MAP_MAX_MEMORY_MB=1536
fi
if [[ "$LIVE_CHILD_REDUCE_MAX_MEMORY_MB" == "" ]]; then
   LIVE_CHILD_REDUCE_MAX_MEMORY_MB=1536
fi

export HADOOP_CLASSPATH=$CLASSPATH
export HADOOP_OPTS="-Dfile.encoding=UTF8 -Duser.timezone=GMT $HADOOP_INGEST_OPTS"
export CHILD_MAP_OPTS="-Xmx${LIVE_CHILD_MAP_MAX_MEMORY_MB}m -XX:+UseConcMarkSweepGC -Dfile.encoding=UTF8 -Duser.timezone=GMT -XX:+UseNUMA -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 $CHILD_INGEST_OPTS"
export CHILD_REDUCE_OPTS="-Xmx${LIVE_CHILD_REDUCE_MAX_MEMORY_MB}m -XX:+UseConcMarkSweepGC -Dfile.encoding=UTF8 -Duser.timezone=GMT -XX:+UseNUMA $CHILD_INGEST_OPTS"


echo $INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_INGEST_CORE_JAR} datawave.ingest.mapreduce.job.IngestJob -jt $INGEST_JOBTRACKER_NODE $INPUT_FILES ${INGEST_CONFIG[@]} -cacheBaseDir $JOB_CACHE_DIR -cacheJars $LIBJARS -user $USERNAME -pass $PASSWORD -instance $WAREHOUSE_INSTANCE_NAME -zookeepers $WAREHOUSE_ZOOKEEPERS -workDir $WORKDIR  -flagFileDir ${FLAG_DIR} -flagFilePattern '.*_live_.*\.flag' -mapred.map.child.java.opts=\"$CHILD_MAP_OPTS\" -mapred.reduce.child.java.opts=\"$CHILD_REDUCE_OPTS\" "${BATCHWRITER_OPTS}" $MAPRED_OPTS -outputMutations -mapOnly -srcHdfs $INGEST_HDFS_NAME_NODE -destHdfs $WAREHOUSE_HDFS_NAME_NODE $EXTRA_OPTS
echo "For decreased latency, one can add the -mapOnly flag at the cost of possibly overcounting duplicate records"

$INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_INGEST_CORE_JAR} datawave.ingest.mapreduce.job.IngestJob -jt $INGEST_JOBTRACKER_NODE $INPUT_FILES ${INGEST_CONFIG[@]} -cacheBaseDir $JOB_CACHE_DIR -cacheJars $LIBJARS -user $USERNAME -pass $PASSWORD -instance $WAREHOUSE_INSTANCE_NAME -zookeepers $WAREHOUSE_ZOOKEEPERS -workDir $WORKDIR  -flagFileDir ${FLAG_DIR} -flagFilePattern '.*_live_.*\.flag' -mapred.map.child.java.opts="$CHILD_MAP_OPTS" -mapred.reduce.child.java.opts="$CHILD_REDUCE_OPTS" "${BATCHWRITER_OPTS}" $MAPRED_OPTS -outputMutations -mapOnly -srcHdfs $INGEST_HDFS_NAME_NODE -destHdfs $WAREHOUSE_HDFS_NAME_NODE $EXTRA_OPTS

RETURN_CODE=$?

exit $RETURN_CODE
