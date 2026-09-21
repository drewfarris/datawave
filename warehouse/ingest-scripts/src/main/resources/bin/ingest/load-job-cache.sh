#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
	READLINK_CMD="python -c 'import os,sys;print os.path.realpath(sys.argv[1])'"
	MKTEMP_OPTS="-t $0"
else
	READLINK_CMD="readlink -f"
	MKTEMP_OPTS=""
fi
THIS_SCRIPT=$(eval $READLINK_CMD $0)
THIS_DIR="${THIS_SCRIPT%/*}"

. "$THIS_DIR/ingest-env.sh"
. "$THIS_DIR/ingest-libs.sh"
. "$THIS_DIR/job-cache-env.sh"

# Check that there are no other instances of this script running
acquire_lock_file $(basename "$0") || exit 1

# Read from the DataWave metadata table to create the edge-key version file. Generate it
# in a temporary directory so a failed or empty update cannot replace the last good copy.
EDGE_KEY_CACHE_DIR="$THIS_DIR/../../config"
EDGE_KEY_CACHE_FILE="$EDGE_KEY_CACHE_DIR/edge-key-version.txt"
EDGE_KEY_CACHE_TMP_DIR=$(mktemp -d "$EDGE_KEY_CACHE_DIR/.edge-key-cache.XXXXXXXX") || {
    echo "[ERROR] Unable to create a temporary directory for $EDGE_KEY_CACHE_FILE"
    exit 1
}

if ! "$THIS_DIR/create-edgekey-version-cache.sh" --update "$EDGE_KEY_CACHE_TMP_DIR"; then
    echo "[ERROR] create-edgekey-version-cache.sh failed while generating $EDGE_KEY_CACHE_FILE"
    rm -r -f "$EDGE_KEY_CACHE_TMP_DIR"
    exit 1
fi

if [[ ! -s "$EDGE_KEY_CACHE_TMP_DIR/edge-key-version.txt" ]]; then
    echo "[ERROR] create-edgekey-version-cache.sh did not generate a nonempty $EDGE_KEY_CACHE_FILE"
    rm -r -f "$EDGE_KEY_CACHE_TMP_DIR"
    exit 1
fi

if ! mv "$EDGE_KEY_CACHE_TMP_DIR/edge-key-version.txt" "$EDGE_KEY_CACHE_FILE"; then
    echo "[ERROR] Unable to install the generated edge-key cache at $EDGE_KEY_CACHE_FILE"
    rm -r -f "$EDGE_KEY_CACHE_TMP_DIR"
    exit 1
fi
rm -r -f "$EDGE_KEY_CACHE_TMP_DIR"

# Swap the job cache directory
echo Old job cache dir is $JOB_CACHE_DIR
OLD_JOB_CACHE_DIR=$JOB_CACHE_DIR
OLD_SUFFIX=""
NEW_SUFFIX=""
if [[ ${JOB_CACHE_DIR: -1} == "A" ]]; then
    JOB_CACHE_DIR=${JOB_CACHE_DIR:0:${#JOB_CACHE_DIR}-1}B
    OLD_SUFFIX="A"
    NEW_SUFFIX="B"
else
   JOB_CACHE_DIR=${JOB_CACHE_DIR:0:${#JOB_CACHE_DIR}-1}A
   OLD_SUFFIX="B"
   NEW_SUFFIX="A"
fi
echo New job cache dir is $JOB_CACHE_DIR

BEFORE=$(basename $OLD_JOB_CACHE_DIR)
AFTER=$(basename $JOB_CACHE_DIR)

if ! sed "s%${BEFORE}%${AFTER}%" "$THIS_DIR/job-cache-env.sh" > "$THIS_DIR/job-cache-env.tmp" ||
    [[ ! -s "$THIS_DIR/job-cache-env.tmp" ]]; then
    echo "[ERROR] Unable to prepare the job-cache environment for $JOB_CACHE_DIR"
    rm -f "$THIS_DIR/job-cache-env.tmp"
    exit 1
fi

. "$THIS_DIR/ingest-libs.sh"

date

# prepare a directory with links to all of the files/directories to put into the jobcache
tmpdir=$(mktemp -d $MKTEMP_OPTS) || {
    echo "[ERROR] Unable to create a temporary directory for the job cache"
    rm -f "$THIS_DIR/job-cache-env.tmp"
    exit 1
}
cleanup()
{
    local status=$?
    rm -r -f "$tmpdir"
    [[ "$status" != 0 ]] && rm -f "$THIS_DIR/job-cache-env.tmp"
    exit "$status"
}
trap cleanup INT TERM EXIT

for f in ${DISTRIBUTED_CACHE_JARS//,/ }; do
    if [[ -e "$f" ]]; then
        fname=${f/*\//}
        # determine the actual path
        f=$(eval "$READLINK_CMD \"\$f\"")
        ln -s "$f" "$tmpdir/$fname"
    else
        echo "[ERROR] Distributed-cache entry does not exist: $f"
        exit 1
    fi
done

# determine the number of processors we can use
if [[ $(uname) == "Darwin" ]]; then
	declare -i CPUS=$(sysctl machdep.cpu.thread_count | awk '{print $2}')
else
	declare -i CPUS=$(cat /proc/cpuinfo | grep processor | awk '{print $3}' | sort -n | tail -1)
fi
# lets use twice the number of processors
CPUS=$(echo "$LOAD_JOBCACHE_CPU_MULTIPLIER * $CPUS" | bc)

remove_candidate()
{
    local hadoop_home=$1
    local hadoop_conf=$2
    local name_node=$3

    "$hadoop_home/bin/hadoop" fs \
        -conf "$hadoop_conf/hdfs-site.xml" \
        -fs "$name_node" \
        -rm -r "${name_node}${JOB_CACHE_DIR}"
}

load_candidate()
{
    local cluster_name=$1
    local hadoop_home=$2
    local hadoop_conf=$3
    local name_node=$4
    local candidate_uri="${name_node}${JOB_CACHE_DIR}"

    if "$hadoop_home/bin/hadoop" fs \
        -conf "$hadoop_conf/hdfs-site.xml" \
        -fs "$name_node" \
        -test -d "$candidate_uri" > /dev/null 2>&1; then
        echo "Replacing $cluster_name job cache candidate: $candidate_uri"
        remove_candidate "$hadoop_home" "$hadoop_conf" "$name_node" || return 1
    else
        echo "Creating $cluster_name job cache candidate: $candidate_uri"
    fi

    # copyFromLocal needs the parent directory chain to exist.
    "$hadoop_home/bin/hadoop" fs \
        -conf "$hadoop_conf/hdfs-site.xml" \
        -fs "$name_node" \
        -mkdir -p "${name_node}${JOB_CACHE_DIR}/.." || return 1
    "$hadoop_home/bin/hadoop" fs \
        -conf "$hadoop_conf/hdfs-site.xml" \
        -fs "$name_node" \
        -copyFromLocal -t "$CPUS" "$tmpdir" "$candidate_uri" || return 1

    if [[ "$name_node" == "hdfs://"* ]]; then
        "$hadoop_home/bin/hadoop" fs \
            -conf "$hadoop_conf/hdfs-site.xml" \
            -setrep -R "$JOB_CACHE_REPLICATION" "$candidate_uri" || return 1
    fi
}

validate_candidate()
{
    local hadoop_home=$1
    local hadoop_conf=$2
    local name_node=$3

    JOB_CACHE_DIR_OVERRIDE="$JOB_CACHE_DIR" \
    JOB_CACHE_HADOOP_HOME_OVERRIDE="$hadoop_home" \
    JOB_CACHE_HADOOP_CONF_OVERRIDE="$hadoop_conf" \
    JOB_CACHE_HDFS_NAME_NODE_OVERRIDE="$name_node" \
        "$THIS_DIR/check-job-cache.sh"
}

if ! load_candidate "ingest" "$INGEST_HADOOP_HOME" "$INGEST_HADOOP_CONF" "$INGEST_HDFS_NAME_NODE"; then
    echo "[ERROR] Failed to load ingest job cache candidate: ${INGEST_HDFS_NAME_NODE}${JOB_CACHE_DIR}"
    remove_candidate "$INGEST_HADOOP_HOME" "$INGEST_HADOOP_CONF" "$INGEST_HDFS_NAME_NODE" > /dev/null 2>&1
    exit 1
fi
if ! validate_candidate "$INGEST_HADOOP_HOME" "$INGEST_HADOOP_CONF" "$INGEST_HDFS_NAME_NODE"; then
    echo "[ERROR] Ingest job cache candidate failed validation: ${INGEST_HDFS_NAME_NODE}${JOB_CACHE_DIR}"
    remove_candidate "$INGEST_HADOOP_HOME" "$INGEST_HADOOP_CONF" "$INGEST_HDFS_NAME_NODE" > /dev/null 2>&1
    exit 1
fi

########### We need this section to allow running the map file merger on the warehouse cluster ##########
if [[ "$WAREHOUSE_HDFS_NAME_NODE" != "$INGEST_HDFS_NAME_NODE" ]]; then
    if ! load_candidate "warehouse" "$WAREHOUSE_HADOOP_HOME" "$WAREHOUSE_HADOOP_CONF" "$WAREHOUSE_HDFS_NAME_NODE"; then
        echo "[ERROR] Failed to load warehouse job cache candidate: ${WAREHOUSE_HDFS_NAME_NODE}${JOB_CACHE_DIR}"
        remove_candidate "$WAREHOUSE_HADOOP_HOME" "$WAREHOUSE_HADOOP_CONF" "$WAREHOUSE_HDFS_NAME_NODE" > /dev/null 2>&1
        remove_candidate "$INGEST_HADOOP_HOME" "$INGEST_HADOOP_CONF" "$INGEST_HDFS_NAME_NODE" > /dev/null 2>&1
        exit 1
    fi
    if ! validate_candidate "$WAREHOUSE_HADOOP_HOME" "$WAREHOUSE_HADOOP_CONF" "$WAREHOUSE_HDFS_NAME_NODE"; then
        echo "[ERROR] Warehouse job cache candidate failed validation: ${WAREHOUSE_HDFS_NAME_NODE}${JOB_CACHE_DIR}"
        remove_candidate "$WAREHOUSE_HADOOP_HOME" "$WAREHOUSE_HADOOP_CONF" "$WAREHOUSE_HDFS_NAME_NODE" > /dev/null 2>&1
        remove_candidate "$INGEST_HADOOP_HOME" "$INGEST_HADOOP_CONF" "$INGEST_HDFS_NAME_NODE" > /dev/null 2>&1
        exit 1
    fi
else
    echo "Warehouse and ingest are one in the same. The validated ingest candidate will serve both."
fi

# Prepare the rollback copy before publishing the candidate.
if ! cp "$THIS_DIR/job-cache-env.sh" "$THIS_DIR/job-cache-env.bak"; then
    echo "[ERROR] Unable to back up $THIS_DIR/job-cache-env.sh"
    exit 1
fi

# Publish only after every candidate has passed validation.
if [[ -n "${ACTIVE_JOB_CACHE_PATH}" ]]; then
  if ! java -cp "${CLASSPATH}" datawave.ingest.jobcache.SetActiveCommand \
    --zookeepers "${INGEST_ZOOKEEPERS}" \
    --path "${ACTIVE_JOB_CACHE_PATH}" \
    --job-cache "${INGEST_HDFS_NAME_NODE}${JOB_CACHE_DIR}"; then
      echo "[ERROR] Failed to set active ingest job cache"
      exit 1
  fi

  if [[ "$WAREHOUSE_HDFS_NAME_NODE" != "$INGEST_HDFS_NAME_NODE" ]]; then
    if ! java -cp "${CLASSPATH}" datawave.ingest.jobcache.SetActiveCommand \
      --zookeepers "${WAREHOUSE_ZOOKEEPERS}" \
      --path "${ACTIVE_JOB_CACHE_PATH}" \
      --job-cache "${WAREHOUSE_HDFS_NAME_NODE}${JOB_CACHE_DIR}"; then
        echo "[ERROR] Failed to set active warehouse job cache"
        if ! java -cp "${CLASSPATH}" datawave.ingest.jobcache.SetActiveCommand \
          --zookeepers "${INGEST_ZOOKEEPERS}" \
          --path "${ACTIVE_JOB_CACHE_PATH}" \
          --job-cache "${INGEST_HDFS_NAME_NODE}${OLD_JOB_CACHE_DIR}"; then
            echo "[ERROR] Failed to roll back the active ingest job cache to ${INGEST_HDFS_NAME_NODE}${OLD_JOB_CACHE_DIR}"
        fi
        exit 1
    fi
  fi
fi

# Remove the prepared directory
rm -r -f "$tmpdir"
trap - INT TERM EXIT
date


#######################################################################################

# If we made it here, everything is loaded into the new job cache
# directory.  So, just swap the the environment script with the new
# one that will tell jobs to run with the new job cache dir.
if ! mv "$THIS_DIR/job-cache-env.tmp" "$THIS_DIR/job-cache-env.sh"; then
    echo "[ERROR] Unable to activate $JOB_CACHE_DIR in $THIS_DIR/job-cache-env.sh"
    exit 1
fi
