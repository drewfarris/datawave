#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
    THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' "$0")
else
    THIS_SCRIPT=$(readlink -f "$0")
fi
THIS_DIR="${THIS_SCRIPT%/*}"

# load-job-cache.sh uses these overrides to validate a candidate before publishing it.
REQUESTED_JOB_CACHE_DIR=${JOB_CACHE_DIR_OVERRIDE:-}
REQUESTED_HADOOP_HOME=${JOB_CACHE_HADOOP_HOME_OVERRIDE:-}
REQUESTED_HADOOP_CONF=${JOB_CACHE_HADOOP_CONF_OVERRIDE:-}
REQUESTED_HDFS_NAME_NODE=${JOB_CACHE_HDFS_NAME_NODE_OVERRIDE:-}

. "$THIS_DIR/ingest-env.sh"
. "$THIS_DIR/job-cache-env.sh"
. "$THIS_DIR/ingest-libs.sh"

JOB_CACHE_DIR=${REQUESTED_JOB_CACHE_DIR:-$JOB_CACHE_DIR}
JOB_CACHE_HADOOP_HOME=${REQUESTED_HADOOP_HOME:-$INGEST_HADOOP_HOME}
JOB_CACHE_HADOOP_CONF=${REQUESTED_HADOOP_CONF:-$INGEST_HADOOP_CONF}
JOB_CACHE_HDFS_NAME_NODE=${REQUESTED_HDFS_NAME_NODE:-$INGEST_HDFS_NAME_NODE}
JOB_CACHE_URI="${JOB_CACHE_HDFS_NAME_NODE}${JOB_CACHE_DIR}"

echo "Checking the consistency of $JOB_CACHE_URI"

hdfs_listing=$(mktemp -t "$(basename "$0").XXXXXXXX") || exit 1
cleanup()
{
    local status=$?
    rm -f "$hdfs_listing"
    exit "$status"
}
trap cleanup INT TERM EXIT

if ! "$JOB_CACHE_HADOOP_HOME/bin/hadoop" fs \
    -conf "$JOB_CACHE_HADOOP_CONF/hdfs-site.xml" \
    -fs "$JOB_CACHE_HDFS_NAME_NODE" \
    -ls -R "$JOB_CACHE_URI" > "$hdfs_listing"; then
    echo "$JOB_CACHE_URI cannot be listed"
    exit 1
fi

get_hdfs_size()
{
    local expected_name=$1
    local permissions replication owner group size date time path extra

    while read -r permissions replication owner group size date time path extra; do
        if [[ -z "$extra" && ( "$path" == "$JOB_CACHE_URI/$expected_name" || "$path" == *"$JOB_CACHE_DIR/$expected_name" ) ]]; then
            echo "$size"
            return 0
        fi
    done < "$hdfs_listing"
    return 1
}

validate_local_file()
{
    local local_file=$1
    local cache_name=$2
    local local_size hdfs_size

    if [[ ! -f "$local_file" ]]; then
        echo "Cannot find local distributed-cache file $local_file"
        return 1
    fi

    local_size=$(/bin/ls -Ll "$local_file" | awk '{print $5}') || return 1
    if ! hdfs_size=$(get_hdfs_size "$cache_name"); then
        echo "$JOB_CACHE_URI missing $cache_name"
        return 1
    fi

    if [[ "$local_size" != "$hdfs_size" ]]; then
        echo "$JOB_CACHE_URI inconsistent for $cache_name: $local_size != $hdfs_size"
        return 1
    fi
}

config_validated=false
for cache_entry in ${DISTRIBUTED_CACHE_JARS//,/ }; do
    if [[ ! -e "$cache_entry" ]]; then
        echo "Distributed-cache entry does not exist: $cache_entry"
        exit 1
    fi

    if [[ -d "$cache_entry" ]]; then
        entry_name=${cache_entry%/}
        entry_name=${entry_name##*/}
        [[ "$cache_entry" == "$CONF_DIR" ]] && config_validated=true

        while IFS= read -r -d '' local_file; do
            file_name=${local_file#"$cache_entry"/}
            [[ "${file_name##*/}" == "."* ]] && continue
            validate_local_file "$local_file" "$entry_name/$file_name" || exit 1
        done < <(find -L "$cache_entry" -type f -print0)
    else
        validate_local_file "$cache_entry" "${cache_entry##*/}" || exit 1
    fi
done

if [[ "$config_validated" != true ]]; then
    echo "$CONF_DIR is not included in DISTRIBUTED_CACHE_JARS"
    exit 1
fi

if ! edge_key_size=$(get_hdfs_size "edge-key-version.txt"); then
    echo "$JOB_CACHE_URI missing edge-key-version.txt"
    exit 1
elif [[ "$edge_key_size" == "0" ]]; then
    echo "edge-key-version.txt in $JOB_CACHE_URI is empty"
    exit 1
fi

trap - INT TERM EXIT
rm -f "$hdfs_listing"
echo "$JOB_CACHE_URI appears consistent"
