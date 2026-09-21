#!/bin/bash

set -e

TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/check-job-cache-test.XXXXXXXX")
cleanup()
{
    rm -r -f "$TEST_DIR"
}
trap cleanup EXIT

SCRIPT_DIR="$TEST_DIR/bin/ingest"
INSTALL_DIR="$TEST_DIR/install"
FAKE_HADOOP_HOME="$TEST_DIR/hadoop"
FAKE_HADOOP_CONF="$TEST_DIR/hadoop-conf"
LISTING_FILE="$TEST_DIR/hdfs-listing"

mkdir -p "$SCRIPT_DIR" "$INSTALL_DIR/config" "$INSTALL_DIR/lib" "$FAKE_HADOOP_HOME/bin" "$FAKE_HADOOP_CONF"
cp "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../main/resources/bin/ingest" && pwd)/check-job-cache.sh" "$SCRIPT_DIR/"

printf 'xml\n' > "$INSTALL_DIR/config/ingest-config.xml"
printf 'edge\n' > "$INSTALL_DIR/config/edge-key-version.txt"
printf 'jar' > "$INSTALL_DIR/lib/datawave.jar"

cat > "$SCRIPT_DIR/ingest-env.sh" <<'EOF'
INGEST_HADOOP_HOME=$TEST_FAKE_HADOOP_HOME
INGEST_HADOOP_CONF=$TEST_FAKE_HADOOP_CONF
INGEST_HDFS_NAME_NODE=file://
EOF

cat > "$SCRIPT_DIR/job-cache-env.sh" <<'EOF'
JOB_CACHE_DIR=/jobCacheA
EOF

cat > "$SCRIPT_DIR/ingest-libs.sh" <<'EOF'
CONF_DIR=$TEST_INSTALL_DIR/config
DISTRIBUTED_CACHE_JARS=$CONF_DIR,$TEST_INSTALL_DIR/lib/datawave.jar,$CONF_DIR/edge-key-version.txt
EOF

cat > "$FAKE_HADOOP_HOME/bin/hadoop" <<'EOF'
#!/bin/bash
cat "$TEST_LISTING_FILE"
EOF
chmod +x "$FAKE_HADOOP_HOME/bin/hadoop"

export TEST_FAKE_HADOOP_HOME="$FAKE_HADOOP_HOME"
export TEST_FAKE_HADOOP_CONF="$FAKE_HADOOP_CONF"
export TEST_INSTALL_DIR="$INSTALL_DIR"
export TEST_LISTING_FILE="$LISTING_FILE"

write_complete_listing()
{
    cat > "$LISTING_FILE" <<'EOF'
-rw-r--r-- 1 user group 4 2026-09-21 00:00 file:///jobCacheA/config/ingest-config.xml
-rw-r--r-- 1 user group 5 2026-09-21 00:00 file:///jobCacheA/config/edge-key-version.txt
-rw-r--r-- 1 user group 3 2026-09-21 00:00 file:///jobCacheA/datawave.jar
-rw-r--r-- 1 user group 5 2026-09-21 00:00 file:///jobCacheA/edge-key-version.txt
EOF
}

expect_failure()
{
    local description=$1
    if "$SCRIPT_DIR/check-job-cache.sh" > /dev/null 2>&1; then
        echo "Expected failure: $description"
        exit 1
    fi
}

write_complete_listing
"$SCRIPT_DIR/check-job-cache.sh"

write_complete_listing
sed -i '\|file:///jobCacheA/datawave.jar$|d' "$LISTING_FILE"
expect_failure "candidate missing distributed jar"

write_complete_listing
sed -i '\|file:///jobCacheA/config/|d' "$LISTING_FILE"
expect_failure "candidate missing config directory"

write_complete_listing
sed -i '\|file:///jobCacheA/edge-key-version.txt$|d' "$LISTING_FILE"
expect_failure "candidate missing top-level edge-key cache"

write_complete_listing
sed -i 's|group 5 2026-09-21 00:00 file:///jobCacheA/edge-key-version.txt$|group 0 2026-09-21 00:00 file:///jobCacheA/edge-key-version.txt|' "$LISTING_FILE"
expect_failure "candidate has empty top-level edge-key cache"

echo "check-job-cache.sh regression tests passed"
