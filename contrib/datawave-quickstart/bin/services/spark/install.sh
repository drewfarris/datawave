#!/usr/bin/env bash

# Resolve env.sh
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVICES_DIR="$( dirname "${THIS_DIR}" )"
BIN_DIR="$( dirname "${SERVICES_DIR}" )"

source "${BIN_DIR}/env.sh"
source "${THIS_DIR}/bootstrap.sh"

sparkIsInstalled && info "Spark is already installed" && exit 1

[ -f "${DW_SPARK_SERVICE_DIR}/${DW_SPARK_DIST}" ] || fatal "Spark tarball not found"

# Extract, set symlink, and verify...
mkdir "${DW_SPARK_SERVICE_DIR}/${DW_SPARK_BASEDIR}" || fatal "Failed to create Spark base directory"
tar xf "${DW_SPARK_SERVICE_DIR}/${DW_SPARK_DIST}" -C "${DW_SPARK_SERVICE_DIR}/${DW_SPARK_BASEDIR}" --strip-components=1 || fatal "Failed to extract Spark tarball"
( cd "${DW_CLOUD_HOME}" && ln -s "bin/services/spark/${DW_SPARK_BASEDIR}" "${DW_SPARK_SYMLINK}" ) || fatal "Failed to set Spark symlink"

! sparkIsInstalled && fatal "Spark was not installed"

info "Spark tarball extracted and symlinked"

# Ensure that $JAVA_HOME is observed by all spark scripts
sed -i'' -e "s|.*\(export JAVA_HOME=\).*|\1${JAVA_HOME}|g" ${SPARK_CONF_DIR}/spark-env.sh

echo
info "Spark initialized and ready to start..."
echo
echo "      Start command: sparkStart"
echo "       Stop command: sparkStop"
echo "     Status command: sparkStatus"
echo
info "See \$DW_CLOUD_HOME/bin/services/spark/bootstrap.sh to view/edit commands as needed"
echo
