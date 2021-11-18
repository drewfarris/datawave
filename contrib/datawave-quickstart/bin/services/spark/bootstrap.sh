# Sourced by env.sh

DW_SPARK_SERVICE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# You may override DW_SPARK_DIST_URI in your env ahead of time, and set as file:///path/to/file.tar.gz for local tarball, if needed
DW_SPARK_DIST_URI="${DW_SPARK_DIST_URI:-http://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-without-hadoop.tgz}"
DW_SPARK_DIST="$( downloadTarball "${DW_SPARK_DIST_URI}" "${DW_SPARK_SERVICE_DIR}" && echo "${tarball}" )"
DW_SPARK_BASEDIR="spark-install"
DW_SPARK_SYMLINK="spark"

DW_SPARK_DFS_URI="hdfs://localhost:9000"
DW_SPARK_MR_INTER_DIR="/jobhist/inter"
DW_SPARK_MR_DONE_DIR="/jobhist/done"
DW_SPARK_RESOURCE_MANAGER_ADDRESS="localhost:8050"

SPARK_HOME="${DW_CLOUD_HOME}/${DW_SPARK_SYMLINK}"

# Hadoop standard exports...
export SPARK_HOME

export PATH=${SPARK_HOME}/bin:$PATH

# Service helpers...

DW_SPARK_CMD_START="( cd ${SPARK_HOME}/bin )"
DW_SPARK_CMD_STOP="( cd ${SPARK_HOME}/bin )"
DW_SPARK_CMD_FIND_ALL_PIDS=""

function sparkIsRunning() {
    DW_SPARK_PID_LIST="$(eval "${DW_SPARK_CMD_FIND_ALL_PIDS}")"
    [ -z "${DW_SPARK_PID_LIST}" ] && return 1 || return 0
}

function sparkStart() {
    sparkIsRunning && echo "Spark is already running" || eval "${DW_SPARK_CMD_START}"
    echo
    info "For detailed status visit 'http://localhost:9870/dfshealth.html#tab-overview' in your browser"
}

function sparkStop() {
    sparkIsRunning && eval "${DW_SPARK_CMD_STOP}" || echo "Spark is already stopped"
}

function sparkStatus() {
    # define local variables for spark processes
    local _sparkHist

    # use a state to parse jps entries
    echo "======  Spark Status  ======"
    sparkIsRunning && {
        local _pid
        local _opt=pid

        local -r _pids=${DW_SPARK_PID_LIST// /|}
        echo "pids: ${DW_SPARK_PID_LIST}"
        for _arg in $(jps -l | egrep "${_pids}"); do
            case ${_opt} in
                pid)
                    _pid=${_arg}
                    _opt=class
                    ;;
                class)
                    local _none
                    local _name=${_arg##*.}
                    case "${_name}" in
                        SparkHistory) _sparkHist=${_pid};;
                        *) _none=true;;
                    esac
                    test -z "${_none}" && info "${_name} => ${_pid}"
                    _pid=
                    _opt=pid
                    unset _none
                    ;;
            esac
        done
    }

    test -z "${_sparkHist}" && warn "Spark History Server is not running"
}

function sparkIsInstalled() {
    [ -L "${DW_CLOUD_HOME}/${DW_SPARK_SYMLINK}" ] && return 0
    [ -d "${DW_SPARK_SERVICE_DIR}/${DW_SPARK_BASEDIR}" ] && return 0
    return 1
}

function sparkUninstall() {
   if sparkIsInstalled ; then
      if [ -L "${DW_CLOUD_HOME}/${DW_SPARK_SYMLINK}" ] ; then
          ( cd "${DW_CLOUD_HOME}" && unlink "${DW_SPARK_SYMLINK}" ) || error "Failed to remove Spark symlink"
      fi

      if [ -d "${DW_SPARK_SERVICE_DIR}/${DW_SPARK_BASEDIR}" ] ; then
          rm -rf "${DW_SPARK_SERVICE_DIR}/${DW_SPARK_BASEDIR}"
      fi

      ! sparkIsInstalled && info "Spark uninstalled" || error "Failed to uninstall Spark"
   else
      info "Spark not installed. Nothing to do"
   fi

   [[ "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_LONG}" || "${1}" == "${DW_UNINSTALL_RM_BINARIES_FLAG_SHORT}" ]] && rm -f "${DW_SPARK_SERVICE_DIR}"/*.tar.gz
}

function sparkInstall() {
   "${DW_SPARK_SERVICE_DIR}"/install.sh
}

function sparkPrintenv() {
   echo
   echo "Spark Environment"
   echo
   ( set -o posix ; set ) | grep SPARK_
   echo
}

function sparkPidList() {

   sparkIsRunning && echo "${DW_SPARK_PID_LIST}"

}

function sparkDisplayBinaryInfo() {
  echo "Source: ${DW_SPARK_DIST_URI}"
  local tarballName="$(basename "$DW_SPARK_DIST_URI")"
  if [[ -f "${DW_SPARK_SERVICE_DIR}/${tarballName}" ]]; then
     echo " Local: ${DW_SPARK_SERVICE_DIR}/${tarballName}"
  else
     echo " Local: Not loaded"
  fi
}
