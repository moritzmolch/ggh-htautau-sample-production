#!/usr/bin/env bash


action () {
    # directory of that script and current working directory 
    local base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local current_dir="$( pwd )"

    # production base paths
    export PROD_BASE_PATH="${base_dir}"
    export PROD_DATA_PATH="${PROD_BASE_PATH}/data"
    export PROD_CONFIG_PATH="${PROD_BASE_PATH}/config"
    export PROD_INPUTS_PATH="${PROD_BASE_PATH}/inputs"

    # helpers
    prod_pip_install () {
        python3 -m pip install --ignore-installed --no-cache-dir --prefix "${PROD_BASE_PATH}/usr" "$@"
    }
    prod_add_py () {
        if [[ ! -z "${1}" ]]; then
            export PYTHONPATH="${1}:${PYTHONPATH}"
        fi
    }
    prod_add_bin () {
        if [[ ! -z "${1}" ]]; then
            export PATH="${1}:${PATH}"
        fi
    }
    prod_add_lib () {
        if [[ ! -z "${1}" ]]; then
            export LD_LIBRARY_PATH="${1}:${LD_LIBRARY_PATH}"
        fi
    }

    # set up CMSSW
    export VO_CMS_SW_DIR="/cvmfs/cms.cern.ch" 
    source "${VO_CMS_SW_DIR}/cmsset_default.sh" || return "$?"
    export CMSSW_VERSION="CMSSW_10_6_29_patch1"
    export CMSSW_BASE_PATH="${PROD_BASE_PATH}/${CMSSW_VERSION}"
    export SCRAM_ARCH="slc7_amd64_gcc700"

    # CMSSW setup
    if [[ ! -d "${CMSSW_BASE_PATH}" ]]; then
        cd "${PROD_BASE_PATH}" || return "$?"
        scramv1 project CMSSW "${CMSSW_VERSION}" || return "$?"
        cd "${CMSSW_VERSION}/src" || return "$?"
        scramv1 build || return "$?"
        eval "$( scramv1 runtime -sh )" || return "$?"
        cd "${current_dir}" || return "$?"
    fi
    cd "${CMSSW_BASE_PATH}/src" || return "$?"
    scramv1 build || return "$?"
    eval "$( scramv1 runtime -sh )" || return "$?"
    cd "${current_dir}" || return "$?"

    # install additional python packages
    prod_pip_install -r "${PROD_BASE_PATH}/requirements.txt"

    # law and luigi paths
    export LAW_HOME="${PROD_BASE_PATH}/.law"
    export LAW_CONFIG_FILE="${PROD_BASE_PATH}/law.cfg"
    export LUIGI_CONFIG_PATH="${PROD_BASE_PATH}/luigi.cfg"

    # add libraries and binaries to path variables
    local python3_version="$( python3 -c "import sys;print('{0.major}.{0.minor}'.format(sys.version_info))" )"
    prod_add_py "${PROD_BASE_PATH}/usr/lib/python${python3_version}/site-packages"
    prod_add_py "${PROD_BASE_PATH}"
    prod_add_bin "${PROD_BASE_PATH}/usr/bin"

    # index tasks
    python3 -m law index --verbose || return "$?"
}


action "$@"
