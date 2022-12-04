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

    # set up CMSSW
    export VO_CMS_SW_DIR="/cvmfs/cms.cern.ch" 
    source "${VO_CMS_SW_DIR}/cmsset_default.sh" || return "$?"
    export CMSSW_VERSION="CMSSW_10_6_29_patch1"
    export CMSSW_BASE_PATH="${PROD_BASE_PATH}/usr/${CMSSW_VERSION}"
    export SCRAM_ARCH="slc7_amd64_gcc700"

    # CMSSW setup
    if [[ ! -d "${CMSSW_BASE_PATH}" ]]; then
        cd "${PROD_BASE_PATH}/usr" || return "$?"
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
    local python3_version="$( python3 -c "import sys;print('{0.major}.{0.minor}'.format(sys.version_info))" )"
    _pip_install () {
        python3 -m pip install --ignore-installed --no-cache-dir --prefix "${PROD_BASE_PATH}/usr" "$@"
    }
    _pip_install -r "${PROD_BASE_PATH}/requirements.txt"

    # law and luigi paths
    export LAW_HOME="${base_dir}/.law"
    export LAW_CONFIG_FILE="${base_dir}/law.cfg"
    export LUIGI_CONFIG_PATH="${base_dir}/luigi.cfg"

    # add tasks as library to PYTHONPATH
    export PYTHONPATH="${PROD_BASE_PATH}:${PROD_BASE_PATH}/usr/local/lib/python${python3_version}/site-packages:${PROD_BASE_PATH}/usr/local/lib/python${python3_version}/dist-packages:${PYTHONPATH}"
    export PATH="${PROD_BASE_PATH}/usr/local/bin:${PATH}"
}


action "$@"
