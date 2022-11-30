#!/usr/bin/env bash

_cmssw_pip_install () {
    PYTHONUSERBASE="${PROD_BASE_PACKAGES_PATH}" pip install --user --no-cache-dir "$@"
}

action () {

    # directory of that script and current working directory 
    local base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local current_dir="$( pwd )"

    # production base paths
    export PROD_BASE_PATH="${base_dir}"
    export PROD_BASE_DATA_PATH="${PROD_BASE_PATH}/data"
    export PROD_BASE_CONFIG_PATH="${PROD_BASE_PATH}/config"
    export PROD_BASE_INPUTS_PATH="${PROD_BASE_PATH}/inputs"
    export PROD_SOFTWARE="${PROD_BASE_PATH}/tmp/software"

    ### CMSSW setup
    export SCRAM_ARCH="slc7_amd64_gcc700"
    export CMSSW_VERSION="CMSSW_10_6_29_patch1"
    export CMSSW_BASE_PATH="${PROD_SOFTWARE}/${CMSSW_VERSION}"

    # set CMS defaults
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || exit "$?"
    
    # install CMSSW
    if [[ ! -d "${CMSSW_BASE_PATH}" ]]; then
        local install_dir="$( dirname "${CMSSW_BASE_PATH}" )"
        local cmssw_version="$( basename "${CMSSW_BASE_PATH}" )"
        cd "${install_dir}" || exit "$?"
        scramv1 project CMSSW "${cmssw_version}" || exit "$?"
        cd "${current_dir}" || exit "$?"
    fi
    
    # set environment
    cd "${CMSSW_BASE_PATH}" || exit "$?"
    eval "$( scramv1 runtime -sh )" || exit "$?"
    cd "${current_dir}" || exit "$?"

    # law and luigi paths
    export LAW_HOME="${base_dir}/.law"
    export LAW_CONFIG_FILE="${base_dir}/law.cfg"
    export LUIGI_CONFIG_PATH="${base_dir}/luigi.cfg"

    # install additional python packages within the python environment provided by CMSSW

    # add tasks as library to PYTHONPATH
    export PYTHONPATH="${PYTHONPATH}:${PROD_BASE_PATH}"

}


action "$@"
