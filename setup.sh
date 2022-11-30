#!/usr/bin/env bash


install_cmssw () {
    local install_dir="${1}"
    local cmssw_version="${2}"
    local scram_arch="${3}"
    local cmssw_base_path="${install_dir}/${cmssw_version}"
    local current_dir="$( pwd )"

    cd "${install_dir}" || return "$?"
    export SCRAM_ARCH="${scram_arch}"
    scramv1 project CMSSW "${cmssw_version}" || return "$?"
    cd "${cmssw_base_path}/src" || return "$?"
    eval "$( scramv1 runtime -sh )" || return "$?"
    scramv1 build -j 10 || return "$?"
    cd "${current_dir}" || return "$?"
}


set_cmssw_environment () {
    local cmssw_base_path="${1}"
    local current_dir="$( pwd )"

    cd "${cmssw_base_path}/src" || return "$?"
    eval "$( scramv1 runtime -sh )" return "$?"
    cd "${current_dir}" || return "$?"
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
    export PROD_BASE_PACKAGES_PATH="${PROD_BASE_PATH}/packages"
    
    # CMS defaults
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" || return "$?"

    # CMSSW setup
    export PROD_CMSSW_BASE_PATH="${PROD_BASE_PATH}/packages/CMSSW_10_6_29_patch1"
    local cmssw_install_dir="$( dirname "${PROD_CMSSW_BASE_PATH}" )"
    local cmssw_version="$( basename "${PROD_CMSSW_BASE_PATH}" )"
    local scram_arch="slc7_amd74_gcc700"

    set_cmssw_environment "${PROD_CMSSW_BASE_PATH}" || return "$?"

    # law and luigi paths
    export LAW_HOME="${base_dir}/.law"
    export LAW_CONFIG_FILE="${base_dir}/law.cfg"
    export LUIGI_CONFIG_PATH="${base_dir}/luigi.cfg"

    # add tasks as library to PYTHONPATH
    export PYTHONPATH="${PYTHONPATH}:${PROD_BASE_PATH}"

}


action "$@"
