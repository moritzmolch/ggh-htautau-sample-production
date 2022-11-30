#!/usr/bin/env bash

_cmssw_pip_install () {
    PYTHONUSERBASE="${PROD_BASE_PACKAGES_PATH}" pip install --user --no-cache-dir "$@"
}

install_cmssw () {
    local install_dir
    install_dir="${1}"
    local cmssw_version
    cmssw_version="${2}"
    local scram_arch
    scram_arch="${3}"
    local cmssw_base_path
    cmssw_base_path="${install_dir}/${cmssw_version}"
    local current_dir
    current_dir="$( pwd )"

    cd "${install_dir}" || return "$?"
    export SCRAM_ARCH="${scram_arch}"
    scramv1 project CMSSW "${cmssw_version}" || return "$?"
    cd "${cmssw_base_path}/src" || return "$?"
    eval "$( scramv1 runtime -sh )" || return "$?"
    scramv1 build -j 10 || return "$?"
    cd "${current_dir}" || return "$?"
}


set_cmssw_environment () {
    local cmssw_base_path
    cmssw_base_path="${1}"
    local current_dir
    current_dir="$( pwd )"

    cd "${cmssw_base_path}/src" || return "$?"
    eval "$( scramv1 runtime -sh )" return "$?"
    cd "${current_dir}" || return "$?"
}


action () {

    # directory of that script and current working directory 
    local base_dir
    base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local current_dir
    current_dir="$( pwd )"

    # production base paths
    export PROD_BASE_PATH="${base_dir}"
    export PROD_BASE_DATA_PATH="${PROD_BASE_PATH}/data"
    export PROD_BASE_CONFIG_PATH="${PROD_BASE_PATH}/config"
    export PROD_BASE_INPUTS_PATH="${PROD_BASE_PATH}/inputs"
    export PROD_BASE_PACKAGES_PATH="${PROD_BASE_PATH}/packages"
    export PROD_SOFTWARE="${PROD_BASE_PATH}/tmp/software"

    # CMS defaults
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" || return "$?"

    # CMSSW setup
    local prod_scram_arch="slc7_amd64_gcc700"
    export CMSSW_RELEASE="CMSSW_10_6_29_patch1"
    export CMSSW_BASE_PATH="${PROD_SOFTWARE}/${CMSSW_RELEASE}"
    if [[ ! -d "${CMSSW_BASE_PATH}" ]]; then
        echo "Install CMSSW release ${CMSSW_RELEASE}"
        install_cmssw "${PROD_SOFTWARE}" "${CMSSW_RELEASE}" "${prod_scram_arch}" || return "$?"
    fi
    set_cmssw_environment "${CMSSW_BASE_PATH}" || return "$?"

    # law and luigi paths
    export LAW_HOME="${base_dir}/.law"
    export LAW_CONFIG_FILE="${base_dir}/law.cfg"
    export LUIGI_CONFIG_PATH="${base_dir}/luigi.cfg"

    # add tasks as library to PYTHONPATH
    export PYTHONPATH="${PROD_BASE_PATH}:${PROD_SOFTWARE}/lib/python3.6/site-packages:${PYTHONPATH}"
    export PATH="${PROD_SOFTWARE}/bin:${PATH}"

}


action "$@"
