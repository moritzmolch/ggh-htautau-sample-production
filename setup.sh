#!/usr/bin/env bash


action () {

    # do not run the setup twice
    if [[ "${PROD_SETUP}" = "1" ]]; then
        echo "production environment has already been set up"
        return "0"
    fi

    # directory of that script and current working directory
    local shell_is_zsh this_file this_dir
    shell_is_zsh="$( [[ -z "${ZSH_VERSION}" ]] && echo "false" || echo "true" )"
    this_file="$( [[ "${shell_is_zsh}" == "true" ]] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]:-${0}}" )"
    this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    current_dir="$( pwd )"

    # define important paths of this project as well as storage targets
    export PROD_BASE="${this_dir}"
    export PROD_JOBS_BASE="${PROD_BASE}/jobs"
    export PROD_BUNDLE_BASE="${PROD_BASE}/bundle"
    export PROD_SOFTWARE_BASE="${PROD_BASE}/software"
    export PROD_SOFTWARE_LOCAL_BASE="${PROD_SOFTWARE_BASE}/local"

    # CMSSW settings (architecture and version)
    export PROD_SCRAM_ARCH="slc7_amd64_gcc700"
    export PROD_CMSSW_VERSION="CMSSW_10_6_29_patch1"
    export PROD_CMSSW_BASE="${PROD_SOFTWARE_BASE}/cmssw/${PROD_CMSSW_VERSION}"

    # helper functions for adding python packages and binaries
    prod_pip_install () {
        pip install --ignore-installed --no-cache-dir --prefix "${PROD_SOFTWARE_LOCAL_BASE}" "$@"
    }
    export -f prod_pip_install

    prod_add_bin () {
        if [[ ! -z "${1}" ]]; then
            export PATH="${1}:${PATH}"
        fi
    }
    export -f prod_add_bin

    prod_add_py () {
        if [[ ! -z "${1}" ]]; then
            export PYTHONPATH="${1}:${PYTHONPATH}"
        fi
    }
    export -f prod_add_bin

    # set up CMSSW
    export SCRAM_ARCH="${PROD_SCRAM_ARCH}"
    export CMSSW_VERSION="${PROD_CMSSW_VERSION}"
    export CMSSW_BASE="${PROD_CMSSW_BASE}"

    # install CMSSW if the CMSSW directory doesn't exist
    if [[ ! -d "${CMSSW_BASE}" ]]; then
        (
            source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" &&
            mkdir -p "$( dirname "${CMSSW_BASE}" )" &&
            cd "$( dirname "${CMSSW_BASE}" )" &&
            scramv1 project CMSSW "${CMSSW_VERSION}" &&
            cd "${CMSSW_VERSION}" &&
            eval "$( scramv1 runtime -sh )" &&
            scram b 
        ) || return "$?"
    fi

    # activate CMSSW installation
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" &&
    cd "${CMSSW_BASE}/src" || return "$?"
    eval "$( scramv1 runtime -sh )" || return "$?"
    cd "${current_dir}" || return "$?"

    # environment variables related to additional software
    export PYTHONWARNINGS="ignore"
    export PROD_ORIG_PYTHONPATH="${PYTHONPATH}"

    # install python packages
    if [[ ! -d "${PROD_SOFTWARE_LOCAL_BASE}" ]]; then
        (
            mkdir -p "${PROD_SOFTWARE_LOCAL_BASE}" &&
            prod_pip_install law &&
            prod_pip_install order
        ) || return "$?"
    fi

    prod_add_bin "${PROD_SOFTWARE_LOCAL_BASE}/bin"
    prod_add_py "${PROD_SOFTWARE_LOCAL_BASE}/lib/python2.7/site-packages"

    # VOMS proxy settings
    export GLOBUS_THREAD_MODEL="none"
    export X509_CERT_DIR="/cvmfs/grid.cern.ch/etc/grid-security/certificates"
    export X509_VOMS_DIR="/cvmfs/grid.cern.ch/etc/grid-security/vomsdir"
    if [[ $( id -u ) && "$?" = "0" ]]; then
        user_id="$( id -u )"
        export X509_USER_PROXY="${X509_USER_PROXY:-/tmp/x509up_u${user_id}}"
    fi
    export VOMS_USERCONF="/cvmfs/grid.cern.ch/etc/grid-security/vomses"

    ## GFAL2 setup
    local grid_base="/cvmfs/grid.cern.ch/centos7-ui-4.0.3-1_umd4v1"
    export GFAL_CONFIG_DIR="${grid_base}/etc/gfal2.d"
    export GFAL_PLUGIN_DIR="${grid_base}/usr/lib64/gfal2-plugins"
    export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${grid_base}/usr/lib64:${grid_base}/usr/lib"
    export PATH="${PATH}:${grid_base}/usr/bin:${grid_base}/usr/sbin"
    export PYTHONPATH="${PYTHONPATH}:${grid_base}/usr/lib64/python2.7/site-packages:${grid_base}/usr/lib/python2.7/site-packages"

    # law setup
    export LAW_HOME="${PROD_BASE}/.law"
    export LAW_CONFIG_FILE="${PROD_BASE}/law.cfg"
    source "$( law completion )"
    law index --verbose

    export PROD_SETUP="1"
}


action "$@"
