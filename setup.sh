#!/usr/bin/env bash


action () {

    if [[ ! -z "${PROD_SETUP}" && "${PROD_SETUP}" == "1" ]]; then
        2>&1 echo "environment has already been set up"
        return "1"
    fi



    # directory of that script and current working directory
    local shell_is_zsh this_file this_dir
    shell_is_zsh="$( [[ -z "${ZSH_VERSION}" ]] && echo "false" || echo "true" )"
    this_file="$( [[ "${shell_is_zsh}" == "true" ]] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]:-${0}}" )"
    this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # set project base paths
    export PROD_BASE="${this_dir}"
    export PROD_SOFTWARE_BASE="${PROD_BASE}/software"
    export PROD_CONDA_BASE="${PROD_SOFTWARE_BASE}/conda"
    export PROD_VENV_BASE="${PROD_SOFTWARE_BASE}/venvs"
    export PROD_CMSSW_BASE="${PROD_SOFTWARE_BASE}/cmssw"

    # set environment variables for CMSSW installation
    export PROD_CMSSW_ENV_NAME="cmssw_default"
    export PROD_CMSSW_VERSION="CMSSW_10_6_29_patch1"
    export PROD_SCRAM_ARCH="slc7_amd64_gcc700"
    export PROD_CMSSW_PATH="${PROD_CMSSW_BASE}/${PROD_CMSSW_ENV_NAME}/${PROD_CMSSW_VERSION}"

    # save original paths for binaries and libraries
    export PROD_ORIG_PATH="${PATH}"
    export PROD_ORIG_LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"
    export PROD_ORIG_PYTHONPATH="${PYTHONPATH}"
    export PROD_ORIG_PYTHON3PATH="${PYTHON3PATH}"

    # define persistent parts of PATH and PYTHONPATH that should always be prepended to the effective PATH and
    # PYTHONPATH of the current environment
    # these parts contain special binaries and libraries that should always be prioritized, e.g. the
    # 'production/' directory that contains the task definitions
    export PROD_PREPEND_PATH="${PROD_SOFTWARE_BASE}/local/bin"
    export PROD_PREPEND_PYTHONPATH="${PROD_BASE}"

    # provide python environment with conda
    source "${this_dir}/sandboxes/_setup_conda.sh" "" || return "$?"

    # law and luigi paths
    export LAW_HOME="${PROD_BASE}/.law"
    export LAW_CONFIG_FILE="${PROD_BASE}/law.cfg"
    export LUIGI_CONFIG_PATH="${PROD_BASE}/luigi.cfg"

    if [[ "$( which law > /dev/null )" && "$?" == "0" ]]; then
        source "$( law completion )" || return "$?"
        law index -q
    fi

    export PROD_SETUP="1"
}


action "$@"
