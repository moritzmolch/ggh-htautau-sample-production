#!/usr/bin/env bash


action () {
    local shell_is_zsh this_file this_dir
    shell_is_zsh="$( [[ -z "${ZSH_VERSION}" ]] && echo "false" || echo "true" )"
    this_file="$( [[ "${shell_is_zsh}" == "true" ]] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]:-${0}}" )"
    this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # check global variables
    if [[ -z "${PROD_SOFTWARE_BASE}" ]]; then
        2>&1 echo "environment variable \$PROD_SOFTWARE_BASE must not be empty (needed by script ${this_file})"
        return "1"
    fi

    PROD_CMSSW_BASE="${PROD_SOFTWARE_BASE}/cmssw"
    PROD_CMSSW_ENV_NAME="$( basename "${this_file%.sh}" )"
    PROD_CMSSW_VERSION="CMSSW_10_6_29_patch1"
    PROD_SCRAM_ARCH="slc7_amd64_gcc700"

    export PROD_CMSSW_BASE PROD_CMSSW_ENV_NAME PROD_CMSSW_VERSION PROD_SCRAM_ARCH

    source "${this_dir}/_setup_cmssw.sh" "$@"
}


action "$@"
