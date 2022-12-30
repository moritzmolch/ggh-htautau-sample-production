#!/usr/bin/env bash


action () {
    local shell_is_zsh this_file this_dir
    local shell_is_zsh="$( [[ -z "${ZSH_VERSION}" ]] && echo "false" || echo "true" )"
    local this_file="$( [[ "${shell_is_zsh}" == "true" ]] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]:-${0}}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # check global variables
    if [[ -z "${PROD_SOFTWARE_BASE}" ]]; then
        2>&1 echo "environment variable \$PROD_SOFTWARE_BASE must not be empty (needed by script ${this_file})"
        return "1"
    fi

    export PROD_CMSSW_BASE="${PROD_SOFTWARE_BASE}/cmssw"
    export PROD_CMSSW_ENV_NAME="$( basename "${this_file%.sh}" )"
    export PROD_CMSSW_VERSION="CMSSW_10_6_29_patch1"
    export PROD_SCRAM_ARCH="slc7_amd64_gcc700"

    source "${this_dir}/_setup_cmssw.sh" "$@"
}


action "$@"
