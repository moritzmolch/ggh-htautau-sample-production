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
    if [[ -z "${PROD_CMSSW_ENV_NAME}" ]]; then
        2>&1 echo "environment variable \$PROD_CMSSW_ENV_NAME must not be empty (needed by script ${this_file})"
        return "2"
    fi
    if [[ -z "${PROD_CMSSW_VERSION}" ]]; then
        2>&1 echo "environment variable \$PROD_CMSSW_VERSION must not be empty (needed by script ${this_file})"
        return "3"
    fi
    if [[ -z "${PROD_SCRAM_ARCH}" ]]; then
        2>&1 echo "environment variable \$PROD_SCRAM_ARCH must not be empty (needed by script ${this_file})"
        return "4"
    fi

    source "${this_dir}/_setup_cmssw.sh" "$@"
}


action "$@"
