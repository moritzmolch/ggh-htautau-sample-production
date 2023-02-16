#!/usr/bin/env bash


setup_crown () {

    # check environment variables
    if [[ -z "${PROD_SOFTWARE_BASE}" ]]; then
        >&2 "environment variable \$PROD_SOFTWARE_BASE must not be empty (required by script ${this_file})"
        return "1"
    fi

    # set environment variables
    export PROD_CROWN_PATH="${PROD_SOFTWARE_BASE}/crown"
    export PROD_CROWN_TAU_ANALYSIS_PATH="${PROD_CROWN_PATH}/analysis_configurations/tau"

    # pull CROWN and the tau analysis configuration
    if [[ ! -d "${PROD_CROWN_PATH}" ]]; then
        git clone --recurse-submodules git@github.com:kit-cms/crown.git "${PROD_CROWN_PATH}" || return "$?"
    fi
    if [[ ! -d "${PROD_CROWN_TAU_ANALYSIS_PATH}" ]]; then
        git clone git@github.com:moritzmolch/tauanalysis-crown.git -b ggh-htautau-pythia8-fastsim "${PROD_CROWN_TAU_ANALYSIS_PATH}" || return "$?"
    fi

    # source the CROWN init script
    source "${PROD_CROWN_PATH}/init.sh" "" || return "$?"

}


setup_crown "$@"