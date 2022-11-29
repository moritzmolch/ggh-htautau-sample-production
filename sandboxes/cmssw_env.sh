#!/usr/bin/env bash


action () {
    local current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

    # set CMS defaults
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || exit "$?"

    # source the CMSSW version
    cd "${PROD_CMSSW_BASE_PATH}" || exit "$?"
    eval "$( scramv1 runtime -sh )"
    cd "${current_dir}"
}


action "$@"
