#!/usr/bin/env bash


htcondor_bootstrap () {

    # use entrypoint directory of HTCondor job for setting $HOME
    SPAWN_DIR="$( pwd )"
    export HOME="${SPAWN_DIR}"

    # make law wlcg tools accessible
    source "{{wlcg_tools}}" "" || return "$?"

    # get the repository and extract it
    local prod_base="${SPAWN_DIR}/htautau-sample-production"
    (
        mkdir -p "${prod_base}" &&
        cd "${prod_base}" &&
        law_wlcg_get_file "{{prod_repo_uris}}" "{{prod_repo_pattern}}" "repo.tgz" &&
        tar -xzf "repo.tgz" -C "${prod_base}" &&
        rm "repo.tgz" &&
        ls -lh
    ) || return "$?"

    # set up conda
    local prod_conda_base="${prod_base}/{{prod_conda_base}}"
    (
        mkdir -p "${prod_conda_base}" &&
        cd "${prod_conda_base}" &&
        law_wlcg_get_file "{{prod_conda_uris}}" "{{prod_conda_pattern}}" "conda.tgz" &&
        tar -xzf "conda.tgz" &&
        rm "conda.tgz" &&
        ls -lh
    ) || return "$?"

    # setup the environment
    cd "${prod_base}" || return "$?"
    source "setup.sh" "" || return "$?"

    # extract and build additional CMSSW source files
    mkdir -p "${PROD_CMSSW_BASE}/${PROD_CMSSW_ENV_NAME}"
    (
        cd "${PROD_CMSSW_BASE}/${PROD_CMSSW_ENV_NAME}" &&
        source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" &&
        scramv1 project CMSSW "${CMSSW_VERSION}" &&
        cd "${CMSSW_VERSION}" &&
        law_wlcg_get_file "{{prod_cmssw_uris}}" "{{prod_cmssw_pattern}}" "cmssw.tgz" &&
        tar -xzf "cmssw.tgz" &&
        cd "src" &&
        eval "$( scramv1 runtime -sh )" &&
        scramv1 build &&
        ls -lh
    ) || return "$?"

    return "0"
}


htcondor_bootstrap "$@"