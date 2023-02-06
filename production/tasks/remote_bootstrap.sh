#!/usr/bin/env bash


htcondor_bootstrap () {

    # use entrypoint directory of HTCondor job for setting $HOME
    SPAWN_DIR="$( pwd )"
    USER="{{user}}"
    echo "spawn dir of bootstrap script: ${SPAWN_DIR}"
    echo "user:                          ${USER}"

    # make law wlcg tools accessible
    source "{{wlcg_tools}}" "" || return "$?"

    # get the repository and extract it
    local prod_base="${SPAWN_DIR}/htautau-sample-production"
    mkdir -p "${prod_base}"
    (
        cd "${prod_base}" &&
        law_wlcg_get_file "{{prod_repo_uris}}" "{{prod_repo_pattern}}" "repo.tgz" &&
        tar -xzf "repo.tgz" -C "${prod_base}" &&
        rm "repo.tgz"
    ) || return "$?"

    # setup the environment
    cd "${prod_base}" || return "$?"
    source "${prod_base}/setup.sh" "" || return "$?"

    export SCRAM_ARCH="${PROD_SCRAM_ARCH}"

    # extract and build additional CMSSW source files
    (
        cd "${CMSSW_BASE}" &&
        law_wlcg_get_file "{{prod_cmssw_uris}}" "{{prod_cmssw_pattern}}" "cmssw.tgz" &&
        tar -xzf "cmssw.tgz" &&
        rm "cmssw.tgz" &&
        cd "${CMSSW_BASE}/src" &&
        eval "$( scramv1 runtime -sh )" &&
        scramv1 build
    ) || return "$?"

    return "0"

}


htcondor_bootstrap "$@"