#!/usr/bin/env bash


htcondor_bootstrap () {

    # use entrypoint directory of HTCondor job for setting $HOME
    export SPAWN_DIR="$( pwd )"
    export USER="{{user}}"
    echo "spawn dir of bootstrap script: ${SPAWN_DIR}"
    echo "user:                          ${USER}"

    # mark this job as remote job
    export TMR_REMOTE_JOB="1"

    # make law wlcg tools accessible
    source "{{wlcg_tools}}" "" || return "$?"

    # get the repository and extract it
    local tmr_base="${SPAWN_DIR}/tautau-mass-reconstruction"
    mkdir -p "${tmr_base}"
    (
        cd "${tmr_base}" &&
        law_wlcg_get_file "{{tmr_repo_uris}}" "{{tmr_repo_pattern}}" "repo.tgz" &&
        tar -xzf "repo.tgz" -C "${tmr_base}" &&
        rm "repo.tgz"
    ) || return "$?"

    # extract conda
    local tmr_conda_base="${tmr_base}/software/conda"
    mkdir -p "${tmr_conda_base}"
    (
        cd "${tmr_conda_base}" &&
        law_wlcg_get_file "{{tmr_conda_uris}}" "{{tmr_conda_pattern}}" "conda.tgz" &&
        tar -xzf "conda.tgz" &&
        rm "conda.tgz" &&
        source "${tmr_conda_base}/bin/activate" &&
        conda-unpack
    ) || return "$?"

    # setup the environment
    cd "${tmr_base}" || return "$?"
    source "${tmr_base}/setup.sh" "" || return "$?"

    return "0"
}


{{tmr_bootstrap_name}}_bootstrap "$@"
