#!/usr/bin/env bash


action () {

    # directory of that script and current working directory 
    local base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local current_dir="$( pwd )"

    # production base paths
    export PROD_BASE_PATH="${base_dir}"
    export PROD_BASE_DATA_PATH="${PROD_BASE_PATH}/data"
    export PROD_BASE_CONFIG_PATH="${PROD_BASE_PATH}/config"

    # law and luigi paths
    export LAW_HOME="${base_dir}/.law"
    export LAW_CONFIG_FILE="${base_dir}/law.cfg"
    export LUIGI_CONFIG_PATH="${base_dir}/luigi.cfg"

    # add tasks as library to PYTHONPATH
    export PYTHONPATH="${PYTHONPATH}:${PROD_BASE_PATH}"

}


action "$@"
