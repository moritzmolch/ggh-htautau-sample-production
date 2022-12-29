#!/usr/bin/env bash

_setup_venv () {
    local install_base="${1}"
    local venv_name="${2}"
    local requirements_file="${3}"

    if [[ -d "${install_base}/${venv_name}" ]]; then
        return
    fi
    python3 -m venv "${install_base}/${venv_name}" "" || ( rm -rf "${install_base}/${venv_name}" && return "$?" )
    source "${install_base}/${venv_name}/bin/activate" || ( rm -rf "${install_base}/${venv_name}" && return "$?" )
    python3 -m pip install -U pip  || ( rm -rf "${install_base}/${venv_name}" && return "$?" )
    python3 -m pip install -r "${requirements_file}" || ( rm -rf "${install_base}/${venv_name}" && return "$?" )
    deactivate || return "$?"
}

action () {
    # directory of that script and current working directory
    local base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local current_dir="$( pwd )"

    # set project base paths
    export PROD_BASE="${base_dir}"
    export PROD_SOFTWARE_BASE="${PROD_BASE}/software"
    export PROD_VENV_BASE="${PROD_SOFTWARE_BASE}/venvs"

    _setup_venv "${PROD_VENV_BASE}" "venv_base" "${PROD_BASE}/requirements.txt" || return "$?"
    source "${PROD_VENV_BASE}/venv_base/bin/activate" "" || return "$?"
    export PYTHONPATH="${PROD_BASE}:${PYTHONPATH}"

    # law and luigi paths
    export LAW_HOME="${PROD_BASE}/.law"
    export LAW_CONFIG_FILE="${PROD_BASE}/law.cfg"
    export LUIGI_CONFIG_PATH="${PROD_BASE}/luigi.cfg"

    law index -q

}


action "$@"
