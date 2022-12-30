#!/usr/bin/env bash


setup_conda () {
    # local variables pointing to some local paths
    local shell_is_zsh this_file
    shell_is_zsh="$( [[ -z "${ZSH_VERSION}" ]] && echo "false" || echo "true" )"
    this_file="$( [[ "${shell_is_zsh}" == "true" ]] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]:-${0}}" )"

    # check environment variables
    if [[ -z "${PROD_CONDA_BASE}" ]]; then
        2>&1 echo "environment variable \$PROD_CONDA_BASE must not be empty (needed by script ${this_file})"
        return "1"
    fi

    # local variables for conda installation
    local miniconda_install_script_url="https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh"
    local miniconda_install_script="miniconda_install_script.sh"
    
    local install_flag_file="${PROD_CONDA_BASE}/install_flag_file"

    # check if conda installation is necessary (check if installation flag file is already present in conda base directory)
    local install_conda
    install_conda="$( [[ -f "${install_flag_file}" ]] && echo "false" || echo "true" )"

    # install 
    if [[ "${install_conda}" == "true" ]]; then
        # fetch install script and install miniconda
        wget "${miniconda_install_script_url}" -O "${miniconda_install_script}" || return "$?"
        bash "${miniconda_install_script}" -b -u -p "${PROD_CONDA_BASE}" || return "$?"
        rm "miniconda_install_script.sh" || return "$?"
        cat << EOF >> "${PROD_CONDA_BASE}/.condarc"
changeps1: false
channels:
    - conda-forge
    - defaults
EOF
    fi

    source "${PROD_CONDA_BASE}/etc/profile.d/conda.sh" "" || return "$?"
    conda activate || return "$?"

    if [[ "${install_conda}" == "true" ]]; then
        conda install --yes luigi law jinja2 || return "$?"

        touch "${install_flag_file}"
        echo "$( date +%s )" >> "${install_flag_file}"
    fi

    # prepend prioritized paths if the corresponding environment variables have been set
    if [[ ! -z "${PROD_PREPEND_PATH}" ]]; then
        export PATH="${PROD_PREPEND_PATH}:${PATH}"
    fi
    if [[ ! -z "${PROD_PREPEND_PYTHONPATH}" ]]; then
        export PYTHONPATH="${PROD_PREPEND_PYTHONPATH}:${PYTHONPATH}"
    fi

}


setup_conda "$@"
