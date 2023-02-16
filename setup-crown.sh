#!/usr/bin/env bash




_setup_software_stack () {
    # local variables pointing to some local paths
    local shell_is_zsh this_file
    shell_is_zsh="$( [[ -z "${ZSH_VERSION}" ]] && echo "false" || echo "true" )"
    this_file="$( [[ "${shell_is_zsh}" == "true" ]] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]:-${0}}" )"
    this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # local variables for the conda installation
    local miniconda_install_script_url="https://repo.anaconda.com/miniconda/Miniconda3-py39_22.11.1-1-Linux-x86_64.sh"
    local miniconda_install_script="${this_dir}/miniconda_install_script.sh"
    local pyversion="3.9"

    # check if required environment variables are defined
    if [[ -z "${PROD_CONDA_BASE}" ]]; then
        2>&1 echo "environment variable \$PROD_CONDA_BASE must not be empty (needed by script ${this_file})"
        return "1"
    fi

    # environment variables related to the behaviour of python programs
    export PYTHONWARNINGS="${PYTHONWARNINGS:-ignore}"
    export VIRTUAL_ENV_DISABLE_PROMPT="${VIRTUAL_ENV_DISABLE_PROMPT:-1}"

    # variables related to the grid environment
    export GLOBUS_THREAD_MODEL="${GLOBUS_THREAD_MODEL:-none}"
    export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
    export X509_VOMS_DIR="${X509_VOMS_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/vomsdir}"
    export X509_VOMSES="${X509_VOMSES:-/cvmfs/grid.cern.ch/etc/grid-security/vomses}"
    export VOMS_USERCONF="${VOMS_USERCONF:-${X509_VOMSES}}"
    export X509_USER_PROXY="${X509_USER_PROXY:-/tmp/x509up_u${user_id}}"

    # define important paths that should always be prioritized
    export PROD_PREPEND_PATH="${PROD_MODULES_BASE}/law/bin:${PROD_SOFTWARE_BASE}/local/bin"
    export PROD_PREPEND_PYTHONPATH="${PROD_BASE}:${PROD_MODULES_BASE}/law:${PROD_MODULES_BASE}/order:${PROD_MODULES_BASE}/scinum"

    # set PATH and PYTHONPATH properly
    export PATH="${PROD_PREPEND_PATH}:${PATH}"
    export PYTHONPATH=""
    export PYTHONPATH="${PROD_PREPEND_PYTHONPATH}:${PYTHONPATH}:${PROD_CONDA_BASE}/lib/python${pyversion}/site-packages"

    # local variable that tells us if conda needs to be installed
    local require_conda_install
    require_conda_install="$( [[ ! -d "${PROD_CONDA_BASE}" ]] && echo "true" || echo "false" )"
    
    # local variable that points to the part of the flag file indicating an ongoing or failed installation
    local pending_install_file="${PROD_CONDA_BASE}/pending_conda_install"

    # install conda if needed
    if [[ "${require_conda_install}" = "true" ]]; then
        # create a flag file for the installation that contains the timestamp when the installation
        # has been started
        mkdir -p "${PROD_CONDA_BASE}"
        date +%s > "${pending_install_file}"

        # perform the installation
        local ret_code
        (
            wget "${miniconda_install_script_url}" -O "${miniconda_install_script}" && \
            bash "${miniconda_install_script}" -b -u -p "${PROD_CONDA_BASE}" && \
            rm "miniconda_install_script.sh"
        )

        # clean up if installation has failed
        ret_code="$?" 
        if [[ "${ret_code}" != "0" ]]; then
            [[ -d "${PROD_CONDA_BASE}" ]] && rm -rf "${PROD_CONDA_BASE}"
            [[ -f "${pending_install_file}" ]] && rm -f "${pending_install_file}"
            return "${ret_code}"
        fi

        # write some useful configuration to the base .condarc file
        cat << EOF >> "${PROD_CONDA_BASE}/.condarc"
changeps1: false
channels:
    - conda-forge
    - defaults
EOF
    fi

    # activate the conda environment
    source "${PROD_CONDA_BASE}/etc/profile.d/conda.sh" "" || return "$?"
    conda activate || return "$?"

    # continue with the installation of the base environment

    if [[ "${require_conda_install}" == "true" ]]; then
        # install some libraries
        conda install --yes libgcc gfal2 gfal2-util python-gfal2 git git-lfs conda-pack || return "$?"
        conda install --yes six luigi || return "$?"
        conda install --yes jinja2 || return "$?"
        conda clean --yes --all

        # add gfal2 activation script to the conda activate.d directory
        mkdir -p "${PROD_CONDA_BASE}/etc/conda/activate.d"
        cat << EOF > "${PROD_CONDA_BASE}/etc/conda/activate.d/gfal_activate.sh"
export GFAL_CONFIG_DIR="\${CONDA_PREFIX}/etc/gfal2.d"
export GFAL_PLUGIN_DIR="\${CONDA_PREFIX}/lib/gfal2-plugins"
export X509_CERT_DIR="${X509_CERT_DIR}"
export X509_VOMS_DIR="${X509_VOMS_DIR}"
export X509_VOMSES="${X509_VOMSES}"
export VOMS_USERCONF="${VOMS_USERCONF}"
EOF
    fi

    return "0"
}


action () {

    # do not run the setup twice
    if [[ "${PROD_SETUP}" = "1" ]]; then
        echo "production environment has already been set up"
        return "0"
    fi

    # directory of that script and current working directory
    local shell_is_zsh this_file this_dir
    shell_is_zsh="$( [[ -z "${ZSH_VERSION}" ]] && echo "false" || echo "true" )"
    this_file="$( [[ "${shell_is_zsh}" == "true" ]] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]:-${0}}" )"
    this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    current_dir="$( pwd )"

    # define important paths of this project as well as storage targets
    export PROD_BASE="${this_dir}"
    export PROD_MODULES_BASE="${PROD_BASE}/modules"
    export PROD_SOFTWARE_BASE="${PROD_BASE}/software"

    export PROD_CONDA_BASE="${PROD_SOFTWARE_BASE}/conda"
    _setup_software_stack

    # set different store locations
    # export PROD_DATA_STORE="${PROD_BASE}/data"
    export PROD_DATA_STORE="/work/mmolch/ggh-htautau-sample-production/data"
    export PROD_JOBS_STORE="${PROD_BASE}/jobs"
    export PROD_BUNDLE_STORE="${PROD_BASE}/bundle"

    # law setup
    export LAW_HOME="${PROD_BASE}/.law"
    export LAW_CONFIG_FILE="${PROD_BASE}/law.cfg"
    source "$( law completion )"
    law index -q

    export PROD_SETUP="1"
}


action "$@"

