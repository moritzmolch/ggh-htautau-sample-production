#!/usr/bin/env bash


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
    export PROD_JOBS_BASE="${PROD_BASE}/jobs"
    export PROD_BUNDLE_BASE="${PROD_BASE}/bundle"
    export PROD_SOFTWARE_BASE="${PROD_BASE}/software"
    export PROD_SOFTWARE_LOCAL_BASE="${PROD_SOFTWARE_BASE}/local"

    # CMSSW settings (architecture and version)
    export PROD_SCRAM_ARCH="slc7_amd64_gcc700"
    export PROD_CMSSW_VERSION="CMSSW_10_6_29_patch1"
    export PROD_CMSSW_BASE="${PROD_SOFTWARE_BASE}/cmssw/${PROD_CMSSW_VERSION}"

    # helper functions for adding python packages and binaries
    prod_pip_install () {
        pip install --ignore-installed --no-cache-dir --prefix "${PROD_LOCAL_BASE}" "$@"
    }
    export -f prod_pip_install

    prod_add_bin () {
        if [[ ! -z "${1}" ]]; then
            export PATH="${1}:${PATH}"
        fi
    }
    export -f prod_add_bin

    prod_add_py () {
        if [[ ! -z "${1}" ]]; then
            export PYTHONPATH="${1}:${PYTHONPATH}"
        fi
    }
    export -f prod_add_bin

    # set up CMSSW
    export SCRAM_ARCH="${PROD_SCRAM_ARCH}"
    export CMSSW_VERSION="${PROD_CMSSW_VERSION}"
    export CMSSW_BASE="${PROD_CMSSW_BASE}"

    # install CMSSW if the CMSSW directory doesn't exist
    if [[ ! -d "${CMSSW_BASE}" ]]; then

        (
            source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" &&
            mkdir -p "$( dirname "${CMSSW_BASE}" )" &&
            scramv1 project CMSSW "${CMSSW_VERSION}" &&
            cd "${CMSSW_VERSION}" &&
            eval "$( scramv1 runtime -sh )" &&
            scram b 
        ) || return "$?"
    fi

    # activate CMSSW installation
    cd "${CMSSW_BASE}/src" || return "$?"
    eval "$( scramv1 runtime -sh )" || return "$?"
    cd "${current_dir}" || return "$?"

    # environment variables related to additional software
    export GLOBUS_THREAD_MODEL="none"
    export PYTHONWARNINGS="ignore"
    export PROD_ORIG_PYTHONPATH="${PYTHONPATH}"
    export PROD_GFAL_PLUGIN_DIR_ORIG="${GFAL_PLUGIN_DIR}"
    export PROD_GFAL_PLUGIN_DIR="${PROD_SOFTWARE_LOCAL_BASE}/gfal_plugins"

    # install python packages
    prod_pip_install luigi
    prod_pip_install law
    prod_pip_install order

    prod_add_bin "${PROD_SOFTWARE_LOCAL_BASE}/bin"
    prod_add_py "${PROD_SOFTWARE_LOCAL_BASE}/lib/python2.7/site-packages"

    # gfal setup
    # ckeck if gfal2 bindings are installed
    local gfal2_bindings_file
    gfal2_bindings_file="$( python -c "import gfal2; print(gfal2.__file__)" &> /dev/null )"
    [[ "$?" != "0" ]] && gfal2_bindings_file=""

    if [ ! -z "$gfal2_bindings_file" ]; then
        ln -s "$gfal2_bindings_file" "$PROD_SOFTWARE_LOCAL_BASE/lib/python2.7/site-packages"
        export GFAL_PLUGIN_DIR="$PROD_GFAL_PLUGIN_DIR_ORIG"
        source "$(law location)/contrib/cms/scripts/setup_gfal_plugins.sh" "${PROD_GFAL_PLUGIN_DIR}"
        unlink "$HGC_GFAL_PLUGIN_DIR/libgfal_plugin_http.so"
    fi
    export GFAL_PLUGIN_DIR="${PROD_GFAL_PLUGIN_DIR}"

    # law setup
    export LAW_HOME="${PROD_BASE}/.law"
    export LAW_CONFIG_FILE="${PROD_BASE}/law.cfg"
    source "$( law completion )"
    law index --verbose

    export PROD_SETUP="1"
}


action "$@"
