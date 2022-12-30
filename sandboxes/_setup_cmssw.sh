#!/usr/bin/env bash


setup_cmssw () {
    local shell_is_zsh this_file orig_pwd
    shell_is_zsh="$( [[ -z "${ZSH_VERSION}" ]] && echo "false" || echo "true" )"
    this_file="$( [[ "${shell_is_zsh}" == "true" ]] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]:-${0}}" )"
    orig_pwd="$( pwd )"

    # check environment variables
    if [[ -z "${PROD_CMSSW_BASE}" ]]; then
        2>&1 echo "environment variable \$PROD_CMSSW_BASE must not be empty (needed by script ${this_file})"
        return "1"
    fi
    if [[ -z "${PROD_CMSSW_ENV_NAME}" ]]; then
        2>&1 echo "environment variable \$PROD_CMSSW_ENV_NAME must not be empty (needed by script ${this_file})"
        return "2"
    fi
    if [[ -z "${PROD_CMSSW_VERSION}" ]]; then
        2>&1 echo "environment variable \$PROD_CMSSW_VERSION must not be empty (needed by script ${this_file})"
        return "3"
    fi
    if [[ -z "${PROD_SCRAM_ARCH}" ]]; then
        2>&1 echo "environment variable \$PROD_SCRAM_ARCH must not be empty (needed by script ${this_file})"
        return "4"
    fi

    # local variables for CMSSW installation
    local install_base="${PROD_CMSSW_BASE}/${PROD_CMSSW_ENV_NAME}"
    local install_flag_file="${install_base}/install_flag_file"

    # check if CMSSW installation is necessary (check if installation flag file is already present in CMSSW environment directory)
    local install_cmssw
    install_cmssw="$( [[ -f "${install_flag_file}" ]] && echo "false" || echo "true" )"

    if [[ "${install_cmssw}" == "true" ]]; then
        # abort if CMS environment directory already exists
        if [[ -d "${install_base}" ]]; then
            2>&1 echo "CMSSW environment directory ${install_base} already exists, delete it before installing CMSSW with the script ${this_file}"
            return "5"
        fi

        # set the environment for installation
        source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"
        export SCRAM_ARCH="${PROD_SCRAM_ARCH}"

        # perform the installation
        echo "installation if ${PROD_CMSSW_VERSION} is started"
        mkdir -p "${install_base}" || return "$?"
        cd "${install_base}" || return "$?"
        scramv1 project CMSSW "${PROD_CMSSW_VERSION}" || return "$?"
        cd "${install_base}/${PROD_CMSSW_VERSION}/src" || return "$?"
        eval "$( scramv1 runtime -sh )" || return "$?"
        scram build || return "$?"
        cd "${install_base}" || return "$?"
        touch "${install_flag_file}" || return "$?"
        echo "$( date +%s )" >> "${install_flag_file}"
        cd "${orig_pwd}" || return "$?"

        echo "finished installation of ${PROD_CMSSW_VERSION} successfully"
    fi
   
    local cmssw_path
    cmssw_path="${install_base}/${PROD_CMSSW_VERSION}"

    # if src/ directory in CMSSW directory tree doesn't exist something went wrong
    if [[ ! -d "${cmssw_path}/src" ]]; then
        2>&1 echo "src directory in CMSSW installation ${cmssw_path} not found"
    fi

    # source the CMSSW installation
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"
    export SCRAM_ARCH="${PROD_SCRAM_ARCH}"
    export CMSSW_VERSION="${PROD_CMSSW_VERSION}"
    cd "${install_base}/${PROD_CMSSW_VERSION}/src" || return "$?"
    eval "$( scramv1 runtime -sh )" || return "$?"
    cd "${orig_pwd}" || return "$?"

    # prepend prioritized paths if the corresponding environment variables have been set
    if [[ ! -z "${PROD_PREPEND_PATH}" ]]; then
        export PATH="${PROD_PREPEND_PATH}:${PATH}"
    fi
    if [[ ! -z "${PROD_PREPEND_PYTHONPATH}" ]]; then
        export PYTHONPATH="${PROD_PREPEND_PYTHONPATH}:${PYTHONPATH}"
    fi
}


setup_cmssw "$@"
