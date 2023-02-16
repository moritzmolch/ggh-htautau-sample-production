import order as od
from collections import OrderedDict
from typing import Sequence


def _create_channel_label(key):
    first, second = key[0], key[1]
    label = ""
    for letter in [first, second]:
        if letter == "e":
            label += "e"
        elif letter == "m":
            label += "\\mu"
        elif letter == "t":
            label += "\\tau_{\\mathrm{h}}"
    return "$" + label + "$"


def _get_base_filename(process):
    base_filename = "GluGluHToTauTau_MH{higgs_mass:d}_{generator:s}_Tune{tune:s}".format(
        higgs_mass=process.get_aux("higgs_mass"),
        generator=process.get_aux("event_generator"),
        tune=process.get_aux("tune"),
    )
    return base_filename


def _get_number_of_files(process: od.Process):
    return 15


def _get_number_of_events_per_file(process: od.Process):
    if process.higgs_mass < 250:
        return 2000
    else:
        return 4000

    base_filename = "GluGluHToTauTau_MH{higgs_mass:d}_{generator:s}_Tune{tune:s}".format(
        higgs_mass=process.get_aux("higgs_mass"),
        generator=process.get_aux("event_generator"),
        tune=process.get_aux("tune"),
    )
    return base_filename


def add_ggh_htautau_processes(cfg: od.Config):
    generator = "pythia8"
    tune = "CP5"
    for i, higgs_mass in enumerate(list(range(50, 250, 1)) + list(range(250, 805, 5))):
        cfg.add_process(
            name="ggh_htautau_mh{higgs_mass:d}".format(higgs_mass=higgs_mass),
            id=i + 1,
            label="$\\mathrm{g}\\mathrm{g} \\to \\mathrm{H}({higgs_mass:d}\\,\\mathrm{GeV}) \\to \\tau\\tau$",
            label_short="$\\mathrm{{ggH}}({higgs_mass:d}) \\to \\tau\\tau$".format(
                higgs_mass=higgs_mass
            ),
            aux=dict(
                higgs_mass=higgs_mass,
                event_generator="pythia8",
                tune="CP5",
                fragment="{base_filename:s}_".format(
                    base_filename=_get_base_filename(higgs_mass, generator, tune)
                ),
            ),
        )


def add_cms_datasets(cfg: od.Config):
    _step = cfg.step
    for i, _process in enumerate(_cfg.processes):
        _n_files = _get_number_of_files(process)
        _n_events_per_file = _get_number_of_events_per_file(process)
        _n_events = _n_files * _n_events_per_file
        _filename_prefix = "{base_filename:s}_{step:s}".format(
            base_filename=_get_base_filename(_process), step=_step
        )
        _cfg.add_dataset(
            name="{process:s}_{step:s}".format(process=_process.name, step=_step),
            id=i + 1,
            processes=[_process],
            keys=[
                "/{step:s}/{filename_prefix:s}_{file_index:s}.root".format(
                    step=_step, filename_prefix=_filename_prefix, file_index=_file_index
                )
                for _file_index in cfg.n_files
            ],
            n_files=_n_files,
            n_events=_n_events,
            aux=dict(
                n_events_per_file=_n_events_per_file,
                filename_prefix=_filename_prefix,
            ),
        )


def add_ntuple_datasets(cfg: od.Config):
    _step = cfg.step
    for i, _process in enumerate(_cfg.processes):
        _filename_prefix = "{base_filename:s}_{step:s}".format(
            base_filename=_get_base_filename(_process), step=_step
        )
        _cfg.add_dataset(
            name="{process:s}_{step:s}".format(process=_process.name, step=_step),
            id=i + 1,
            processes=[_process],
            keys=[
                "/{step:s}/{filename_prefix:s}_{channel:s}.root".format(
                    step=_step, filename_prefix=_filename_prefix, channel=_channel
                )
                for _channel in _cfg.channels
            ],
            n_files=len(_cfg.channels),
            aux=dict(
                filename_prefix=_filename_prefix,
            ),
        )


def add_ditau_channels(cfg: od.Config):
    for i, key in enumerate(["mt", "et", "tt", "em", "mm", "ee"]):
        cfg.add_channel(
            od.Channel(
                name=key,
                id=i + 1,
                label=_create_channel_label(key),
            )
        )


def create_base_config(analysis: od.Analysis, campaign: od.Campaign, name: str) -> od.Config:
    """
    Base configuration for the production.
    """
    cfg_base = analysis.add_config(campaign, name=name, id=0)
    add_ggh_htautau_processes(cfg_base)
    return cfg_base


def create_cms_config(
    cfg_base: od.Config,
    cfg_previous_step: od.Config,
    step_name: str,
    cms_driver_kwargs: OrderedDict,
    cms_driver_args: Sequence,
) -> od.Config:
    _step = step_name
    _cfg = cfg_base.copy()

    # add some auxiliary data like the step name and the config of the previous step
    _cfg.set_aux("step", _step)
    _cfg.set_aux("previous", cfg_previous_step)

    # add arguments for the cmsDriver.py script to generate production configurations
    _cfg.set_aux("cms_driver_kwargs", cms_driver_kwargs)
    _cfg.set_aux("cms_driver_args", cms_driver_args)

    # add the datasets
    add_cms_datasets(_cfg)

    return _cfg


def create_aodsim_config(
    cfg_base: od.Config,
    cfg_previous_step: od.Config,
    step_name: str,
    cms_driver_kwargs: OrderedDict,
    cms_driver_args: Sequence,
) -> od.Config:
    cms_driver_kwargs = OrderedDict(
        {
            "customise": "Configuration/DataProcessing/Utils.addMonitoring",
            "customise_commands": "from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;randSvc"
            + " = RandomNumberServiceHelper(process.RandomNumberGeneratorService);randSvc.populate()",
            "step": "GEN,SIM,RECOBEFMIX,DIGI,DATAMIX,L1,DIGI2RAW,L1Reco,RECO",
            "datatier": "AODSIM",
            "eventcontent": "AODSIM",
            "pileup_input": "dbs:/Neutrino_E-10_gun/"
            + "RunIIFall17FSPrePremix-PUFSUL18CP5_106X_upgrade2018_realistic_v16-v1/PREMIX",
            "conditions": "106X_upgrade2018_realistic_v16_L1v1",
            "beamspot": "Realistic25ns13TeVEarly2018Collision",
            "procModifiers": "premix_stage2",
            "datamix": "PreMix",
            "era": "Run2_2018_FastSim",
        }
    )
    cms_driver_args = ["fast", "no_exec", "mc"]

    return create_cms_config(
        cfg_base,
        cfg_previous_step,
        step_name,
        cms_driver_kwargs,
        cms_driver_args,
    )


def create_miniaod_config(
    cfg_base: od.Config,
    cfg_previous_step: od.Config,
    step_name: str,
    cms_driver_kwargs: OrderedDict,
    cms_driver_args: Sequence,
) -> od.Config:
    cms_driver_kwargs = OrderedDict(
        {
            "customise": "Configuration/DataProcessing/Utils.addMonitoring",
            "customise_commands": "from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;randSvc"
            + " = RandomNumberServiceHelper(process.RandomNumberGeneratorService);randSvc.populate()",
            "step": "PAT",
            "datatier": "MINIAODSIM",
            "eventcontent": "MINIAODSIM",
            "conditions": "106X_upgrade2018_realistic_v16_L1v1",
            "beamspot": "Realistic25ns13TeVEarly2018Collision",
            "era": "Run2_2018",
            "procModifiers": "run2_miniAOD_UL",
            "geometry": "DB:Extended",
        }
    )
    cms_driver_args = ["fast", "no_exec", "runUnscheduled", "mc"]

    return create_cms_config(
        cfg_base,
        cfg_previous_step,
        step_name,
        cms_driver_kwargs,
        cms_driver_args,
    )


def create_nanoaod_config(
    cfg_base: od.Config,
    cfg_previous_step: od.Config,
    step_name: str,
    cms_driver_kwargs: OrderedDict,
    cms_driver_args: Sequence,
) -> od.Config:
    cms_driver_kwargs = OrderedDict(
        {
            "customise": "Configuration/DataProcessing/Utils.addMonitoring",
            "customise_commands": "from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;randSvc"
            + " = RandomNumberServiceHelper(process.RandomNumberGeneratorService);randSvc.populate()",
            "step": "NANO",
            "datatier": "NANOAODSIM",
            "eventcontent": "NANOAODSIM",
            "conditions": "106X_upgrade2018_realistic_v16_L1v1",
            "beamspot": "Realistic25ns13TeVEarly2018Collision",
            "era": "Run2_2018,run2_nanoAOD_106Xv2",
        }
    )
    cms_driver_args = ["fast", "no_exec", "mc"]

    return create_cms_config(
        cfg_base,
        cfg_previous_step,
        step_name,
        cms_driver_kwargs,
        cms_driver_args,
    )


def create_ntuple_config(cfg_base: od.Config, cfg_previous_step: od.Config) -> od.Config:
    _step = "ntuple"
    _cfg = cfg_base.copy()

    # add some auxiliary data like the step name and the config of the previous step
    _cfg.set_aux("step", _step)
    _cfg.set_aux("previous", cfg_previous_step)

    # add di-tau decay channels
    add_ditau_channels(_cfg)

    # define one dataset per generated collection of files for a Higgs boson mass
    # each dataset is divided into one file per decay channel
    add_ntuple_datasets(_cfg)

    return _cfg


analysis = od.Analysis(name="ggh_htautau_mc_production", id=1)
campaign = od.Campaign("Run2_2018_UltraLegacy", id=1, ecm=13, bx=25)

cfg_base = create_base_config(analysis, campaign)
cfg_aodsim = create_aodsim_config(cfg_base, None)
cfg_miniaod = create_miniaod_config(cfg_base, cfg_aodsim)
cfg_nanoaod = create_nanoaod_config(cfg_base, cfg_miniaod)
cfg_ntuple = create_ntuple_config(cfg_base, cfg_nanoaod)
