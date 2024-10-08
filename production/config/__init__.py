import order as od


class ProductionConfig(object):

    _instance = None
    _analysis = None

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls)
            cls._analysis = cls.init_analysis()
        return cls._instance

    def get_analysis(self):
        return self.__class__._analysis

    @classmethod
    def init_analysis(cls):
        # the analysis
        ggh_htautau_production = od.Analysis(name="ggh_htautau_production", id=0)

        # campaigns - distinguish between different data tiers

        mc_ul18_fastsim_aodsim = od.Campaign(name="mc_ul18_fastsim_aodsim", id=10, ecm=13, bx=25)
        mc_ul18_fastsim_miniaod = od.Campaign(name="mc_ul18_fastsim_miniaod", id=20, ecm=13, bx=25)
        mc_ul18_fastsim_nanoaod = od.Campaign(name="mc_ul18_fastsim_nanoaod", id=30, ecm=13, bx=25)


        # configs that connect analysis with campaigns

        cfg_aodsim = ggh_htautau_production.add_config(
            mc_ul18_fastsim_aodsim,
            aux=dict(
                step="aodsim",
                cms_driver_kwargs={
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
                },
                cms_driver_args=["fast", "no_exec", "mc"],
            ),
        )

        cfg_miniaod = ggh_htautau_production.add_config(
            mc_ul18_fastsim_miniaod,
            aux=dict(
                step="miniaod",
                config_previous=cfg_aodsim.name,
                cms_driver_kwargs={
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
                },
                cms_driver_args=["fast", "no_exec", "runUnscheduled", "mc"],
            ),
        )

        cfg_nanoaod = ggh_htautau_production.add_config(
            mc_ul18_fastsim_nanoaod,
            aux=dict(
                step="nanoaod",
                config_previous=cfg_miniaod.name,
                cms_driver_kwargs={
                    "customise": "Configuration/DataProcessing/Utils.addMonitoring",
                    "customise_commands": "from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;randSvc"
                    + " = RandomNumberServiceHelper(process.RandomNumberGeneratorService);randSvc.populate()",
                    "step": "NANO",
                    "datatier": "NANOAODSIM",
                    "eventcontent": "NANOAODSIM",
                    "conditions": "106X_upgrade2018_realistic_v16_L1v1",
                    "beamspot": "Realistic25ns13TeVEarly2018Collision",
                    "era": "Run2_2018,run2_nanoAOD_106Xv2",
                },
                cms_driver_args=["fast", "no_exec", "mc"],
            ),
        )

        configs = [cfg_aodsim, cfg_miniaod, cfg_nanoaod]


        # processes -- define a process for each Higgs mass point

        ggh_htautau = od.Process(
            name="ggh_htautau",
            id=0,
            label="$\\mathrm{g}\\mathrm{g} \\to \\mathrm{H} \\to \\tau\\tau$",
        )

        i = 1

        for higgs_mass in range(30, 350, 1):
            ggh_htautau.add_process(
                name="ggh_htautau_mh{higgs_mass:d}".format(higgs_mass=higgs_mass),
                id=i,
                label="$\\mathrm{g}\\mathrm{g} \\to \\mathrm{H}({higgs_mass:d}\\,\\mathrm{GeV}) \\to \\tau\\tau$",
                aux=dict(
                    higgs_mass=higgs_mass,
                    filename_prefix="GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5".format(
                        higgs_mass=higgs_mass
                    ),
                ),
            )
            i += 1
        for higgs_mass in range(350, 805, 5):
            ggh_htautau.add_process(
                name="ggh_htautau_mh{higgs_mass:d}".format(higgs_mass=higgs_mass),
                id=i,
                label="$\\mathrm{g}\\mathrm{g} \\to \\mathrm{H}({higgs_mass:d}\\,\\mathrm{GeV}) \\to \\tau\\tau$",
                aux=dict(
                    higgs_mass=higgs_mass,
                    filename_prefix="GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5".format(
                        higgs_mass=higgs_mass
                    ),
                ),
            )
            i += 5
        for cfg in configs:
            cfg.add_process(ggh_htautau)


        # datasets -- define a dataset instance for each Higgs mass and for each data tier

        i_cfg = 1
        for cfg in configs:
            i_ds = 10
            for higgs_mass in range(30, 130, 1):
                n_files = 100
                n_events_per_file = 2000
                d = od.Dataset(
                    name="ggh_htautau_mh{higgs_mass:d}_{step:s}".format(
                        higgs_mass=higgs_mass, step=cfg.get_aux("step")
                    ),
                    id=i_ds + i_cfg,
                    processes=[
                        cfg.get_process("ggh_htautau_mh{higgs_mass:d}".format(higgs_mass=higgs_mass))
                    ],
                    keys=[
                        "/{step:s}/GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}_{file_index:d}.root".format(
                            step=cfg.get_aux("step"), higgs_mass=higgs_mass, file_index=i
                        )
                        for i in range(n_files)
                    ],
                    n_files=n_files,
                    n_events=n_events_per_file * n_files,
                    aux=dict(
                        n_events_per_file=n_events_per_file,
                        filename_prefix="GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}".format(
                            higgs_mass=higgs_mass, step=cfg.get_aux("step")
                        ),
                    ),
                )
                cfg.add_dataset(d)
                i_ds += 10
            for higgs_mass in range(130, 150, 1):
                n_files = 100
                n_events_per_file = 2000
                d = od.Dataset(
                    name="ggh_htautau_mh{higgs_mass:d}_{step:s}".format(
                        higgs_mass=higgs_mass, step=cfg.get_aux("step")
                    ),
                    id=i_ds + i_cfg,
                    processes=[
                        cfg.get_process("ggh_htautau_mh{higgs_mass:d}".format(higgs_mass=higgs_mass))
                    ],
                    keys=[
                        "/{step:s}/GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}_{file_index:d}.root".format(
                            step=cfg.get_aux("step"), higgs_mass=higgs_mass, file_index=i
                        )
                        for i in range(n_files)
                    ],
                    n_files=n_files,
                    n_events=n_events_per_file * n_files,
                    aux=dict(
                        n_events_per_file=n_events_per_file,
                        filename_prefix="GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}".format(
                            higgs_mass=higgs_mass, step=cfg.get_aux("step")
                        ),
                    ),
                )
                cfg.add_dataset(d)
                i_ds += 10
            for higgs_mass in range(150, 250, 1):
                n_files = 50
                n_events_per_file = 2000
                d = od.Dataset(
                    name="ggh_htautau_mh{higgs_mass:d}_{step:s}".format(
                        higgs_mass=higgs_mass, step=cfg.get_aux("step")
                    ),
                    id=i_ds + i_cfg,
                    processes=[
                        cfg.get_process("ggh_htautau_mh{higgs_mass:d}".format(higgs_mass=higgs_mass))
                    ],
                    keys=[
                        "/{step:s}/GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}_{file_index:d}.root".format(
                            step=cfg.get_aux("step"), higgs_mass=higgs_mass, file_index=i
                        )
                        for i in range(n_files)
                    ],
                    n_files=n_files,
                    n_events=n_events_per_file * n_files,
                    aux=dict(
                        n_events_per_file=n_events_per_file,
                        filename_prefix="GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}".format(
                            higgs_mass=higgs_mass, step=cfg.get_aux("step")
                        ),
                    ),
                )
                cfg.add_dataset(d)
                i_ds += 10
            for higgs_mass in range(250, 350, 1):
                n_files = 12
                n_events_per_file = 4000
                d = od.Dataset(
                    name="ggh_htautau_mh{higgs_mass:d}_{step:s}".format(
                        higgs_mass=higgs_mass, step=cfg.get_aux("step")
                    ),
                    id=i_ds + i_cfg,
                    processes=[
                        cfg.get_process("ggh_htautau_mh{higgs_mass:d}".format(higgs_mass=higgs_mass))
                    ],
                    keys=[
                        "/{step:s}/GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}_{file_index:d}.root".format(
                            step=cfg.get_aux("step"), higgs_mass=higgs_mass, file_index=i
                        )
                        for i in range(n_files)
                    ],
                    n_files=n_files,
                    n_events=n_events_per_file * n_files,
                    aux=dict(
                        n_events_per_file=n_events_per_file,
                        filename_prefix="GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}".format(
                            higgs_mass=higgs_mass, step=cfg.get_aux("step")
                        ),
                    ),
                )
                cfg.add_dataset(d)
                i_ds += 50
            for higgs_mass in range(350, 800, 5):
                n_files = 30
                n_events_per_file = 4000
                d = od.Dataset(
                    name="ggh_htautau_mh{higgs_mass:d}_{step:s}".format(
                        higgs_mass=higgs_mass, step=cfg.get_aux("step")
                    ),
                    id=i_ds + i_cfg,
                    processes=[
                        cfg.get_process("ggh_htautau_mh{higgs_mass:d}".format(higgs_mass=higgs_mass))
                    ],
                    keys=[
                        "/{step:s}/GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}_{file_index:d}.root".format(
                            step=cfg.get_aux("step"), higgs_mass=higgs_mass, file_index=i
                        )
                        for i in range(n_files)
                    ],
                    n_files=n_files,
                    n_events=n_events_per_file * n_files,
                    aux=dict(
                        n_events_per_file=n_events_per_file,
                        filename_prefix="GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}".format(
                            higgs_mass=higgs_mass, step=cfg.get_aux("step")
                        ),
                    ),
                )
                cfg.add_dataset(d)
                i_ds += 50
            i_cfg += 1
            #for higgs_mass in range(600, 805, 5):
            #    n_files = 15
            #    n_events_per_file = 4000
            #    d = od.Dataset(
            #        name="ggh_htautau_mh{higgs_mass:d}_{step:s}".format(
            #            higgs_mass=higgs_mass, step=cfg.get_aux("step")
            #        ),
            #        id=i_ds + i_cfg,
            #        processes=[
            #            cfg.get_process("ggh_htautau_mh{higgs_mass:d}".format(higgs_mass=higgs_mass))
            #        ],
            #        keys=[
            #            "/{step:s}/GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}_{file_index:d}.root".format(
            #                step=cfg.get_aux("step"), higgs_mass=higgs_mass, file_index=i
            #            )
            #            for i in range(n_files)
            #        ],
            #        n_files=n_files,
            #        n_events=n_events_per_file * n_files,
            #        aux=dict(
            #            n_events_per_file=n_events_per_file,
            #            filename_prefix="GluGluHToTauTau_MH{higgs_mass:d}_pythia8_TuneCP5_{step:s}".format(
            #                higgs_mass=higgs_mass, step=cfg.get_aux("step")
            #            ),
            #        ),
            #    )
            #    cfg.add_dataset(d)
            #    i_ds += 50
            #i_cfg += 1


        return ggh_htautau_production
