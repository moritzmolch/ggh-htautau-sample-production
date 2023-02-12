# ggh-htautau-sample-production

Production of gg &#x2192; H &#x2192; &#x1D70F;&#x1D70F; samples with variable Higgs mass.

The events are generated with `Pythia8` in leading order and interfaced with `FastSIM` for a fast simulation of the CMS detector. The workflow is managed with [law](https://github.com/riga/law).


## Setup

The software environment is set up with the `setup.sh` script in the root directory of this project. Executing

```bash
source setup.sh
```

installs missing software requirements and sets the shell environment for the production workflow.


## Fragment

The root object of the Monte Carlo production chain is the _fragment_ which contains information about the physics process at generator level. For the generation of gg &#x2192; H &#x2192; &#x1D70F;&#x1D70F; events with `Pythia8` a modification of the fragment `GGToHtautau_13TeV_pythia8_cff.py` from CMSSW
is used. The original fragment is available in the [cms-sw/cmssw GitHub repository](https://github.com/cms-sw/cmssw/blob/CMSSW_10_6_X/Configuration/Generator/python/GGToHtautau_13TeV_pythia8_cff.py).

Some amendmends to the original fragment have been made:

- The tune `CP5` is used instead of `CUEP8M1`.

- `onIfMatch` is used instead of `onIfAny`. This explicitly requires a Higgs boson decay into a pair of opposite-sign &#x1D70F; leptons

- the threshold mass `mMin` of the Higgs boson is reduced from 50 GeV to 25 GeV in order to account the production of samples with Higgs masses around that threshold.

The most important part of the fragment is the `processParameters` section:
```python
[...]
processParameters = cms.vstring(
    'HiggsSM:gg2H = on',
    '25:onMode = off',
    '25:onIfMatch = 15 -15',
    '25:m0 = 125.0',
    '25:mMin = 25.0',
),
[...]
```
This set of parameters defines the hard process for event generation. Higgs bosons that are produced via gluon-gluon fusion exclusively decay into pairs of &#x1D70F; leptons. The parameter `25:m0` describes the maximum of Breit-Wigner curve of the mass spectrum. Especially for higher masses `25:m0` must not be identified with the actual mass of the generated Higgs boson as the decay width of the Higgs boson increases towards higher values of `25:m0`.

The fragments for all processes can be generated with the command:

```bash
law run FragmentGeneration
```

The generated files are placed in the `src/Configuration/GenProduction/python` directory of the CMSSW installation that is set up during the execution of the `setup.sh` script. Before setting up the configuration for the production the CMSSW release has to be compiled. The compilation can be triggered with:

```bash
law run CompileCMSSW
```

## AODSIM step

Configuration files for event production in CMSSW are created with the `cmsDriver.py` tool. 

When performing the detector simulation with `FastSIM` separate steps like event generation, detector simulation, pileup premixing and reconstruction can be summarized in one production step. The following command creates a configuration file for the production of a `AODSIM` file:

```bash
cmsDriver.py Configuration/GenProduction/python/GluGluHToTauTau_MH50_pythia8_TuneCP5_cff.py \
             --python_filename GluGluHToTauTau_MH50_pythia8_TuneCP5_aodsim_0_cfg.py \
             --fileout file:GluGluHToTauTau_MH50_pythia8_TuneCP5_aodsim_0.root \
             --customise Configuration/DataProcessing/Utils.addMonitoring \
             --customise_commands from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;randSvc = RandomNumberServiceHelper(process.RandomNumberGeneratorService);randSvc.populate() \
             --era Run2_2018_FastSim \
             --conditions 106X_upgrade2018_realistic_v16_L1v1 \
             --beamspot Realistic25ns13TeVEarly2018Collision \
             --procModifiers premix_stage2 \
             --step GEN,SIM,RECOBEFMIX,DIGI,DATAMIX,L1,DIGI2RAW,L1Reco,RECO \
             --eventcontent AODSIM \
             --datatier AODSIM \
             --datamix PreMix \
             --pileup_input dbs:/Neutrino_E-10_gun/RunIIFall17FSPrePremix-PUFSUL18CP5_106X_upgrade2018_realistic_v16-v1/PREMIX \
             --fast \
             --no_exec \
             --mc \
             --number 2000
```

The most important arguments are:

 - `Configuration/GenProduction/python/GluGluHToTauTau_MH50_pythia8_TuneCP5_cff.py` - path to the fragment relative to the CMSSW root directory that contains information about the event generation

 - `--python_filename GluGluHToTauTau_MH50_pythia8_TuneCP5_aodsim_0_cfg.py` - name of the output configuration file

 - `--fileout file:GluGluHToTauTau_MH50_pythia8_TuneCP5_aodsim_0.root` - name of the output file of the production

 - `--customise_commands from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper;randSvc = RandomNumberServiceHelper(process.RandomNumberGeneratorService);randSvc.populate()` - providing a random seed for random number generators which is especially important when the production of the Monte Carlo dataset is splitted

 - `--era Run2_2018_FastSim`, `--conditions 106X_upgrade2018_realistic_v16_L1v1`, `--beamspot Realistic25ns13TeVEarly2018Collision` - define the environment of the production regarding the detector setup, alignment and calibration conditions as well as the beam setup
 
 - `--step GEN,SIM,RECOBEFMIX,DIGI,DATAMIX,L1,DIGI2RAW,L1Reco,RECO` - production steps; the chain that is defined here can be understood as follows: event generation &#x2192; detector simulation &#x2192; reconstruction &#x2192; digitization of detector signals &#x2192; premixing of the pileup input &#x2192; L1 trigger simulation &#x2192; conversion of digitized signals into raw data &#x2192; L1 trigger reconstruction &#x2192; reconstruction

 - `--eventcontent AODSIM` - specification of the event format in the output file
 
 - `--datamix PreMix`, `--pileup_input dbs:/Neutrino_E-10_gun/RunIIFall17FSPrePremix-PUFSUL18CP5_106X_upgrade2018_realistic_v16-v1/PREMIX` - pileup dataset for premixing
 
 - `--fast` - perform a fast simulation of the detector

 - `--mc` - produce a Monte Carlo dataset

 - `--number 2000` - number of produced events
  
The workflow creates a configuration template for each value the Higgs mass that is defined in the configuration. The production of the dataset is split in order to speed up the production by enabling parallel processing of the events. Therefore a configuration for the production of each file is generated from the configuration templates. 

When the generation of configuration files has been finished the production can be started. The CMSSW command for producing the file configured with the `cmsDriver.py` tool above would be:

```bash
cmsRun GluGluHToTauTau_MH50_pythia8_TuneCP5_aodsim_0_cfg.py
```

In context of the workflow for this production all of the tasks described above can be triggered with a single command:

```bash
law run AODSIMProduction
```

Configuration files for the production are generated on demand if they do not exist yet. After having ensured that all requirements are fulfilled jobs for the production of `AODSIM` files are submitted to the ETP HTCondor batch system.

It might be appropriate to not submit all jobs at once. The set of submitted jobs can be reduced by either producing files for a selected range of branches,

```bash
law run AODSIMProduction --branches 0:400
```

or by explicitly setting the allowed number of parallel jobs:

```bash
law run AODSIMProduction --parallel-jobs 400
```
