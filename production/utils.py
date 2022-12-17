class HTTDataset:
    def __init__(self, higgs_mass, higgs_width, number_of_events, branch=None):
        self.higgs_mass = higgs_mass
        self.higgs_width = higgs_width
        self.has_modified_higgs_width = higgs_width != -1
        self.number_of_events = number_of_events
        self.branch = branch
        higgs_mass_text = (
            str(int(higgs_mass))
            if int(higgs_mass) == higgs_mass
            else str(higgs_mass).replace(".", "p")
        )
        self.basename = "GluGluHToTauTau_MH{higgs_mass:s}_pythia8_TuneCP5".format(
            higgs_mass=higgs_mass_text
        )
        self.files = self._create_file_splitting()

    def _create_file_splitting(self):
        files = []
        index = 0
        i_start, i_stop = 0, 0
        while i_start < self.number_of_events:
            if i_start < 10000:
                i_stop = i_start + 2000
            else:
                i_stop = i_start + 4000
            f = HTTFile(self, index, i_stop - i_start)
            files.append(f)
            i_start = i_stop
            index += 1
        return files

    def get_fragment_filename(self):
        return "{basename:s}_cff.py".format(basename=self.basename)

    def get_step_config_filename(self, step):
        return "{basename:s}_{step:s}_cfg.py".format(basename=self.basename, step=step)

    def get_step_root_filename(self, step):
        return "{basename:s}_{step:s}.root".format(basename=self.basename, step=step)


class HTTFile:
    def __init__(self, dataset, index, number_of_events, branch=None):
        self.dataset = dataset
        self.index = index
        self.number_of_events = number_of_events
        self.branch = branch

    def get_step_config_filename(self, step):
        return "{basename:s}_{step:s}_{index:d}_cfg.py".format(
            basename=self.dataset.basename, step=step, index=self.index
        )

    def get_step_root_filename(self, step):
        return "{basename:s}_{step:s}_{index:d}.root".format(
            basename=self.dataset.basename, step=step, index=self.index
        )
