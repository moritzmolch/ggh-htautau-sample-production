import law
import luigi
import os

law.contrib.load("tasks")


class CompileCROWN(law.SandboxTask, law.tasks.RunOnceTask):
    crown_path = os.path.expandvars("${PROD_CROWN_PATH}")

    crown_analysis = luigi.Parameter(default="tau")
    crown_config = luigi.Parameter(default="config_slimmed")
    crown_eras = law.CSVParameter(default="2018")
    crown_samples = law.CSVParameter(default="ggh_htautau")
    crown_scopes = law.CSVParameter(default="mt")
    crown_shifts = law.CSVParameter(default="none")
    crown_threads = luigi.IntParameter(default=1)
    crown_debug = luigi.BoolParameter(default=False)
    crown_optimized = luigi.BoolParameter(default=False)

    cleanup_build_dir = luigi.BoolParameter(default=False)

    sandbox = "bash::${PROD_BASE}/sandboxes/crown_tau_analysis.sh"

    def run(self):
        # delete old build files if cleaning up the directory is requested
        build_dir = law.LocalDirectoryTarget(os.path.join(self.crown_path, "build"))
        if self.cleanup_build_dir and build_dir.exists():
            build_dir.remove()

        # create the build dir if it doesn't exist
        if not build_dir.exists():
            build_dir.touch()

        # build the compilation command for CROWN and run it in the build directory
        cmake_kwargs = {
            "ANALYSIS": self.crown_analysis,
            "CONFIG": self.crown_config,
            "SAMPLES": ",".join(self.crown_samples),
            "ERAS": ",".join(self.crown_eras),
            "SCOPES": ",".join(self.crown_scopes),
            "SHIFTS": ",".join(self.crown_shifts),
            "THREADS": str(self.crown_threads),
            "DEBUG": "true" if self.crown_debug else "false",
            "OPTIMIZED": "true" if self.crown_optimized else "false",
        }
        cmd_comp = ["cmake", ".."]
        cmd_comp += ["-D{key:s}={value:s}".format(key=k, value=v) for k, v in cmake_kwargs.items()]
        cmd_comp = law.util.quote_cmd(cmd_comp)
        self.publish_message("run CROWN compilation with command '{cmd:s}'".format(cmd=cmd_comp))
        p, lines = law.util.readable_popen(
            cmd_comp,
            cwd=build_dir.path,
            env=self.env,
            shell=True,
            executable="/bin/bash",
        )
        for line in lines:
            self.publish_message(line)
            if p.poll() is not None:
                break
        if p.returncode != 0:
            raise RuntimeError("CROWN compilation failed")

        # install the compiled code
        cmd_inst = law.util.quote_cmd(["make", "install", "-j", self.crown_threads])
        self.publish_message("run CROWN installation with command '{cmd:s}'".format(cmd=cmd_inst))
        p, lines = law.util.readable_popen(
            cmd_inst,
            cwd=build_dir.path,
            env=self.env,
            shell=True,
            executable="/bin/bash",
        )
        for line in lines:
            self.publish_message(line)
            if p.poll() is not None:
                break
        if p.returncode != 0:
            raise RuntimeError("CROWN installation failed")

