from __future__ import annotations

import subprocess
from pathlib import Path

import qlever.commands.setup_config as QleverCmd
from qlever.log import log
from qlever.util import get_random_string


class SetupConfigCommand(QleverCmd.SetupConfigCommand):
    """
    Class for executing the `setup-config` command.
    """

    def __init__(self):
        self.qleverfiles_path = (
            Path(__file__).parent.parent.parent / "qlever" / "Qleverfiles"
        )
        self.qleverfile_names = [
            p.name.split(".")[1]
            for p in self.qleverfiles_path.glob("Qleverfile.*")
        ]

    def execute(self, args) -> bool:
        # Construct the command line and show it.
        qleverfile_path = (
            self.qleverfiles_path / f"Qleverfile.{args.config_name}"
        )
        setup_config_cmd = (
            f"cat {qleverfile_path}"
            f" | sed -E 's/(^ACCESS_TOKEN.*)/\\1_{get_random_string(12)}/'"
        )
        setup_config_cmd += "> Qleverfile"
        self.show(setup_config_cmd, only_show=args.show)
        if args.show:
            return True

        # If there is already a Qleverfile in the current directory, exit.
        qleverfile_path = Path("Qleverfile")
        if qleverfile_path.exists():
            log.error("`Qleverfile` already exists in current directory")
            log.info("")
            log.info(
                "If you want to create a new Qleverfile using "
                "`qlever setup-config`, delete the existing Qleverfile "
                "first"
            )
            return False

        # Copy the Qleverfile to the current directory.
        try:
            subprocess.run(
                setup_config_cmd,
                shell=True,
                check=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
        except Exception as e:
            log.error(
                f'Could not copy "{qleverfile_path}" to current directory: {e}'
            )
            return False

        # If we get here, everything went well.
        log.info(
            f'Created Qleverfile for config "{args.config_name}"'
            f" in current directory"
        )
        return True
