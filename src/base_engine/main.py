#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

# Copyright 2024, University of Freiburg,
# Chair of Algorithms and Data Structures
# Author: Tanmay Garg

from __future__ import annotations

import sys
import traceback
from pathlib import Path

from termcolor import colored

from base_engine.command import CommandObjects
from base_engine.config import ArgumentsManager
from qlever.config import ConfigException
from qlever.log import log, log_levels


def main():
    script_name = Path(sys.argv[0]).stem
    command_objects = CommandObjects(script_name=script_name)

    # Parse the command line arguments and read the Configfile
    try:
        engine_config = ArgumentsManager(
            script_name=script_name, command_objects=command_objects
        )
        args = engine_config.parse_args()
    except ConfigException as e:
        log.error(e)
        log.info("")
        log.info(traceback.format_exc())
        exit(1)

    # Execute the command.
    command_object = command_objects[args.command]
    log.setLevel(log_levels[args.log_level])
    try:
        log.info("")
        log.info(colored(f"Command: {args.command}", attrs=["bold"]))
        log.info("")
        command_successful = command_object.execute(args)
        log.info("")
        if not command_successful:
            exit(1)
    except KeyboardInterrupt:
        log.info("")
        log.info("Ctrl-C pressed, exiting ...")
        log.info("")
        exit(1)
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
        log.info("")
        log.info(traceback.format_exc())
        exit(1)
