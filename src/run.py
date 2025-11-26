#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
from logging.handlers import TimedRotatingFileHandler
from typing import Any, List
from uuid import uuid4

from src.get_backups_to_prune import should_prune

Arguments = namedtuple(
    "ArgNamespace",
    ["live", "log_level", "disable_pruning", "ignore_missing_dirs"],
)


def get_arguments() -> Arguments:
    """Parse script arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--live",
        default=False,
        action="store_true",
        help="Send API calls to rclone when the live flag is true. Otherwise this program will only run locally and output logs.",
    )
    parser.add_argument(
        "--disable-pruning",
        default=False,
        action="store_true",
        help="Do not prune old backups.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        type=str,
        help="The console log level. One of: [DEBUG, INFO, WARNING, ERROR]. ",
    )
    parser.add_argument(
        "-i",
        default=False,
        action="store_true",
        help="Ignore missing dirs and proceed with the backup without prompting. This flag is ignored when service mode is enabled.",
    )
    args = parser.parse_args()
    setattr(args, "ignore_missing_dirs", args.i)
    return args  # type: ignore


@lru_cache(maxsize=1)
def parent_path() -> str:
    script_path = os.path.abspath(__file__)
    return os.path.dirname(script_path)


@lru_cache(maxsize=1)
def parent_dir() -> str:
    script_path = os.path.abspath(__file__)
    parent_path = os.path.dirname(script_path)
    return os.path.basename(parent_path)


def config_path() -> str:
    return f"{parent_dir()}/config.yaml"


def now_str() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z%z")


def format_seconds(seconds: int) -> str:
    td = timedelta(seconds=seconds)
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if days != 0:
        return f"{days}d {hours}h {minutes}m"
    if hours != 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def wait_for_confirm(message: str) -> bool:
    while True:
        response = input(f"{now_str()} {message} (y/n)?")
        if response.lower().strip() == "y":
            return True
        elif response.lower().strip() == "n":
            return False
        else:
            print("Enter (y/Y) to confirm or (n/N) to cancel.")


def run(*args, check=True, capture_output=True, text=True, **kwargs) -> subprocess.CompletedProcess:
    """Run a command with some default arguments."""
    return subprocess.run(args, text=text, check=check, capture_output=capture_output, **kwargs)


def refresh_current_backups_from_rclone(config: Config):
    with open(f"{parent_path()}/current_backups.txt", "w") as f:
        subprocess.run(["rclone", "lsf", config.rclone_remote], stdout=f)


class BackupRunner:

    class DirNotFound(Exception):
        """
        The backup directory was not found.
        """

        def __init__(self, dirname: str):
            self.dirname = dirname
            super().__init__(
                "The directory '{}' was not found. Paths checked: [{}, {}]".format(
                    dirname,
                    dirname,
                    f"{os.getcwd()}/{dirname}",
                )
            )

    class SkipDir(Exception):
        """
        The backup directory was not found and will be skipped.
        """

        def __init__(self, dirname: str):
            self.dirname = dirname
            super().__init__(f"Skipping the directory: {dirname}.")

    def __init__(self, config: Config):
        self.config = config
        self.logger = self._logger_from_config(config)

    def _logger_from_config(self, config: Config) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(get_arguments().log_level)
        run("mkdir", "-p", f"{parent_dir()}/{config.logdir}")
        file_handler = TimedRotatingFileHandler(
            f"{parent_dir()}/{config.logdir}/log.txt",
            when="midnight",  # rotate once per day at midnight
            interval=30,  # ~monthly rotation
            backupCount=12,
            utc=True,
        )
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s]: %(message)s",
            datefmt="%Y-%m-%d_%I-%M-%S_%p_%Z%z",
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        return logger

    def _parse_dirname(self, dirname: str) -> str:
        """
        Make sure the directory name exists on the machine. This method tries
        the following in order: 1.) absolute path, and 2.) relative path from the
        parent directory to where `run.py` is located, i.e., the current working
        directory when running the script. Will raise an exception if the directory
        doesn't exist and otherwise return the guaranteed path to the directory.
        """
        if os.path.exists(dirname):
            return dirname
        elif os.path.exists(f"{os.getcwd()}/{dirname}"):
            return f"{os.getcwd()}/{dirname}"
        elif get_arguments().ignore_missing_dirs == True or self.config.service_mode.enabled == True:
            # Ignore missing directories if "-i" flag passed or we are running in service mode (since a daemon
            # shouldn't prompt the CLI for input to make a decision.)
            raise self.SkipDir(dirname)
        raise self.DirNotFound(dirname)

    def run(self) -> None:
        LIVE = bool(get_arguments().live)
        ZIP_DIR = f"{uuid4().hex}_tmp_backup_manager_workspace"

        self.logger.info("Starting backup...")

        # Log the config data in DEBUG
        data = {
            "remote": config.rclone_remote,
            "directories": config.dirs,
            "databases": [str(d) for d in config.databases],
            "pruning": "Keep {} daily, {} weekly, {} monthly, and {} yearly backups.".format(
                config.pruning.keep_daily,
                config.pruning.keep_weekly,
                config.pruning.keep_monthly,
                config.pruning.keep_yearly,
            ),
        }
        self.logger.debug(f"Loaded config: {json.dumps(data, indent=2)}")

        # warn if directory to backup is missing
        for dirname in self.config.dirs:
            try:
                parsed_dirname = self._parse_dirname(dirname)
            except self.DirNotFound:
                backup = wait_for_confirm(
                    f"[WARNING]: Could not find directory '{dirname}'. Do you wish to proceed"
                )
                if backup == False:
                    self.logger.info("Backup cancelled.")
                    exit(0)
            except self.SkipDir:
                continue

        # Check database connections exist
        for db in config.databases:
            if db.test_conn() == False:
                self.logger.error(f"Backup failed. Could not connect to database: {db}.")
                exit(1)

        # Backup the databases
        BACKUP_WORKSPACE = f"{parent_path()}/{ZIP_DIR}"
        BACKUP_TIMESTAMP = datetime.now().astimezone().strftime(config.file_format.datetime)
        run("mkdir", BACKUP_WORKSPACE)
        self.logger.info(f"Created temporary workspace: '{BACKUP_WORKSPACE}'.")
        for i, db in enumerate(sorted(config.databases, key=lambda db: db.name)):
            # use index to make sure names are unique and throw in db name for identification
            self.logger.info(f"Dumping database {db}...")
            dump_path = f"{BACKUP_WORKSPACE}/{BACKUP_TIMESTAMP}_{db.name.replace('/', '_').replace('.', '_')}_{i}"
            db.dump(dump_path)

        self.logger.info(f"Copying backup directories {config.dirs}...")
        for dirname in config.dirs:
            try:
                parsed_dirname = self._parse_dirname(dirname)
                self.logger.debug(f"Copying {dirname}...")
                run("mkdir", "-p", f"{BACKUP_WORKSPACE}/{dirname}")
                run("cp", "-r", parsed_dirname, f"{BACKUP_WORKSPACE}/{dirname}", capture_output=False)
            except (self.SkipDir, self.DirNotFound):
                self.logger.warning(f"Skipping dir '{dirname}' because it was not found.")

        prefix = config.file_format.prefix
        prefix = "" if prefix.strip() == "" else f"{prefix}_"
        COMPRESSED_BACKUP_PATH = f"{parent_dir()}/{prefix}{BACKUP_TIMESTAMP}.zip"
        self.logger.info(f"Compressing files to {COMPRESSED_BACKUP_PATH}...")
        # zip -r - folder/ | pv -s $(du -sb folder/ | awk '{print $1}')
        du_result = run("du", "-sb", f"{parent_dir()}/{ZIP_DIR}").stdout
        num_bytes = int(du_result.split()[0])
        zip_proc = subprocess.Popen(
            # "-" = write zip to stdout
            ["zip", "-rq", "-", f"{parent_dir()}/{ZIP_DIR}"],
            stdout=subprocess.PIPE,
        )
        with open(COMPRESSED_BACKUP_PATH, "wb") as out_file:
            # piped output to progress viewer to see progress bar
            pv_proc = subprocess.Popen(["pv", "-s", str(num_bytes)], stdin=zip_proc.stdout, stdout=out_file)
            if zip_proc.stdout:
                # let zip_proc get SIGPIPE if pv exits
                zip_proc.stdout.close()
            pv_proc.communicate()
        run("rm", "-rf", BACKUP_WORKSPACE, capture_output=False)
        if LIVE:
            self.logger.info(f"Uploading backup to rclone remote: '{config.rclone_remote}'.")
            run("rclone", "copy", COMPRESSED_BACKUP_PATH, config.rclone_remote, capture_output=False)
        else:
            self.logger.info("Skipping upload to rclone because the '--live' flag is false.")
        run("rm", "-rf", COMPRESSED_BACKUP_PATH)

        if get_arguments().disable_pruning == True:
            self.logger.info("Skipping pruning because '--disable-pruning' was passed as an argument.")
        else:
            PRUNE_FILE = f"{parent_path()}/to_prune.txt"
            if LIVE:
                self.logger.info("Pruning old backup files...")
                refresh_current_backups_from_rclone(config)

                # Find backups to prune
                with open(f"{parent_path()}/current_backups.txt", "r") as f:
                    filenames = [line.strip() for line in f if line.strip()]
                to_prune = should_prune(
                    filenames=filenames,
                    file_format=f"{prefix}{config.file_format.datetime}.zip",
                    keep_daily=config.pruning.keep_daily,
                    keep_weekly=config.pruning.keep_weekly,
                    keep_monthly=config.pruning.keep_monthly,
                    keep_yearly=config.pruning.keep_yearly,
                )
                with open(PRUNE_FILE, "w") as f:
                    for fn in to_prune:
                        f.write(fn + "\n")
                self.logger.debug(f"Prune list written to {PRUNE_FILE}. {len(to_prune)} files to prune.")

                # Prune backups
                with open(PRUNE_FILE, "r") as prune_file:
                    for backup_filename in prune_file.readlines():
                        backup_filename = backup_filename.replace("\n", "")
                        self.logger.info(f"Pruning {backup_filename}...")
                        run("rclone", "delete", f"{config.rclone_remote}/{backup_filename}", capture_output=False)
            else:
                self.logger.info("Skipping pruning because the '--live' flag is false.")
            run("rm", "-rf", PRUNE_FILE)

        # refresh again to update the backups list after backup is complete
        if LIVE:
            refresh_current_backups_from_rclone(config)
        else:
            self.logger.info("Skipping refresh because the '--live' flag is false.")

        self.logger.info("Done!")


if __name__ == "__main__":

    if os.getcwd() == parent_path():
        raise Config.InvalidConfig(
            "Backup failed. Please make sure you're running the program from your project's root directory."
        )

    config = load_configuration()
    runner = BackupRunner(config)
    runner.logger.info(config.service_mode)

    if config.service_mode.enabled == True:
        systemd.daemon.notify("READY=1")  # daemon is ready after config is loaded
        # Run periodically
        last_ran_at = datetime.now().astimezone() - timedelta(weeks=(52 * 20))  # 20 years back.. arbitrary
        while True:
            next_run_in = config.service_mode.next_run_in(last_ran_at)
            if next_run_in < 0:
                try:
                    runner.run()
                    last_ran_at = datetime.now().astimezone()
                    next_run_in = config.service_mode.next_run_in(last_ran_at)
                    runner.logger.info(f"Next run in {format_seconds(int(next_run_in))}. Going to sleep...")
                except Exception as e:
                    runner.logger.error("Backup failed.", exc_info=e)
            time.sleep(30)  # check against service mode schedule around every 30 seconds
    else:
        # Run once...
        try:
            runner.run()
        except Exception as e:
            runner.logger.error("Backup failed.", exc_info=e)
