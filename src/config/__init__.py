from dataclasses import dataclass
from typing import Any, Dict, List

import yaml

from src.config.base import BaseConfig as _BaseConfig
from src.config.db import Database as _Database


@dataclass
class Config(_BaseConfig):

    @dataclass
    class FileFormat(_BaseConfig):
        prefix: str
        datetime: str

    @dataclass
    class PruningStrategy(_BaseConfig):
        keep_daily: int
        keep_weekly: int
        keep_monthly: int
        keep_yearly: int

    rclone_remote: str
    file_format: FileFormat
    dirs: List[str]
    pruning: PruningStrategy
    databases: List[_Database]
    logdir: str

    @classmethod
    def _load_data(cls, config_path="config.yaml") -> Dict[str, Any]:
        """
        Load configuration data from a file path.
        """
        try:
            with open(config_path, "r") as config_file:
                return dict(yaml.safe_load(config_file))
        except FileNotFoundError as e:
            raise cls.ConfigLoadError(f'Config file "{config_path}" could not be found.') from e
        except Exception as e:
            raise cls.ConfigLoadError(
                "Something went wrong while loading the config into memory. This is probably a YAML parsing error."
            ) from e

    @classmethod
    def build(cls, config_path="config.yaml") -> "Config":
        try:
            return Config(
                rclone_remote=data["rclone"]["remote"],
                file_format=Config.FileFormat(
                    prefix=data["format"]["prefix"],
                    datetime=data["format"]["datetime"],
                ),
                dirs=data["dirs"],
                pruning=Config.PruningStrategy(
                    keep_daily=data["pruning"]["keep_daily"],
                    keep_weekly=data["pruning"]["keep_weekly"],
                    keep_monthly=data["pruning"]["keep_monthly"],
                    keep_yearly=data["pruning"]["keep_yearly"],
                ),
                databases=[
                    *[
                        Config.Database(
                            provider=Config.Database.Provider.POSTGRES,
                            name=db["name"],
                            host=db["host"],
                            port=db["port"],
                            username=db["username"],
                            password=db["password"],
                        )
                        for db in (data["databases"]["postgres"] if data["databases"]["postgres"] else [])
                    ],
                    *[
                        # Database name is just the sqlite file path here, and everything else is left blank.
                        Config.Database(
                            provider=Config.Database.Provider.SQLITE,
                            name=db["path"],
                            host="",
                            port="",
                            username="",
                            password="",
                        )
                        for db in (data["databases"]["sqlite"] if data["databases"]["sqlite"] else [])
                    ],
                ],
                logdir=data["logs"]["dir"],
            )

        except (KeyError, ValueError) as e:
            raise Config.InvalidConfig from e
