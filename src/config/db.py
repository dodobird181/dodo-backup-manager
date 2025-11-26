"""
Handle database communication.
"""

import os
from dataclasses import dataclass
from enum import Enum

from src.config.base import BaseConfig


@dataclass
class Database(BaseConfig):

    class UnsupportedProvider(Exception):
        def __init__(self, provider):
            super().__init__(f"The database provider '{provider}' is not yet supported.")

    class Provider(Enum):
        POSTGRES = "postgres"
        SQLITE = "sqlite"

    provider: Provider
    name: str
    host: str
    port: str
    username: str
    password: str

    def __repr__(self) -> str:
        return "Config.Database(provider='{}', name='{}', host='{}', username='{}', password='{}')".format(
            self.provider,
            self.name,
            self.host,
            self.username,
            "*****",  # password redacted
        )

    def __str__(self) -> str:
        if self.provider == Database.Provider.POSTGRES:
            return f"<{self.provider.name} Database - {self.name}@{self.host}:{self.port}>"
        elif self.provider == Database.Provider.SQLITE:
            return f"<{self.provider.name} Database - '{self.name}'>"
        raise Database.UnsupportedProvider(self.provider)

    # def conn_args(self) -> List[str]:
    #     """Connection arguments for the database."""
    #     if self.provider == Database.Provider.POSTGRES:
    #         return ["-h", self.host, "-p", self.port, "-U", self.username, "-d", self.name]
    #     elif self.provider == Database.Provider.SQLITE:
    #         return ["ls", "-lh", self.name]
    #     raise Database.UnsupportedProvider(self.provider)

    # def test_conn(self) -> bool:
    #     if self.provider == Database.Provider.POSTGRES:
    #         os.environ["PGPASSWORD"] = self.password  # Needed to avoid prompting for a password
    #         result = run("psql", *self.conn_args(), "-c", "\\q", check=False)
    #         if result.returncode == 0:
    #             return True
    #         return False
    #     elif self.provider == Database.Provider.SQLITE:
    #         result = run(*self.conn_args(), check=False)
    #         if result.returncode == 0:
    #             return True
    #         return False
    #     raise Database.UnsupportedProvider(self.provider)

    # def dump(self, path: str) -> subprocess.CompletedProcess:
    #     if self.provider == Database.Provider.POSTGRES:
    #         os.environ["PGPASSWORD"] = self.password  # Needed to avoid prompting for a password
    #         with open(f"{path}.sql", "w") as dump_file:
    #             return run("pg_dump", *self.conn_args(), stdout=dump_file, capture_output=False)
    #     elif self.provider == Database.Provider.SQLITE:
    #         return run("cp", self.name, f"{path}.db")
    #     raise Database.UnsupportedProvider(self.provider)
