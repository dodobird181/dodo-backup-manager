import os
import re
from typing import Any, List


class BaseConfig:
    """
    Base config handles environment variable substution automatically for any string-typed attributes.
    """

    class ConfigError(Exception):
        """
        Something went wrong while trying to load or build the config.
        """

        ...

    class ConfigLoadError(ConfigError):
        """
        There was a problem loading the config into memory from disk.
        """

        ...

    class ConfigBuildError(ConfigError):
        """
        There was a problem building the config using the available data in memory.
        """

        ...

    def _expand_env_var(self, path: str) -> str:
        """
        Expand environment variables in the path and raise `MissingEnvVar` if any
        variable is missing from the environment.
        """
        if not bool(re.search(r"\$\{[^}]+\}", path)):
            # no varnames are in the path
            return path
        expanded = os.path.expandvars(path)
        if expanded == path:
            # expansion didn't happen but there is an env var pattern. This means the var is not set.
            raise self.ConfigBuildError(
                f"One or more variable(s) in the string '{path}' is missing from the environment but required in the config."
            )
        return expanded

    def __setattr__(self, key: str, value: Any) -> None:
        """
        Attempt to inject environment variables defined by the syntax '${VAR_NAME}'
        when string attributes are set.
        """
        if isinstance(value, str):
            expanded = self._expand_env_var(value)
            super().__setattr__(key, expanded)
        elif isinstance(value, List):
            expanded = [self._expand_env_var(x) if isinstance(x, str) else x for x in value]
            super().__setattr__(key, expanded)
        else:
            # normal setattr for all other datatypes
            super().__setattr__(key, value)
