from __future__ import annotations

import enum
from typing import TYPE_CHECKING

from typing_extensions import override

if TYPE_CHECKING:
    from typing import Optional


"""
This fixture is used to determine which version of Postgres to use for tests.
"""


# Inherit PgVersion from str rather than int to make it easier to pass as a command-line argument
# TODO: use enum.StrEnum for Python >= 3.11
class PgVersion(str, enum.Enum):
    V14 = "14"
    V15 = "15"
    V16 = "16"
    V17 = "17"

    # Default Postgres Version for tests that don't really depend on Postgres itself
    DEFAULT = V16

    # Instead of making version an optional parameter in methods, we can use this fake entry
    # to explicitly rely on the default server version (could be different from pg_version fixture value)
    NOT_SET = "<-POSTRGRES VERSION IS NOT SET->"

    # Make it less confusing in logs
    @override
    def __repr__(self) -> str:
        return f"'{self.value}'"

    # Make this explicit for Python 3.11 compatibility, which changes the behavior of enums
    @override
    def __str__(self) -> str:
        return self.value

    # In GitHub workflows we use Postgres version with v-prefix (e.g. v14 instead of just 14),
    # sometime we need to do so in tests.
    @property
    def v_prefixed(self) -> str:
        return f"v{self.value}"

    @classmethod
    @override
    def _missing_(cls, value: object) -> Optional[PgVersion]:
        known_values = {v.value for _, v in cls.__members__.items()}

        # Allow passing version as a string with "v" prefix (e.g. "v14")
        if isinstance(value, str) and value.lower().startswith("v") and value[1:] in known_values:
            return cls(value[1:])
        # Allow passing version as an int (e.g. 15 or 150002, both will be converted to PgVersion.V15)
        elif isinstance(value, int) and str(value)[:2] in known_values:
            return cls(str(value)[:2])

        # Make mypy happy
        # See https://github.com/python/mypy/issues/3974
        return None
