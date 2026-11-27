"""Host-only execution identities; authority lives in the World SQLite DB."""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ExecutionPermit:
    world_id: str
    person_id: str
    owner_token: str = field(repr=False)
    epoch: int


# Diagnostic loaded generation only; no permit fields or APIs change.
from .source_attestation import register as _register_source
_SOURCE_GENERATION = _register_source(globals())
