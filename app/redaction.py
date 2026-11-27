"""Small, deterministic redaction boundary for provider-facing diagnostics.

Only fixed machine codes may cross the provider/host boundary. Payloads that
leave the settings or usage surfaces are copied recursively and never expose
credential values or opaque credential references.
"""
import re

_SAFE_CODE = re.compile(r'[A-Z][A-Z0-9_]{0,79}\Z')
_DEFAULT_CODE = 'PROVIDER_ERROR'
_KNOWN_PREFIXES = (
    'ACTIVATION_', 'ACTOR_', 'ADJUDICATION_', 'AUTH_', 'COORDINATOR_',
    'CREDENTIAL_', 'DM_', 'EXECUTION_', 'HOST_', 'HTTP_', 'INVALID_',
    'MUTATION_', 'NODE_', 'OWNED_', 'PI_', 'PRODUCT_', 'PROVIDER_',
    'REAL_', 'RECEIPT_', 'SCHEDULER_', 'SETTINGS_', 'SIWC_', 'STORAGE_',
    'TECHNICAL_', 'WINDOW_', 'WORLD_',
)
_SENSITIVE_KEYS = frozenset({
    'access_token', 'api_key', 'apikey', 'authorization', 'client_secret',
    'code', 'code_verifier', 'cookie', 'credential', 'credential_blob',
    'credential_ref', 'id_token', 'password', 'pkce', 'private_key',
    'refresh_token', 'secret', 'secret_key', 'set-cookie', 'token',
})


def safe_code(value, fallback=_DEFAULT_CODE):
    """Return a fixed-format error code without preserving arbitrary text."""
    if (type(value) is str and _SAFE_CODE.fullmatch(value)
            and value.startswith(_KNOWN_PREFIXES)):
        return value
    if (type(fallback) is str and _SAFE_CODE.fullmatch(fallback)
            and fallback.startswith(_KNOWN_PREFIXES)):
        return fallback
    return _DEFAULT_CODE


def redact_text(value, secrets=()):
    """Replace exact secret strings, longest first, without logging inputs."""
    if type(value) is not str:
        return '<redacted>'
    result = value
    candidates = sorted({secret for secret in secrets
                         if type(secret) is str and secret},
                       key=len, reverse=True)
    for secret in candidates:
        result = result.replace(secret, '<redacted>')
    return result


def redact(value, secrets=(), *, depth=0, max_depth=8):
    """Return a bounded JSON-shaped copy safe for external diagnostics."""
    if depth > max_depth:
        return '<redacted>'
    if type(value) is dict:
        result = {}
        for key, item in value.items():
            normalized = key.lower().replace('-', '_') if type(key) is str else ''
            if normalized in _SENSITIVE_KEYS or normalized.endswith('_token'):
                result[key] = '<redacted>'
            else:
                result[key] = redact(item, secrets, depth=depth + 1,
                                     max_depth=max_depth)
        return result
    if type(value) is list:
        return [redact(item, secrets, depth=depth + 1, max_depth=max_depth)
                for item in value]
    if type(value) is tuple:
        return [redact(item, secrets, depth=depth + 1, max_depth=max_depth)
                for item in value]
    if type(value) is str:
        return redact_text(value, secrets)
    if value is None or type(value) in (bool, int, float):
        return value
    return '<redacted>'


def exception_code(error, fallback=_DEFAULT_CODE):
    """Expose only a safe fixed code; never use ``str(error)`` externally."""
    return safe_code(getattr(error, 'code', None), fallback)


__all__ = ['exception_code', 'redact', 'redact_text', 'safe_code']
