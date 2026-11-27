"""Explicit provider routing seam for real services and the offline Pi worker.

A real router has independent role and DM adapter slots and never falls back to
Pi when either slot is absent. The scripted router is a separate mode, so tests
can prove that an unconfigured real route performs no provider call.
"""
from typing import Protocol

from .real_provider import ProviderFault

ROLE_ROUTE = 'roles'
DM_ROUTE = 'dm'
REAL_MODE = 'real'
SCRIPTED_PI_MODE = 'scripted-pi'


class ProviderAdapter(Protocol):
    name: str

    def role(self, request, attempt=None): ...

    def dm(self, payload, attempt=None, expected_version=None): ...

    def close(self): ...


class RealProviderAdapter:
    """Adapter around the already-bounded RealGateway."""
    name = 'real-gateway'

    def __init__(self, gateway):
        if gateway is None or not all(callable(getattr(gateway, name, None))
                                      for name in ('role', 'dm')):
            raise ValueError('INVALID_REAL_PROVIDER_ADAPTER')
        self.gateway = gateway

    def role(self, request, attempt=None):
        return self.gateway.role(request, attempt)

    def dm(self, payload, attempt=None, expected_version=None):
        return self.gateway.dm(payload, attempt, expected_version)

    def close(self):
        closer = getattr(self.gateway, 'close', None)
        if closer is not None:
            closer()


class PiProviderAdapter:
    """Offline Pi seam; the injected callable is the only provider boundary."""
    name = 'scripted-pi'

    def __init__(self, role_call, *, dm_call=None, close_call=None):
        if not callable(role_call) or (dm_call is not None and not callable(dm_call)):
            raise ValueError('INVALID_PI_PROVIDER_ADAPTER')
        self._role_call = role_call
        self._dm_call = dm_call
        self._close_call = close_call

    def role(self, request, attempt=None):
        return self._role_call(request, attempt)

    def dm(self, payload, attempt=None, expected_version=None):
        if self._dm_call is None:
            raise ProviderFault('PI_DM_UNAVAILABLE')
        return self._dm_call(payload, attempt, expected_version)

    def close(self):
        if self._close_call is not None:
            self._close_call()


class ProviderRouter:
    """Route role and DM requests without implicit provider substitution."""
    def __init__(self, mode, *, roles=None, dm=None, pi=None):
        if mode not in (REAL_MODE, SCRIPTED_PI_MODE):
            raise ValueError('INVALID_PROVIDER_MODE')
        if mode == REAL_MODE and pi is not None:
            raise ValueError('REAL_PI_FALLBACK_FORBIDDEN')
        if mode == SCRIPTED_PI_MODE and (roles is not None or dm is not None):
            raise ValueError('SCRIPTED_REAL_ROUTE_FORBIDDEN')
        if mode == SCRIPTED_PI_MODE and pi is None:
            raise ValueError('PI_PROVIDER_REQUIRED')
        self.mode = mode
        self.roles = roles
        self.dm_adapter = dm
        self.pi = pi

    @property
    def real(self):
        return self.mode == REAL_MODE

    def _missing(self, kind):
        raise ProviderFault('REAL_UNCONFIGURED' if self.real
                            else 'PI_PROVIDER_UNAVAILABLE')

    def _route(self, kind):
        if kind == ROLE_ROUTE:
            adapter = self.roles if self.real else self.pi
        elif kind == DM_ROUTE:
            adapter = self.dm_adapter if self.real else self.pi
        else:
            raise ProviderFault('INVALID_PROVIDER_ROUTE')
        if adapter is None:
            self._missing(kind)
        return adapter

    def role(self, request, attempt=None):
        return self._route(ROLE_ROUTE).role(request, attempt)

    def dm(self, payload, attempt=None, expected_version=None):
        return self._route(DM_ROUTE).dm(payload, attempt, expected_version)

    def snapshot(self):
        def name(adapter):
            value = getattr(adapter, 'name', None)
            return value if type(value) is str and value else 'custom'
        if self.real:
            return {'mode': REAL_MODE,
                    ROLE_ROUTE: name(self.roles) if self.roles is not None else None,
                    DM_ROUTE: name(self.dm_adapter) if self.dm_adapter is not None else None}
        return {'mode': SCRIPTED_PI_MODE, ROLE_ROUTE: name(self.pi), DM_ROUTE: name(self.pi)}

    def close(self):
        seen = set()
        for adapter in (self.roles, self.dm_adapter, self.pi):
            if adapter is None or id(adapter) in seen:
                continue
            seen.add(id(adapter))
            closer = getattr(adapter, 'close', None)
            if closer is not None:
                closer()


__all__ = ['DM_ROUTE', 'ProviderAdapter', 'ProviderRouter', 'PiProviderAdapter',
           'RealProviderAdapter', 'REAL_MODE', 'ROLE_ROUTE', 'SCRIPTED_PI_MODE']
