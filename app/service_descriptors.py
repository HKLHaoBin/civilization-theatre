"""Private metadata preparation only. No provider, credentials, storage or network."""
import copy
import re
from urllib.parse import urlsplit

def validate(config):
    fields={'provider','base_url','model','credential_ref','timeout_seconds','max_retries'}
    if type(config) is not dict or set(config)!={'roles','dm'}:
        raise ValueError('INVALID_SERVICE_DESCRIPTOR')
    for service in config.values():
        if type(service) is not dict or set(service)!=fields:
            raise ValueError('INVALID_SERVICE_DESCRIPTOR')
        for name,lower,upper in [('timeout_seconds',1,120),('max_retries',0,3)]:
            value=service[name]
            if type(value) is not int or not lower <= value <= upper:
                raise ValueError('INVALID_SERVICE_DESCRIPTOR')
        try:
            url=service['base_url']
            if type(url) is not str or not url or '\\' in url or any(c.isspace() or ord(c)<32 or ord(c)==127 for c in url):
                raise ValueError
            parsed=urlsplit(url)
            if parsed.scheme not in {'http','https'} or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
                raise ValueError
            if parsed.port is not None and not 1 <= parsed.port <= 65535:
                raise ValueError
        except ValueError:
            raise ValueError('INVALID_SERVICE_DESCRIPTOR') from None
        provider=service['provider'];model=service['model'];ref=service['credential_ref']
        if type(provider) is not str or re.fullmatch(r'[a-z][a-z0-9._-]{0,63}',provider) is None:
            raise ValueError('INVALID_SERVICE_DESCRIPTOR')
        if type(model) is not str or not model or len(model)>256 or model!=model.strip() or any(ord(c)<32 or ord(c)==127 for c in model):
            raise ValueError('INVALID_SERVICE_DESCRIPTOR')
        if ref is not None and (type(ref) is not str or re.fullmatch(r'civilization-theatre/service/[0-9a-f]{32}',ref) is None):
            raise ValueError('INVALID_SERVICE_DESCRIPTOR')
    return copy.deepcopy(config)

def public_summary(config):
    private=validate(config)
    return {kind:{'provider':s['provider'],'model':s['model'],
        'base_url_configured':bool(s['base_url']),
        'credential_reference_present':s['credential_ref'] is not None,
        'credential_status':'not-checked',
        'timeout_seconds':s['timeout_seconds'],'max_retries':s['max_retries']}
        for kind,s in private.items()}
