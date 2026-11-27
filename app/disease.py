"""Pure identity/validation for uncalibrated fictional contact rules.

Unknown JSON fields are preserved in the identity, not interpreted as mechanics.
No world imports, clocks, randomness, mutation or storage.
"""
import hashlib
import json

CONTACT_RUNTIME = 'CONTACT_EXPOSURE_V2'


def contact_mechanism_digest(runtime, rules):
    """Explicit opt-in validation; never reinterpret a legacy unknown field."""
    pathology_digest(rules)
    if type(runtime) is not str or runtime != CONTACT_RUNTIME:
        raise ValueError('unsupported pathology_runtime')
    if rules['version'] != 'NON_MEDICAL_FICTIONAL_CONTACT_V2':
        raise ValueError('pathology.version must identify CONTACT_V2')
    lifetime = rules.get('exposure_lifetime_seconds')
    if type(lifetime) is not int or lifetime < 1:
        raise ValueError('pathology.exposure_lifetime_seconds must be a positive integer')
    encoded = json.dumps({'runtime':runtime, 'pathology':rules}, ensure_ascii=False,
                         sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(encoded.encode('utf-8')).hexdigest()


def pathology_digest(rules):
    """Return SHA256 of ALL canonical JSON fields; reject invalid known rules."""
    if type(rules) is not dict:
        raise ValueError('pathology must be an object')
    if type(rules.get('version')) is not str or not rules['version'].strip():
        raise ValueError('pathology.version must be nonempty text')
    for name in ('symptom_seconds', 'severe_seconds', 'recovery_seconds',
                 'contact_offset', 'open_contacts', 'restricted_contacts', 'contacts_per_infection'):
        minimum = 0 if name in ('contact_offset', 'open_contacts', 'restricted_contacts') else 1
        if type(rules.get(name)) is not int or rules[name] < minimum:
            raise ValueError(f'pathology.{name} must be an integer >= {minimum}')
    if not rules['symptom_seconds'] < rules['severe_seconds'] < rules['recovery_seconds']:
        raise ValueError('pathology durations must satisfy symptom < severe < recovery')
    if rules['restricted_contacts'] > rules['open_contacts']:
        raise ValueError('pathology restricted_contacts must not exceed open_contacts')
    try:
        encoded = json.dumps(rules, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
        return hashlib.sha256(encoded.encode('utf-8')).hexdigest()
    except (TypeError, ValueError, UnicodeError, RecursionError) as error:
        raise ValueError('pathology must contain finite JSON values') from error
