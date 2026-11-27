"""Public facade; the binding implementation lives in attested actor_execution.
No authority guard imports this facade or trusts a new unregistered module.
"""
from .actor_execution import make_request, make_candidate, body_matches, valid_request, valid_usage, request_digest
