"""Independent non-production candidate. Import performs no native calls."""
import re
import ctypes as c
from ctypes import wintypes as w
from typing import Protocol

CRED_TYPE_GENERIC = 1
CRED_PERSIST_LOCAL_MACHINE = 2
MAX_BLOB = 2560
ERROR_NOT_FOUND = 1168
ERROR_NO_SUCH_LOGON_SESSION = 1312


class CREDENTIAL_ATTRIBUTEW(c.Structure):
    _fields_ = [('Keyword', w.LPWSTR), ('Flags', w.DWORD),
                ('ValueSize', w.DWORD), ('Value', c.POINTER(w.BYTE))]


class CREDENTIALW(c.Structure):
    _fields_ = [('Flags', w.DWORD), ('Type', w.DWORD),
                ('TargetName', w.LPWSTR), ('Comment', w.LPWSTR),
                ('LastWritten', w.FILETIME), ('CredentialBlobSize', w.DWORD),
                ('CredentialBlob', c.POINTER(w.BYTE)), ('Persist', w.DWORD),
                ('AttributeCount', w.DWORD), ('Attributes', c.POINTER(CREDENTIAL_ATTRIBUTEW)),
                ('TargetAlias', w.LPWSTR), ('UserName', w.LPWSTR)]


class CredentialError(Exception):
    def __init__(self, code, winerror=0):
        self.code = code
        self.winerror = winerror
        super().__init__(code, winerror)


class CredentialAdapter(Protocol):
    """Minimal opaque-secret contract used by settings and the gateway."""
    def read(self, ref): ...
    def write(self, ref, secret): ...
    def delete(self, ref): ...


class WindowsCredentialAdapter:
    """Explicit native construction; injected APIs are unit boundaries, not native.

    LOCAL_MACHINE means this user on this machine across logon sessions, not
    all users. Other processes with the same user's authority may read records.
    TargetName is natively case-insensitive; metadata validation stays strict.
    """
    def __init__(self, *, api=None, get_last_error=None):
        self._api = api if api is not None else c.WinDLL('Advapi32.dll', use_last_error=True)
        self._last_error = get_last_error if get_last_error is not None else c.get_last_error
        for name, args, result in [
            ('CredWriteW', [c.POINTER(CREDENTIALW), w.DWORD], w.BOOL),
            ('CredReadW', [w.LPCWSTR, w.DWORD, w.DWORD, c.POINTER(c.POINTER(CREDENTIALW))], w.BOOL),
            ('CredDeleteW', [w.LPCWSTR, w.DWORD, w.DWORD], w.BOOL),
            ('CredFree', [c.c_void_p], None),
        ]:
            fn = getattr(self._api, name)
            fn.argtypes = args
            fn.restype = result


    def delete(self, ref):
        validate_ref(ref)
        if not self._api.CredDeleteW(ref, CRED_TYPE_GENERIC, 0):
            error = self._last_error()
            if error == ERROR_NOT_FOUND:
                return False
            raise CredentialError('NO_LOGON_SESSION' if error == ERROR_NO_SUCH_LOGON_SESSION else 'OS_ERROR', error)
        return True

    def read(self, ref):
        validate_ref(ref)
        pointer = c.POINTER(CREDENTIALW)()
        if not self._api.CredReadW(ref, CRED_TYPE_GENERIC, 0, c.byref(pointer)):
            error = self._last_error()
            if error == ERROR_NOT_FOUND:
                return None
            raise CredentialError('NO_LOGON_SESSION' if error == ERROR_NO_SUCH_LOGON_SESSION else 'OS_ERROR', error)
        try:
            if not pointer:
                raise CredentialError('INVALID_NATIVE_RECORD')
            item = pointer.contents
            size = item.CredentialBlobSize
            if (item.Type != CRED_TYPE_GENERIC or item.TargetName != ref
                    or item.Persist != CRED_PERSIST_LOCAL_MACHINE or size > MAX_BLOB
                    or (size and not item.CredentialBlob)):
                raise CredentialError('INVALID_NATIVE_RECORD')
            return OpaqueSecret(c.string_at(item.CredentialBlob, size) if size else b'')
        finally:
            if pointer:
                self._api.CredFree(pointer)

    def write(self, ref, secret):
        validate_ref(ref)
        if type(secret) not in (bytes, bytearray) or not 1 <= len(secret) <= MAX_BLOB:
            raise ValueError('INVALID_SECRET')
        owned = (w.BYTE * len(secret)).from_buffer_copy(secret)
        try:
            item = CREDENTIALW()
            item.Type = CRED_TYPE_GENERIC
            item.TargetName = ref
            item.CredentialBlobSize = len(secret)
            item.CredentialBlob = c.cast(owned, c.POINTER(w.BYTE))
            item.Persist = CRED_PERSIST_LOCAL_MACHINE
            if not self._api.CredWriteW(c.byref(item), 0):
                error = self._last_error()
                raise CredentialError('NO_LOGON_SESSION' if error == ERROR_NO_SUCH_LOGON_SESSION else 'OS_ERROR', error)
        finally:
            c.memset(owned, 0, len(owned))


def validate_ref(ref):
    if type(ref) is not str or re.fullmatch(r'civilization-theatre/service/[0-9a-f]{32}', ref) is None:
        raise ValueError('INVALID_CREDENTIAL_REF')
    return ref


class OpaqueSecret:
    """Owned mutable copy only. reveal_bytes makes an unerasable Python copy.

    No process isolation or comprehensive Python/GC memory erasure is promised.
    Trusted callers must close explicitly; repr/str never expose the value.
    """
    __slots__ = ('_buffer', '_closed')

    def __init__(self, value):
        if type(value) not in (bytes, bytearray):
            raise ValueError('INVALID_SECRET')
        self._buffer = bytearray(value)
        self._closed = False

    def __repr__(self):
        return '<OpaqueSecret redacted>'

    __str__ = __repr__

    def reveal_bytes(self):
        if self._closed:
            raise ValueError('SECRET_CLOSED')
        return bytes(self._buffer)

    def close(self):
        for index in range(len(self._buffer)):
            self._buffer[index] = 0
        self._closed = True


class InMemoryCredentialAdapter:
    """Redacted synthetic vault for contract tests only.

    This adapter is deliberately not selected by production defaults. It keeps
    mutable copies, returns OpaqueSecret values, and never exposes stored bytes
    through repr/str or a status method.
    """
    def __init__(self):
        self._values = {}

    def read(self, ref):
        validate_ref(ref)
        value = self._values.get(ref)
        return None if value is None else OpaqueSecret(value)

    def write(self, ref, secret):
        validate_ref(ref)
        if type(secret) not in (bytes, bytearray) or not 1 <= len(secret) <= MAX_BLOB:
            raise ValueError('INVALID_SECRET')
        self._values[ref] = bytearray(secret)

    def delete(self, ref):
        validate_ref(ref)
        value = self._values.pop(ref, None)
        if value is None:
            return False
        for index in range(len(value)):
            value[index] = 0
        return True

    def clear(self):
        for ref in tuple(self._values):
            self.delete(ref)
