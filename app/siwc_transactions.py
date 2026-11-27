"""1005 short in-memory arbitration. F is NOT a durability acknowledgement.
No file, vault, network, unlock or GC operation runs under this lock.
"""
import threading
import uuid
from .real_provider import ProviderFault


class AuthTransaction:
    def __init__(self,guard=None,*,kind='account'):
        self.id=uuid.uuid4().hex;self.kind=kind;self.guard=guard
        self.lock=threading.RLock();self.phase='PREPARING';self.durable=False
        self.writer_active=True;self.plan=None;self.view_confirmed=False

    def bind_plan(self,old,candidate):
        import copy
        with self.lock:self.plan=(copy.deepcopy(old),copy.deepcopy(candidate))

    def writer_stopped(self):
        # Called only from the outer writer finally, after L scope has exited.
        with self.lock:self.writer_active=False

    def recovery_ready(self):
        with self.lock:return not self.writer_active and self.phase=='UNKNOWN'

    def confirm_view(self,confirmed=True):
        with self.lock:self.view_confirmed=confirmed

    def check(self):
        with self.lock:
            if self.phase=='CANCEL_WON':raise ProviderFault('SIWC_AUTH_CANCELLED')
            guard=self.guard if self.phase=='PREPARING' else None
        if guard is not None:guard()

    def finalize(self):
        # Callers have already staged+readback slots and fsynced final bytes.
        # Guard is the Session's short epoch/budget check, never native IO.
        with self.lock:
            if self.phase=='CANCEL_WON':raise ProviderFault('SIWC_AUTH_CANCELLED')
            if self.phase!='PREPARING':raise ProviderFault('SIWC_TRANSACTION_STATE')
            if self.guard is not None:self.guard()
            self.phase='FINALIZING'

    def cancel(self):
        with self.lock:
            if self.phase=='PREPARING':self.phase='CANCEL_WON'
            return self._status()

    def finish(self,outcome):
        # Only AFTER authoritative readback AND native lock release returned.
        with self.lock:
            self.phase=outcome;self.durable=outcome in ('COMMITTED','CANCELLED')

    def _status(self):
        outcome={'PREPARING':'preparing','CANCEL_WON':'cancel_won_pending',
            'FINALIZING':'commit_won_pending','COMMITTED':'committed_complete',
            'CANCELLED':'cancelled_complete','UNKNOWN':'commit_unknown'}.get(self.phase,'commit_unknown')
        return dict(txid=self.id,outcome=outcome,request_ack=True,durable_ack=self.durable,
            durability='confirmed' if self.durable else 'pending_or_unknown',
            cancellation_complete=self.phase=='CANCELLED',
            account_mutation_pending=not self.durable and not self.view_confirmed,
            current_view_confirmed=self.durable or self.view_confirmed,
            historical_outcome_unknown=self.phase=='UNKNOWN')

    def status(self):
        with self.lock:return self._status()
