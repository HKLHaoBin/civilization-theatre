"""Host-only root capability. Never serialized into domain or provider data."""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class CoordinatorPermit:
    world_id: str
    owner_token: str = field(repr=False)
    epoch: int


class CoordinatorAttempt:
    """Captured root lifetime; maintenance never takes the pipeline lock."""
    def __init__(self, world, permit, policy):
        import threading
        self.world, self.permit, self.policy = world, permit, policy
        self.stop = threading.Event()
        self.failure = None
        self.deadline = float('inf')
        self.thread = None

    def check(self):
        if self.failure is not None:
            raise self.failure
        if self.stop.is_set():
            from .world import WorldError
            raise WorldError('COORDINATOR_EXECUTION_CANCELLED', '协调宿主已停止')

    def start(self, on_failure):
        import threading
        from .actor_execution import storage_retry
        def maintain():
            while not self.stop.wait(self.policy.renew_interval_seconds):
                try:
                    self.check()
                    def renew():
                        self.check()
                        return self.world.renew_coordinator_execution(self.permit, ttl_seconds=self.policy.ttl_seconds)
                    storage_retry(renew, self.policy, stop=self.stop, _heartbeat_deadline=True)
                except Exception as error:
                    self.failure = error
                    self.stop.set()
                    on_failure()
                    return
        self.thread = threading.Thread(target=maintain, name='coordinator-heartbeat', daemon=True)
        self.thread.start()

    def finish(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join(2.5)
        return self.thread is None or not self.thread.is_alive()


# Diagnostic loaded generation only; no lifecycle or authority semantics change.
from .source_attestation import register as _register_source
_SOURCE_GENERATION = _register_source(globals())
