"""Explicit control-only test fixture; never imported by production.

_run_step loop copied from frozen candidates/p02-3a/app/server.py,
SHA256 de65345bfcda83c9160ad5af91fab307a5e55b8d3d220190427700bbcb98d354.
No activation creation/completion, no scheduler bypass in production.
"""
from app.server import Coordinator,DAY


class LegacyControlCoordinator(Coordinator):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self._reject_registered()

    def _reject_registered(self):
        from app.coordinator_scheduler import exists
        with self._authority._connection(readonly=True) as db:
            if exists(db,'coordinator_scheduler_registration'):
                raise RuntimeError('LEGACY_CONTROL_REGISTERED_FORBIDDEN')

    def _prepare_scheduler(self,captured_owner):
        self._reject_registered()
        captured_owner.check()

    def _run_step(self, continuous):
        self._reject_registered()
        while True:
            state = self.world.snapshot()
            if state['status'] == 'horizon_paused':
                break
            (self._authority or self.world).advance(min(state['time'] + DAY, state['horizon']))
            if self.world.snapshot()['status'] == 'running':
                for person in self.world.snapshot()['persons']:
                    if getattr(self.actors, 'execution_fenced', False) is True:
                        self.actors.act(person['id'], pause_on_failure=True)
                    else:
                        self.actors.act(person['id'])
            if not continuous or self.stop.is_set():
                if self.world.snapshot()['status'] == 'running':
                    (self._authority or self.world).control('pause')
                break
