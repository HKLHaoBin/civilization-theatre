"""CLI forwarding contract with explicit doubles; not HTTP/world acceptance."""
from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app import server


class ScenarioCLITests(unittest.TestCase):
    def test_explicit_scenario_path_reaches_world_constructor(self):
        received = []
        class World:
            def __init__(self, db_path, *, scenario_path=None):
                received.append((db_path, scenario_path))
            def initialize(self):
                return {'status':'user_paused'}
        closed = []
        fake_server = SimpleNamespace(server_address=('127.0.0.1', 0),
                                      serve_forever=lambda: None,
                                      server_close=lambda: closed.append(True))
        with tempfile.TemporaryDirectory(dir=Path(os.environ['LOCALAPPDATA'])/'hermes/cache/scratch') as directory:
            path = Path(directory)
            requested = path/'authored scenario.json'
            with patch.dict(sys.modules, {'app.world': SimpleNamespace(WorldStore=World)}), \
                    patch.object(server, 'create_default_actors', return_value=object()), \
                    patch.object(server, 'make_server', return_value=fake_server), redirect_stdout(io.StringIO()):
                try:
                    server.main(['--port','0','--data-dir',str(path),'--scenario',str(requested)])
                except SystemExit as error:
                    self.fail(f'explicit scenario CLI rejected: {error.code}')
            self.assertEqual(received, [(path/'world.sqlite', requested)])
            self.assertEqual(closed, [True])


if __name__ == '__main__':
    unittest.main()
