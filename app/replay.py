"""Explicit read-only archive replay entrypoint; separate from writer startup.

source_manifest is an explicit JSON path, exactly:
  {"world_id": "<canonical UUID>", "files": {
    "world.sqlite": "<lowercase SHA256>", "index.html": "<SHA256>",
    "app.js": "<SHA256>", "app.css": "<SHA256>"}}
All files must already exist. Copy only world.sqlite to a fresh archive directory
before invocation; never point the replay at the active writer's data directory.
No actors database, authority, migration or world initialization is needed.
"""
import json
import hashlib
import math
import time
from pathlib import Path
import re
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


class ReplayHTTPServer(ThreadingHTTPServer):
    # Join owned request handlers on close, including browser preconnections.
    daemon_threads = False
    block_on_close = True


class ReadOnlyTrace:
    """Narrow protocol over the original read functions, not a writer subclass."""
    def __init__(self, world):
        from .adjudication import Adjudicator
        self.world = world
        self._read = Adjudicator._read.__get__(self, type(self))
        self.trace = Adjudicator.trace.__get__(self, type(self))
        self._child_id = Adjudicator._child_id


class ReplayReader:
    """Only public observation operations; no actor, scheduler or DM factory."""
    def __init__(self, world):
        self._world = world
        self._trace = ReadOnlyTrace(world)

    def snapshot(self, seq=None):
        state = self._world.snapshot(seq)
        state['causality'] = self._trace.trace(seq=state['revision'])
        state['runtime'] = dict(read_only=True, busy=False, pause_requested=False, error=None)
        if seq is not None:
            state['runtime']['historical'] = True
        return state

    def observe(self, person_id, seq=None):
        return self._world.observe(person_id, seq=seq)



def _validated_inputs(source_manifest, data_dir, web_root, nonce, port):
    """Validate files and syntax before importing any world machinery."""
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError('INVALID_PORT')
    if type(nonce) is not str or re.fullmatch(r'[A-Za-z0-9_-]{1,128}', nonce) is None:
        raise ValueError('INVALID_NONCE')
    try:
        data = Path(data_dir).resolve(strict=True)
        web = Path(web_root).resolve(strict=True)
        manifest_path = Path(source_manifest).resolve(strict=True)
        if not data.is_dir() or not web.is_dir() or manifest_path.stat().st_size > 16384:
            raise ValueError('INVALID_INPUT')
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        if type(manifest) is not dict or set(manifest) != {'world_id','files'}:
            raise ValueError('INVALID_MANIFEST')
        if str(uuid.UUID(manifest['world_id'])) != manifest['world_id']:
            raise ValueError('INVALID_WORLD_ID')
        names = {'world.sqlite','index.html','app.js','app.css'}
        if type(manifest['files']) is not dict or set(manifest['files']) != names:
            raise ValueError('INVALID_MANIFEST')
        blobs = {}
        for name, expected in manifest['files'].items():
            if type(expected) is not str or re.fullmatch('[0-9a-f]{64}', expected) is None:
                raise ValueError('INVALID_DIGEST')
            path = (data if name == 'world.sqlite' else web) / name
            if path.is_symlink() or not path.is_file():
                raise ValueError('INVALID_FILE')
            blobs[name] = path.read_bytes()
            if hashlib.sha256(blobs[name]).hexdigest() != expected:
                raise ValueError('SOURCE_MISMATCH')
        if any(Path(str(data/'world.sqlite')+suffix).exists() for suffix in ('-wal','-shm','-journal')):
            raise ValueError('UNSAFE_SIDECAR')
        return data, manifest, blobs
    except (OSError, TypeError, KeyError, AttributeError, ValueError):
        raise ValueError('INVALID_REPLAY_INPUT') from None


def make_replay_server(*, source_manifest, data_dir, web_root, nonce, port):
    data, manifest, blobs = _validated_inputs(source_manifest, data_dir, web_root, nonce, port)
    from .world import WorldStore, WorldError
    world = WorldStore(data/'world.sqlite')
    if world.snapshot()['world_id'] != manifest['world_id']:
        raise ValueError('WORLD_MISMATCH')
    reader = ReplayReader(world)
    def sequence(query):
        if 'seq' not in query:
            return None
        values = query['seq']
        if (len(values) != 1 or not re.fullmatch(r'[0-9]{1,19}', values[0])
                or not 1 <= int(values[0]) <= 2**63-1):
            raise ValueError('INVALID_SEQUENCE')
        return int(values[0])

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            self.request.settimeout(2)
            super().setup()

        def log_message(self, *args):
            pass

        def send_payload(self, status, payload, mime):
            self.send_response(status)
            self.send_header('Content-Type', mime+'; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'")
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            if self.command != 'HEAD':
                self.wfile.write(payload)

        def reply(self, status, body):
            self.send_payload(status, json.dumps(body, ensure_ascii=False, allow_nan=False).encode('utf-8'), 'application/json')

        def trusted(self):
            port = self.server.server_port
            hosts = {f'127.0.0.1:{port}', f'localhost:{port}'}
            origins = self.headers.get_all('Origin', [])
            return (len(self.headers.get_all('Host', [])) == 1
                and self.headers.get('Host', '').lower() in hosts
                and len(origins) <= 1
                and (not origins or origins[0] in {f'http://{host}' for host in hosts}))

        def error(self, status, code):
            self.reply(status, {'error':{'code':code,'message':'请求未完成；只读归档不提供写操作'}})

        def reject_write(self):
            self.error(405, 'READ_ONLY') if self.trusted() else self.error(403, 'FORBIDDEN')

        def __getattr__(self, name):
            # BaseHTTPRequestHandler dispatches arbitrary HTTP methods via do_*.
            if name.startswith('do_'):
                return self.reject_write
            raise AttributeError(name)

        def do_GET(self):
            if not self.trusted():
                self.error(403, 'FORBIDDEN'); return
            try:
                if len(self.path) > 4096:
                    raise ValueError('INVALID_REQUEST')
                parsed = urlsplit(self.path)
                if parsed.scheme or parsed.netloc or parsed.fragment:
                    raise ValueError('INVALID_REQUEST')
                query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=16)
                if parsed.path == '/api/meta' and not query:
                    self.reply(200, dict(mode='readonly-replay', read_only=True,
                        capabilities=['readonly-replay','history-person','causal-trace','map-topology'],
                        limitations=['只读归档，不调用模型，不推进世界。']))
                elif parsed.path == '/api/state':
                    if set(query)-{'seq'}:
                        raise ValueError('INVALID_SEQUENCE')
                    self.reply(200, reader.snapshot(sequence(query)))
                elif parsed.path == '/api/person':
                    if (set(query)-{'id','seq'} or 'id' not in query or len(query['id']) != 1
                            or not query['id'][0] or len(query['id'][0]) > 256):
                        raise ValueError('INVALID_PERSON')
                    self.reply(200, reader.observe(query['id'][0], sequence(query)))
                elif parsed.path in ('/','/index.html','/app.js','/app.css') and not query:
                    name = 'index.html' if parsed.path == '/' else parsed.path[1:]
                    self.send_payload(200, blobs[name], {'index.html':'text/html','app.js':'text/javascript','app.css':'text/css'}[name])
                else:
                    self.error(404, 'NOT_FOUND')
            except ValueError as error:
                code = str(error) if str(error) in {'INVALID_SEQUENCE','INVALID_PERSON'} else 'INVALID_REQUEST'
                self.error(400, code)
            except WorldError as error:
                self.error(409, error.code)
            except Exception:
                self.error(500, 'TECHNICAL_ERROR')

    return ReplayHTTPServer(('127.0.0.1', port), Handler)


def main(argv=None):
    """No default data, port, identity, assets or source attestation."""
    import argparse
    parser = argparse.ArgumentParser(description='文明剧团只读归档回放（不调用模型）')
    parser.add_argument('--source-manifest', required=True, type=Path)
    parser.add_argument('--data-dir', required=True, type=Path)
    parser.add_argument('--web-root', required=True, type=Path)
    parser.add_argument('--nonce', required=True)
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--run-for', type=float)
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error('port must be between 0 and 65535')
    if re.fullmatch(r'[A-Za-z0-9_-]{1,128}', args.nonce) is None:
        parser.error('invalid nonce')
    if args.run_for is not None and (not math.isfinite(args.run_for) or not 0 < args.run_for <= 3600):
        parser.error('run-for must be finite and greater than 0 and at most 3600 seconds')
    try:
        server = make_replay_server(source_manifest=args.source_manifest, data_dir=args.data_dir,
            web_root=args.web_root, nonce=args.nonce, port=args.port)
    except Exception:
        parser.error('replay input validation failed; no world created')
    try:
        import os
        print(json.dumps(dict(mode='readonly-replay', read_only=True, pid=os.getpid(),
            nonce=args.nonce, host='127.0.0.1', port=server.server_port)), flush=True)
        if args.run_for is None:
            server.serve_forever(poll_interval=0.1)
        else:
            deadline = time.monotonic() + args.run_for
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                server.timeout = min(0.1, remaining)
                server.handle_request()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

