"""Static asset/DOM contract checks only; NOT browser acceptance."""
from html.parser import HTMLParser
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class Hooks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = set()
        self.remote = []
        self.scripts = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            self.ids.add(attrs['id'])
        if tag in ('script', 'link'):
            target = attrs.get('src', attrs.get('href', ''))
            if target.startswith(('http:', 'https:', '//')):
                self.remote.append(target)
        if tag == 'script':
            self.scripts.append(attrs)


class WebAssetsTests(unittest.TestCase):
    def test_observation_workspace_dom_hooks(self):
        path = ROOT / 'web/index.html'
        self.assertTrue(path.exists(), 'observation workspace HTML is not implemented')
        html = path.read_text(encoding='utf-8')
        parser = Hooks()
        parser.feed(html)
        required = {'model-mode', 'day-status', 'step', 'run', 'pause', 'app-status',
                    'country-select', 'person-select', 'map', 'map-legend', 'place-detail',
                    'world-metrics', 'person-card', 'card-fields', 'card-feedback',
                    'observations', 'person-status', 'events', 'replay', 'latest',
                    'replay-status', 'retry', 'settings-note'}
        self.assertFalse(required - parser.ids)
        self.assertEqual(parser.remote, [])
        self.assertIn({'type': 'module', 'src': '/app.js'}, parser.scripts)
        self.assertIn('不是测绘地图', html)
        self.assertIn('全知', html)
        self.assertIn('合法见闻', html)
        self.assertIn('未接入', html)

    def test_safe_runtime_and_responsive_assets(self):
        js_path, css_path = ROOT / 'web/app.js', ROOT / 'web/app.css'
        self.assertTrue(css_path.exists(), 'responsive workspace stylesheet is missing')
        source = js_path.read_text(encoding='utf-8')
        css = css_path.read_text(encoding='utf-8')
        for forbidden in ('innerHTML', 'outerHTML', 'insertAdjacentHTML', 'document.write', 'eval('):
            self.assertNotIn(forbidden, source)
        for hook in ('boot', 'loadState', 'loadPerson', 'renderMap', 'renderEvents', 'submitCard'):
            self.assertIn('function ' + hook + '(', source)
        for endpoint in ('/api/meta', '/api/state', '/api/person?id=', '/api/control', '/api/card'):
            self.assertIn(endpoint, source)
        self.assertIn('textContent', source)
        self.assertIn('createElementNS', source)
        self.assertIn('@media', css)
        self.assertIn(':focus-visible', css)
        self.assertNotIn('@import', css)
        self.assertNotIn('https://', css)


if __name__ == '__main__':
    unittest.main()
