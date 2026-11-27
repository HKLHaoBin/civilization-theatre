"""Pure lexical-contract regressions; no application/SQLite import or world."""
import ast
from pathlib import Path
import re
import unittest


class HistoryAuditSQLLexicalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source_path = Path(__file__).resolve().parents[1] / 'coordinator_scheduler.py'
        tree = ast.parse(cls.source_path.read_bytes())
        wanted = {'_HISTORY_SELECT_FORMS', '_history_sql_tokens',
                  '_HISTORY_SELECT_TOKENS', '_history_read_sql'}
        nodes = []
        for node in tree.body:
            names = {node.name} if isinstance(node, ast.FunctionDef) else {
                target.id for target in node.targets if isinstance(target, ast.Name)
            } if isinstance(node, ast.Assign) else set()
            if names & wanted:
                nodes.append(node)
        if len(nodes) != len(wanted):
            raise AssertionError('exact original classifier closure is required')
        namespace = {'re': re}
        exec(compile(ast.Module(body=nodes, type_ignores=[]),
                     str(cls.source_path), 'exec'), namespace)
        cls.classify = staticmethod(namespace['_history_read_sql'])

    def test_numeric_keyword_concatenation_is_not_a_complete_trusted_form(self):
        queries = [
            "SELECT 1FROM actor.sqlite_master WHERE type='table' AND name='actor_session_heads'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=1AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=-1AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=1e2AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=.5AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=+12AND person_id='p'",
        ]
        for query in queries:
            with self.subTest(SQL=query):
                self.assertIs(self.classify(query), False)

    def test_legitimate_separation_and_literal_contents_remain_readonly(self):
        queries = [
            "SELECT 1 FROM actor.sqlite_master WHERE type='table' AND name='actor_session_heads'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=1 AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=-1 AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=1e2 AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=.5 AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=+12 AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id='1AND;--literal' AND person_id='p''q'",
            "  select  1  from actor.sqlite_master where type='table' and name='actor_session_heads' ; ",
            "  pragma BUSY_TIMEOUT = 1 ; ",
        ]
        for query in queries:
            with self.subTest(SQL=query):
                self.assertIs(self.classify(query), True)

    def test_unknown_and_compound_forms_stay_rejected(self):
        queries = [
            'SELECT history_unknown_function(1)',
            'SELECT 1 FROM',
            'SELECT 1; DELETE FROM actor.actor_intents',
            'SELECT 1 UNION SELECT 2',
            "SELECT * FROM actor.actor_intents WHERE intent_id=1+2 AND person_id='p'",
            "SELECT * FROM actor.actor_intents WHERE intent_id=unknown_name AND person_id='p'",
            'PRAGMA busy_timeout=2',
            'COMMIT',
        ]
        for query in queries:
            with self.subTest(SQL=query):
                self.assertIs(self.classify(query), False)


if __name__ == '__main__':
    unittest.main()
