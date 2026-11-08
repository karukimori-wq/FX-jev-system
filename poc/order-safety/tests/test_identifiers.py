import unittest
from fx_poc.identifiers import scoped_id
from fx_poc.models import Mode

class IdentifierTests(unittest.TestCase):
    def test_modes_runs_and_kinds_are_distinct(self):
        ids = {scoped_id(mode,run,kind,'same') for mode in Mode for run in ('a','b') for kind in ('decision','close','execution','position')}
        self.assertEqual(16,len(ids))
    def test_encoding_is_unambiguous_and_restart_stable(self):
        self.assertNotEqual(scoped_id(Mode.LIVE,'a:b','decision','c'),scoped_id(Mode.LIVE,'a','decision','b:c'))
        self.assertEqual(scoped_id(Mode.DEMO,'日本語','execution','"\\'),scoped_id('demo','日本語','execution','"\\'))
    def test_invalid_identity_rejected(self):
        for args in [('paper','a','decision','x'),('live','','decision','x'),('live','a','other','x'),('live','a','decision',None)]:
            with self.subTest(args=args), self.assertRaises(ValueError): scoped_id(*args)
