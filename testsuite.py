from Article_Parser import ParseRtf
from pathlib import Path
import unittest

homeDir = str(Path.home())

class ParseTest(unittest.TestCase)

    def test_parser_construction(self):
        self.assertTrue(parser = ParseRtf(output_directory=homeDir))

    def test_time(self):
        '2009-07-01 9:35:07'
        self.assertTrue()



print('all tests completed successfully')