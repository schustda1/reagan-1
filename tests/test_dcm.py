import unittest
from dcm import DCMApi


class TestDCM(unittest.TestCase):
    def __init__(self):
        self.dcm = DCMApi()

    def test_parser_exists(self):
        self.assertTrue(self.ss.parser)


if __name__ == "__main__":
    unittest.main()
