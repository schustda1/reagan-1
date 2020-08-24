import unittest
from subclass import Subclass


class TestSubclass(unittest.TestCase):
    def test_can_call_module(self):
        Subclass()

    def test_parser_exists(self):
        self.assertTrue(Subclass().parser)


if __name__ == "__main__":
    unittest.main()
