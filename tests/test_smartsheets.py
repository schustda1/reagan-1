import unittest
from smartsheets import SmartsheetAPI


class TestSmartsheets(unittest.TestCase):
    def __init__(self):
        self.ss = Smartsheets()

    def test_parser_exists(self):
        self.assertTrue(self.ss.parser)

    def test_bearer_token_in_parser(self):
        token = self.ss.parser.get("smartsheets", "bearer_token")
        self.assertTrue(token)


if __name__ == "__main__":
    unittest.main()
