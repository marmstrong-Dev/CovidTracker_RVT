import unittest
from src.App import main_menu


class AppTests(unittest.TestCase):
    def test_routes(self):
        self.assertFalse(main_menu())
