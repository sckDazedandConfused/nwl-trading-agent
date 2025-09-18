# test_api_client.py
import unittest
from unittest.mock import patch
import src.api_client as api_client

class TestSessionSingleton(unittest.TestCase):
    def setUp(self):
        # Reset the singleton before each test
        api_client._session = None

    @patch('src.api_client.requests.Session')
    def test_session_singleton(self, mock_session):
        instance1 = api_client._session_pooled()
        instance2 = api_client._session_pooled()
        self.assertIs(instance1, instance2)
        mock_session.assert_called_once()

if __name__ == '__main__':
    unittest.main()