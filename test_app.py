import unittest
from main import app
from worldcheck import Parse_WorldCheck
from unittest.mock import MagicMock, patch

class TestApp(unittest.TestCase):

    def setUp(self):
        self.app = app.test_client()
        
    def test_parseWorldCheckSuccess(self): 
        headers = {'TenantID': 'SQL'}
        response = self.app.get('/parseWorldCheck', headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Data processed", response.data)
    
    def test_parseWorldCheckInvalidData(self):
        headers = {'TenantID': 'SQL'}
        # Send request with invalid data
        response = self.app.get('/parseWorldCheck', headers=headers, data={})
        self.assertEqual(response.status_code, 200)  # Expect a Bad Request response
        self.assertIn(b"Data processed", response.data)
    
    def test_parseWorldCheckNoTenantID(self):
        # Send request without the TenantID header
        response = self.app.get('/parseWorldCheck')
        self.assertEqual(response.status_code, 400)  # Expect a Bad Request response
        self.assertIn(b"Missing TenantID", response.data)
        
    def test_Parse_WorldCheck_Valid_TenantValue(self):
        # Test with valid TenantValue ("sql")
        TenantValue = "sql"  #or mySql
        result = Parse_WorldCheck(TenantValue)
        self.assertTrue(result)  # Expect that it returns True for a valid value

    @patch('worldcheck.get_database_connection')
    def test_Parse_WorldCheck_Invalid_TenantValue(self, mock_get_db_connection):
        # mock connection
        mock_db_connection = MagicMock()
        mock_get_db_connection.return_value = mock_db_connection 
        TenantValue = "invalid_value"
        result = Parse_WorldCheck(TenantValue)
        # Assertions based on your test case
        self.assertFalse(result) 
  
