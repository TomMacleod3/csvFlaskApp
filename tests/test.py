import unittest
from unittest.mock import patch, MagicMock
from app import app, allowed_file, clean_column_name, connect_to_mysql

class FlaskAppTest(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    def test_home_page(self):
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)

    def test_post_no_files(self):
        response = self.app.post('/', content_type='multipart/form-data')
        self.assertIn(b'No files selected or all files were of invalid types', response.data)
        self.assertEqual(response.status_code, 200)

    def test_allowed_file(self):
        self.assertTrue(allowed_file('file.csv'))
        self.assertFalse(allowed_file('file.txt'))

    def test_clean_column_name(self):
        self.assertEqual(clean_column_name('Column (Name)'), 'Column_Name')
        self.assertEqual(clean_column_name('column (name)'), 'column_name')

    @patch('app.mysql.connector.connect')
    def test_connect_to_mysql(self, mock_connect):
        connect_to_mysql()
        mock_connect.assert_called_once()

if __name__ == '__main__':
    unittest.main()