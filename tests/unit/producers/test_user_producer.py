#!/usr/bin/env python3
# tests/unit/producers/test_user_producer.py

import unittest
from unittest.mock import patch, MagicMock, call
import json
from src.producers.user_producer import generate_user

class TestUserProducer(unittest.TestCase):
    """Unit tests for user_producer.py"""

    @patch('src.producers.user_producer.fake')
    def test_generate_user_structure(self, mock_fake):
        """Test that generate_user returns a properly structured user object"""
        # Setup mock data
        mock_fake.name.return_value = "Test User"
        mock_fake.email.return_value = "test@example.com"
        mock_fake.job.return_value = "Software Engineer"
        mock_fake.street_address.return_value = "123 Test St"
        mock_fake.postcode.return_value = "12345"
        mock_fake.latitude.return_value = "40.7128"
        mock_fake.longitude.return_value = "-74.0060"
        
        # Call the function
        user = generate_user()
        
        # Assertions
        self.assertIsInstance(user, dict)
        self.assertIn('user_id', user)
        self.assertIn('name', user)
        self.assertIn('email', user)
        self.assertIn('age', user)
        self.assertIn('gender', user)
        self.assertIn('occupation', user)
        self.assertIn('account_created', user)
        
        # Check nested structures
        self.assertIn('location', user)
        self.assertIsInstance(user['location'], dict)
        self.assertIn('country', user['location'])
        self.assertIn('city', user['location'])
        self.assertIn('latitude', user['location'])
        self.assertIn('longitude', user['location'])
        
        self.assertIn('financial', user)
        self.assertIsInstance(user['financial'], dict)
        self.assertIn('credit_score', user['financial'])
        self.assertIn('risk_score', user['financial'])
        
        self.assertIn('preferences', user)
        self.assertIsInstance(user['preferences'], dict)
        self.assertIn('spending_categories', user['preferences'])

    @patch('src.producers.user_producer.PostgresDB')
    def test_user_insertion_to_postgres(self, mock_postgres):
        """Test that users are properly inserted into PostgreSQL"""
        # Setup mock
        mock_db_instance = MagicMock()
        mock_postgres.return_value = mock_db_instance
        mock_db_instance.is_connected.return_value = True
        mock_db_instance.insert_user.return_value = True
        
        # We'll need to patch the run function and other dependencies
        with patch('src.producers.user_producer.generate_user') as mock_generate:
            # Setup mock user data
            mock_user = {
                'user_id': 12345,
                'name': 'Test User',
                'email': 'test@example.com',
                'age': 30,
                'gender': 'M',
                'occupation': 'Software Engineer',
                'account_created': '2023-01-01T00:00:00',
                'location': {
                    'country': 'US',
                    'city': 'New York',
                    'address': '123 Test St',
                    'postal_code': '12345',
                    'latitude': 40.7128,
                    'longitude': -74.0060
                },
                'financial': {
                    'credit_score': 750,
                    'risk_score': 0.2,
                    'income_bracket': 'High',
                    'preferred_payment_methods': ['Credit Card', 'PayPal']
                },
                'preferences': {
                    'spending_categories': ['Electronics', 'Travel'],
                    'communication_channel': 'Email'
                }
            }
            mock_generate.return_value = mock_user
            
            # Import and patch the run function
            with patch('src.producers.user_producer.run') as mock_run:
                from src.producers.user_producer import run
                
                # Call the patched run function with test parameters
                with patch('src.producers.user_producer.time.sleep'):  # Avoid actual sleep
                    with patch('src.producers.user_producer.running', True, create=True):
                        # Set up to run only once
                        mock_run.side_effect = lambda: None
                        run()
                
                # Verify the database insert was called
                mock_db_instance.insert_user.assert_called()

    @patch('src.producers.user_producer.random')
    def test_user_data_randomization(self, mock_random):
        """Test that user data is properly randomized"""
        # Setup mocks for random values
        mock_random.randint.side_effect = [30, 750]  # age, credit_score
        mock_random.random.return_value = 0.2  # For risk_score
        mock_random.choice.side_effect = [
            'M',  # gender
            'High',  # income_bracket
            ['Electronics', 'Travel'],  # spending_categories
            'Email'  # communication_channel
        ]
        
        # Call with patched faker
        with patch('src.producers.user_producer.fake'):
            user = generate_user()
            
            # Verify random calls were made correctly
            mock_random.randint.assert_any_call(18, 85)  # age range
            mock_random.randint.assert_any_call(300, 850)  # credit score range
            mock_random.choice.assert_any_call(['M', 'F', 'Other'])  # gender options
            
            # Verify the values in the generated user
            self.assertEqual(user['age'], 30)
            self.assertEqual(user['gender'], 'M')
            self.assertEqual(user['financial']['credit_score'], 750)
            self.assertEqual(user['financial']['income_bracket'], 'High')
            self.assertEqual(user['preferences']['spending_categories'], ['Electronics', 'Travel'])
            self.assertEqual(user['preferences']['communication_channel'], 'Email')

if __name__ == '__main__':
    unittest.main()
