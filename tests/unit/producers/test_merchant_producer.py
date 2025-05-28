#!/usr/bin/env python3
# tests/unit/producers/test_merchant_producer.py

import unittest
from unittest.mock import patch, MagicMock, call
import json
import uuid
from datetime import datetime, timedelta

class TestMerchantProducer(unittest.TestCase):
    """Unit tests for merchant_producer.py"""

    @patch('src.producers.merchant_producer.fake')
    @patch('src.producers.merchant_producer.random')
    @patch('src.producers.merchant_producer.uuid')
    def test_generate_merchant_structure(self, mock_uuid, mock_random, mock_fake):
        """Test that generate_merchant returns a properly structured merchant object"""
        # Setup mock data
        mock_uuid.uuid4.return_value = "test-uuid-1234"
        mock_fake.company.return_value = "Test Company"
        mock_fake.country_code.return_value = "US"
        mock_fake.city.return_value = "New York"
        mock_fake.street_address.return_value = "123 Business St"
        mock_fake.postcode.return_value = "12345"
        mock_fake.latitude.return_value = "40.7128"
        mock_fake.longitude.return_value = "-74.0060"
        mock_fake.date_this_decade.return_value = datetime.now() - timedelta(days=365*5)
        mock_random.choice.side_effect = [
            "Retail",  # merchant_category
            "USD",     # currency
            ["Visa", "MasterCard", "Amex"],  # payment_methods
            False,     # has_fraud_history
            0.05       # fraud_risk
        ]
        mock_random.randint.return_value = 50  # employees
        
        # Import the function
        from src.producers.merchant_producer import generate_merchant
        
        # Call the function
        merchant = generate_merchant()
        
        # Assertions
        self.assertIsInstance(merchant, dict)
        self.assertIn('merchant_id', merchant)
        self.assertIn('merchant_name', merchant)
        self.assertIn('merchant_category', merchant)
        
        # Check nested structures
        self.assertIn('location', merchant)
        self.assertIsInstance(merchant['location'], dict)
        self.assertIn('country', merchant['location'])
        self.assertIn('city', merchant['location'])
        self.assertIn('address', merchant['location'])
        self.assertIn('postal_code', merchant['location'])
        self.assertIn('latitude', merchant['location'])
        self.assertIn('longitude', merchant['location'])
        
        self.assertIn('business', merchant)
        self.assertIsInstance(merchant['business'], dict)
        self.assertIn('founding_date', merchant['business'])
        self.assertIn('size', merchant['business'])
        self.assertIn('website', merchant['business'])
        
        self.assertIn('payment', merchant)
        self.assertIsInstance(merchant['payment'], dict)
        self.assertIn('methods', merchant['payment'])
        self.assertIn('currency', merchant['payment'])
        
        self.assertIn('risk', merchant)
        self.assertIsInstance(merchant['risk'], dict)
        self.assertIn('fraud_history', merchant['risk'])
        self.assertIn('risk_score', merchant['risk'])

    @patch('src.producers.merchant_producer.PostgresDB')
    def test_merchant_insertion_to_postgres(self, mock_postgres):
        """Test that merchants are properly inserted into PostgreSQL"""
        # Setup mock
        mock_db_instance = MagicMock()
        mock_postgres.return_value = mock_db_instance
        mock_db_instance.is_connected.return_value = True
        mock_db_instance.insert_merchant.return_value = True
        
        # We'll need to patch the run function and other dependencies
        with patch('src.producers.merchant_producer.generate_merchant') as mock_generate:
            # Setup mock merchant data
            mock_merchant = {
                'merchant_id': str(uuid.uuid4()),
                'merchant_name': 'Test Company',
                'merchant_category': 'Retail',
                'location': {
                    'country': 'US',
                    'city': 'New York',
                    'address': '123 Business St',
                    'postal_code': '12345',
                    'latitude': 40.7128,
                    'longitude': -74.0060
                },
                'business': {
                    'founding_date': '2018-01-01',
                    'size': 'Medium',
                    'website': 'https://testcompany.com',
                    'employees': 50
                },
                'payment': {
                    'methods': ['Visa', 'MasterCard', 'Amex'],
                    'currency': 'USD',
                    'online_payments': True
                },
                'risk': {
                    'fraud_history': False,
                    'risk_score': 0.05,
                    'last_audit': '2023-01-01'
                }
            }
            mock_generate.return_value = mock_merchant
            
            # Import and patch the run function
            with patch('src.producers.merchant_producer.run') as mock_run:
                from src.producers.merchant_producer import run
                
                # Call the patched run function with test parameters
                with patch('src.producers.merchant_producer.time.sleep'):  # Avoid actual sleep
                    with patch('src.producers.merchant_producer.running', True, create=True):
                        # Set up to run only once
                        mock_run.side_effect = lambda: None
                        run()
                
                # Verify the database insert was called
                mock_db_instance.insert_merchant.assert_called()

    @patch('src.producers.merchant_producer.random')
    def test_merchant_category_affects_attributes(self, mock_random):
        """Test that merchant category influences other merchant attributes"""
        # Import the function
        from src.producers.merchant_producer import generate_merchant
        
        # Test for different categories
        categories = ["Retail", "Electronics", "Travel", "Dining", "Finance"]
        
        for category in categories:
            # Reset mocks
            mock_random.reset_mock()
            
            # Setup category selection
            mock_random.choice.side_effect = [
                category,  # merchant_category
                "USD",     # currency
                ["Visa", "MasterCard"],  # payment_methods
                False,     # has_fraud_history
                0.05       # fraud_risk
            ]
            
            # Patch other dependencies
            with patch('src.producers.merchant_producer.fake'):
                with patch('src.producers.merchant_producer.uuid'):
                    # Call the function
                    merchant = generate_merchant()
                    
                    # Verify the category was set correctly
                    self.assertEqual(merchant['merchant_category'], category)
                    
                    # Different assertions based on category
                    if category == "Finance":
                        # Finance merchants should have higher risk scrutiny
                        self.assertIn('risk', merchant)
                        self.assertIn('last_audit', merchant['risk'])
                    
                    if category in ["Retail", "Electronics"]:
                        # These categories typically have online presence
                        self.assertIn('payment', merchant)
                        self.assertIn('online_payments', merchant['payment'])

if __name__ == '__main__':
    unittest.main()
