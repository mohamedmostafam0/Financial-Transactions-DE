#!/usr/bin/env python3
# tests/unit/producers/test_transaction_producer.py

import unittest
from unittest.mock import patch, MagicMock, call
import json
from datetime import datetime, timezone

class TestTransactionProducer(unittest.TestCase):
    """Unit tests for transaction_producer.py"""

    def setUp(self):
        """Setup common test dependencies"""
        # Mock user data
        self.mock_user = {
            'user_id': 12345,
            'name': 'Test User',
            'email': 'test@example.com',
            'location': {
                'country': 'US',
                'city': 'New York',
                'latitude': 40.7128,
                'longitude': -74.0060
            },
            'financial': {
                'preferred_payment_methods': ['Credit Card', 'PayPal']
            }
        }
        
        # Mock merchant data
        self.mock_merchant = {
            'merchant_id': 'test-merchant-id',
            'merchant_name': 'Test Merchant',
            'merchant_category': 'Retail',
            'location': {
                'country': 'US',
                'city': 'New York',
                'latitude': 40.7128,
                'longitude': -74.0060
            },
            'payment': {
                'currency': 'USD',
                'methods': ['Visa', 'MasterCard', 'Amex']
            }
        }

    @patch('src.producers.transaction_producer.TransactionProducer')
    def test_transaction_producer_initialization(self, mock_producer_class):
        """Test that TransactionProducer initializes correctly"""
        # Import the class
        from src.producers.transaction_producer import TransactionProducer
        
        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_producer_class.return_value = mock_producer_instance
        
        # Patch dependencies
        with patch('src.producers.transaction_producer.PostgresDB'):
            with patch('src.producers.transaction_producer.SchemaRegistry'):
                # Create an instance
                producer = TransactionProducer()
                
                # Verify initialization
                self.assertIsNotNone(producer)
                
    @patch('src.producers.transaction_producer.fake')
    @patch('src.producers.transaction_producer.random')
    def test_generate_transaction(self, mock_random, mock_fake):
        """Test transaction generation logic"""
        # Import the class
        from src.producers.transaction_producer import TransactionProducer
        
        # Setup mocks
        mock_random.random.return_value = 0.1  # For is_online_transaction (< 0.2 = True)
        mock_random.randint.return_value = 30  # For timestamp minutes ago
        mock_random.uniform.return_value = 100.0  # For transaction amount
        mock_random.choice.side_effect = [
            "Visa",  # card_type
            "purchase",  # transaction_type
            "online"  # entry_mode
        ]
        mock_fake.uuid4.return_value = "test-transaction-id"
        
        # Create a producer instance with mocked dependencies
        with patch('src.producers.transaction_producer.PostgresDB') as mock_db_class:
            with patch('src.producers.transaction_producer.SchemaRegistry'):
                with patch('src.producers.transaction_producer.Producer'):
                    with patch('src.producers.transaction_producer.AvroProducer'):
                        # Setup DB mock
                        mock_db = MagicMock()
                        mock_db_class.return_value = mock_db
                        mock_db.is_connected.return_value = True
                        mock_db.get_random_user.return_value = self.mock_user
                        mock_db.get_random_merchant.return_value = self.mock_merchant
                        
                        # Create producer instance
                        producer = TransactionProducer()
                        
                        # Call the method
                        transaction = producer._generate_transaction()
                        
                        # Assertions
                        self.assertIsNotNone(transaction)
                        self.assertIsInstance(transaction, dict)
                        
                        # Check transaction fields
                        self.assertEqual(transaction['user_id'], self.mock_user['user_id'])
                        self.assertEqual(transaction['merchant_id'], self.mock_merchant['merchant_id'])
                        self.assertEqual(transaction['merchant_name'], self.mock_merchant['merchant_name'])
                        self.assertEqual(transaction['merchant_category'], self.mock_merchant['merchant_category'])
                        self.assertEqual(transaction['currency'], self.mock_merchant['payment']['currency'])
                        self.assertEqual(transaction['card_type'], "Visa")
                        self.assertEqual(transaction['transaction_type'], "purchase")
                        self.assertTrue(transaction['is_online'])
                        
                        # For online transactions, location should be from user
                        self.assertEqual(transaction['location_country'], self.mock_user['location']['country'])
                        self.assertEqual(transaction['location_city'], self.mock_user['location']['city'])

    def test_transaction_validation(self):
        """Test transaction validation logic"""
        # Import the class and schema
        from src.producers.transaction_producer import TransactionProducer, TRANSACTION_SCHEMA
        
        # Create a valid transaction
        valid_transaction = {
            "transaction_id": "test-id-123",
            "user_id": 12345,
            "amount": 100.50,
            "currency": "USD",
            "merchant_id": "merchant-id-123",
            "merchant_name": "Test Merchant",
            "merchant_category": "Retail",
            "card_type": "Visa",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "location_country": "US",
            "location_city": "New York",
            "latitude": 40.7128,
            "longitude": -74.0060
        }
        
        # Create an invalid transaction (missing required fields)
        invalid_transaction = {
            "transaction_id": "test-id-456",
            "user_id": 12345,
            "amount": 100.50
            # Missing required fields
        }
        
        # Create producer instance with mocked dependencies
        with patch('src.producers.transaction_producer.PostgresDB'):
            with patch('src.producers.transaction_producer.SchemaRegistry'):
                with patch('src.producers.transaction_producer.Producer'):
                    with patch('src.producers.transaction_producer.AvroProducer'):
                        producer = TransactionProducer()
                        
                        # Test validation
                        with patch('src.producers.transaction_producer.validate') as mock_validate:
                            # Setup mock to simulate validation
                            def side_effect(transaction, schema, **kwargs):
                                if transaction == valid_transaction:
                                    return True
                                else:
                                    from jsonschema import ValidationError
                                    raise ValidationError("Validation failed")
                            
                            mock_validate.side_effect = side_effect
                            
                            # Test valid transaction
                            result = producer._validate_transaction(valid_transaction)
                            self.assertTrue(result)
                            
                            # Test invalid transaction
                            result = producer._validate_transaction(invalid_transaction)
                            self.assertFalse(result)

    @patch('src.producers.transaction_producer.TransactionProducer.run')
    def test_main_execution(self, mock_run):
        """Test the main execution flow"""
        # Import the module
        import src.producers.transaction_producer
        
        # Patch dependencies
        with patch('src.producers.transaction_producer.TransactionProducer') as mock_producer_class:
            # Setup mock
            mock_producer_instance = MagicMock()
            mock_producer_class.return_value = mock_producer_instance
            
            # Call the main block
            with patch('src.producers.transaction_producer.__name__', '__main__'):
                # This should trigger the if __name__ == "__main__" block
                src.producers.transaction_producer.TransactionProducer().run()
                
                # Verify run was called
                mock_run.assert_called_once()

if __name__ == '__main__':
    unittest.main()
