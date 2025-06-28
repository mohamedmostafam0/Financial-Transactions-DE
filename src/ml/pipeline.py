import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score, precision_recall_curve, auc
from xgboost import XGBClassifier
from imblearn.over_sampling import SMOTE
from google.cloud import bigquery
import joblib

client = bigquery.Client()

class FraudDetectionPipeline:
    def __init__(self):
        self.models = {
            'rf': RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                class_weight='balanced',
                random_state=42
            ),
            'gb': GradientBoostingClassifier(
                n_estimators=100,
                learning_rate=0.1,
                random_state=42
            ),
            'xgb': XGBClassifier(
                objective='binary:logistic',
                scale_pos_weight=10,  # Adjust for imbalance
                random_state=42
            )
        }
        self.final_model = None
        self.feature_importances = None
        self.features = [
            'amount', 'use_chip', 'has_chip', 'credit_limit',
            'current_age', 'distance_to_merchant',
            'transaction_hour', 'transaction_day_of_week',
            'unemployment_rate', 'unemployment_change_rate'
        ]

    def load_data(self):
        """Load data from BigQuery"""
        query = """
        SELECT
            {', '.join(self.features)},
            is_fraud
        FROM `your-project.your-dataset.ml_features`
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        """
        df = client.query(query).to_dataframe()
        return df

    def prepare_data(self, df):
        """Prepare data for training"""
        X = df[self.features]
        y = df['is_fraud']
        
        # Convert categorical variables
        X['use_chip'] = X['use_chip'].astype(int)
        X['has_chip'] = X['has_chip'].astype(int)
        
        # Scale numerical features
        numerical_features = ['amount', 'credit_limit', 'distance_to_merchant',
                            'unemployment_rate', 'unemployment_change_rate']
        X[numerical_features] = (X[numerical_features] - 
                               X[numerical_features].mean()) / 
                               X[numerical_features].std()
        
        return X, y

    def train(self):
        """Train the fraud detection model"""
        df = self.load_data()
        X, y = self.prepare_data(df)
        
        # Handle class imbalance
        smote = SMOTE(random_state=42)
        X_resampled, y_resampled = smote.fit_resample(X, y)
        
        # Evaluate each model
        results = {}
        for name, model in self.models.items():
            scores = cross_val_score(
                model, X_resampled, y_resampled,
                scoring='f1', cv=5
            )
            results[name] = {
                'mean_f1': np.mean(scores),
                'std_f1': np.std(scores)
            }
        
        # Select best model based on F1 score
        best_model_name = max(results, key=lambda k: results[k]['mean_f1'])
        self.final_model = self.models[best_model_name]
        
        # Train final model
        self.final_model.fit(X_resampled, y_resampled)
        
        # Get feature importances
        self.feature_importances = pd.DataFrame({
            'feature': self.features,
            'importance': self.final_model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        # Evaluate final model
        y_pred = self.final_model.predict(X)
        y_pred_proba = self.final_model.predict_proba(X)[:, 1]
        
        # Calculate precision-recall curve
        precision, recall, _ = precision_recall_curve(y, y_pred_proba)
        pr_auc = auc(recall, precision)
        
        print("\nFinal Model Performance:")
        print(f"Best Model: {best_model_name}")
        print(classification_report(y, y_pred))
        print(f"PR AUC Score: {pr_auc:.4f}")
        print("\nTop Features:")
        print(self.feature_importances.head())
        
        # Save model
        joblib.dump({
            'model': self.final_model,
            'feature_importances': self.feature_importances,
            'best_model': best_model_name
        }, 'fraud_detection_model.pkl')

    def predict(self, transaction_data):
        """Predict fraud probability for new transaction"""
        if self.final_model is None:
            model_data = joblib.load('fraud_detection_model.pkl')
            self.final_model = model_data['model']
        
        df = pd.DataFrame([transaction_data])
        X = df[self.features]
        
        proba = self.final_model.predict_proba(X)[0][1]
        prediction = {
            'fraud_probability': float(proba),
            'is_fraud': proba > 0.5,
            'confidence': float(abs(proba - 0.5) * 2),
            'model_used': model_data['best_model'],
            'top_features': model_data['feature_importances'].head(5).to_dict()
        }
        
        return prediction

if __name__ == '__main__':
    pipeline = FraudDetectionPipeline()
    pipeline.train()
