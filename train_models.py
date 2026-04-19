import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error, accuracy_score, f1_score, confusion_matrix
import joblib
import os
import datetime

# ==================================================
# 1. LOAD DATASET
# ==================================================
def load_and_analyze(file_path):
    print(f"--- Loading Dataset: {file_path} ---")
    encodings = ['utf-8', 'latin1', 'cp1252']
    df = None
    for enc in encodings:
        try:
            df = pd.read_csv(file_path, sep=None, engine='python', encoding=enc)
            print(f"Successfully loaded with {enc} encoding.")
            break
        except Exception as e:
            continue
    
    if df is None:
        raise Exception("Failed to load dataset with multiple encodings.")

    print(f"Total Rows: {len(df)}")
    print(f"Total Columns: {len(df.columns)}")
    print(f"Columns: {list(df.columns)}")
    print("\nMissing Values Report:")
    print(df.isnull().sum())
    return df

# ==================================================
# 2. AUTO ANALYZE & MAPPING
# ==================================================
def intelligent_mapping(df):
    # Mapping of target project names to possible dataset synonyms
    synonyms = {
        'ph': ['ph'],
        'temperature': ['temprature', 'temp', 'temperature'],
        'fat': ['fat', 'fat_percent', 'fat '],
        'quality': ['grade', 'quality', 'class', 'label'],
        'water_percent': ['water', 'water_percent', 'adulteration'],
        'litres': ['litres', 'quantity', 'milk_qty', 'volume'],
        'date': ['date', 'timestamp', 'time']
    }
    
    new_cols = {}
    for col in df.columns:
        clean_col = col.strip().lower()
        for target, syn_list in synonyms.items():
            if any(syn in clean_col for syn in syn_list):
                new_cols[col] = target
                break
    
    df = df.rename(columns=new_cols)
    print(f"Mapped Columns: {new_cols}")
    
    # Check for missing columns and augment if necessary
    required = ['ph', 'temperature', 'fat', 'quality']
    for col in required:
        if col not in df.columns:
            print(f"Warning: {col} missing. Creating synthetic {col}.")
            if col == 'ph': df['ph'] = np.random.uniform(6.4, 6.8, size=len(df))
            elif col == 'temperature': df['temperature'] = np.random.uniform(34.0, 45.0, size=len(df))
            elif col == 'fat': df['fat'] = np.random.uniform(2.5, 6.0, size=len(df))
            elif col == 'quality': df['quality'] = np.random.choice(['low', 'medium', 'high'], size=len(df))
    
    if 'litres' not in df.columns:
        print("Adding synthetic 'litres' column for yield prediction demo.")
        df['litres'] = np.random.uniform(2.0, 10.0, size=len(df))
    
    if 'date' not in df.columns:
        print("Adding synthetic 'date' sequence.")
        base = datetime.date.today()
        df['date'] = [base - datetime.timedelta(days=x) for x in range(len(df))]
        df['date'] = pd.to_datetime(df['date'])
    else:
        df['date'] = pd.to_datetime(df['date'])

    return df

# ==================================================
# 3. DATA CLEANING
# ==================================================
def clean_data(df):
    # Strip spaces in column names
    df.columns = df.columns.str.strip()
    
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Handle Nulls
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = df[col].fillna(df[col].mode()[0])
            
    return df

# ==================================================
# 4. FEATURE ENGINEERING
# ==================================================
def feature_engineering(df):
    # Time features
    df['day'] = df['date'].dt.day
    df['month'] = df['date'].dt.month
    df['weekday'] = df['date'].dt.weekday
    
    # Shifted target for yield prediction (Predict next day)
    df = df.sort_values('date')
    df['next_day_litres'] = df['litres'].shift(-1)
    
    # Rolling average
    df['avg_previous_litres'] = df['litres'].rolling(window=3).mean()
    
    # Categorical encoding for Quality
    if 'quality' in df.columns:
        df['quality_numeric'] = df['quality'].map({'low': 0, 'medium': 1, 'high': 2})
    
    df = df.dropna() # Drop rows where shift created nulls
    return df

# ==================================================
# 5. TRAINING & EVALUATION
# ==================================================
def train_pipeline(df):
    # Split
    train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
    
    # --- A) MILK YIELD PREDICTION ---
    features_y = ['litres', 'fat', 'ph', 'temperature']
    # Add other features if they exist
    features_y = [f for f in features_y if f in df.columns]
    
    X_train_y, y_train_y = train_df[features_y], train_df['next_day_litres']
    X_test_y, y_test_y = test_df[features_y], test_df['next_day_litres']
    
    yield_model = RandomForestRegressor(n_estimators=100, random_state=42)
    yield_model.fit(X_train_y, y_train_y)
    
    y_pred_y = yield_model.predict(X_test_y)
    print("\n--- Yield Prediction Metrics ---")
    print(f"R2 Score: {r2_score(y_test_y, y_pred_y):.4f}")
    print(f"RMSE: {np.sqrt(mean_squared_error(y_test_y, y_pred_y)):.4f}")
    
    # --- B) QUALITY CLASSIFICATION ---
    X_train_q = train_df[features_y]
    y_train_q = train_df['quality']
    X_test_q = test_df[features_y]
    y_test_q = test_df['quality']
    
    quality_model = RandomForestClassifier(n_estimators=100, random_state=42)
    quality_model.fit(X_train_q, y_train_q)
    
    y_pred_q = quality_model.predict(X_test_q)
    print("\n--- Quality Classification Metrics ---")
    print(f"Accuracy: {accuracy_score(y_test_q, y_pred_q):.4f}")
    
    # --- C) FRAUD DETECTION (Unsupervised) ---
    fraud_model = IsolationForest(contamination=0.05, random_state=42)
    fraud_model.fit(df[features_y])
    
    # Save Models
    joblib.dump(yield_model, 'yield_model.pkl')
    joblib.dump(quality_model, 'quality_model.pkl')
    joblib.dump(fraud_model, 'fraud_model.pkl')
    print("\nModels saved: yield_model.pkl, quality_model.pkl, fraud_model.pkl")
    
    return yield_model, quality_model, fraud_model, features_y

# ==================================================
# 6. VISUALIZATION
# ==================================================
def save_visualizations(df, yield_model, features):
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df.tail(30), x='date', y='litres')
    plt.title('Milk Yield Trend (Last 30 records)')
    plt.savefig('yield_trend.png')
    
    plt.figure(figsize=(8, 5))
    sns.countplot(data=df, x='quality')
    plt.title('Quality Distribution')
    plt.savefig('quality_dist.png')
    
    # Feature Importance
    importances = yield_model.feature_importances_
    plt.figure(figsize=(10, 6))
    sns.barplot(x=importances, y=features)
    plt.title('Feature Importance for Yield')
    plt.savefig('feature_importance.png')
    print("Charts saved: yield_trend.png, quality_dist.png, feature_importance.png")

# ==================================================
# MAIN EXECUTION
# ==================================================
def main():
    file_path = 'milknew.csv'
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return
        
    df = load_and_analyze(file_path)
    df = intelligent_mapping(df)
    df = clean_data(df)
    df = feature_engineering(df)
    
    yield_model, quality_model, fraud_model, features = train_pipeline(df)
    save_visualizations(df, yield_model, features)
    
    # Prediction Demo
    sample = df.iloc[-1][features].values.reshape(1, -1)
    print("\n--- Prediction Demo ---")
    print(f"Input: {dict(df.iloc[-1][features])}")
    print(f"Predicted Tomorrow Litres: {yield_model.predict(sample)[0]:.2f}")
    print(f"Predicted Quality: {quality_model.predict(sample)[0]}")
    
    print("\n--- ML Pipeline Complete ---")

if __name__ == "__main__":
    main()
