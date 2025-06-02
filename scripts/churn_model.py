import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import os

# Load RFM data
rfm = pd.read_csv("data/customer_segments.csv")

# Step 1: Simulate churn label (Recency > 90 days = churn)
rfm['churn'] = rfm['Recency'].apply(lambda x: 1 if x > 90 else 0)

# Debug: print label distribution
print("🧪 Churn distribution:\n", rfm['churn'].value_counts())

# Step 2: Features and target
X = rfm[['Recency', 'Frequency', 'Monetary']]
y = rfm['churn']

# Step 3: Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

# Step 4: Train the model
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)

# Step 5: Predict
y_pred = clf.predict(X_test)

# Safe probability prediction
if clf.n_classes_ > 1:
    rfm['churn_probability'] = clf.predict_proba(X)[:, 1]
else:
    rfm['churn_probability'] = 0.0

rfm['predicted_churn'] = clf.predict(X)

# Step 6: Classification report
print("\n📊 Classification Report:\n")
print(classification_report(y_test, y_pred))

# Step 7: Export
os.makedirs('data', exist_ok=True)
rfm[['customer_id', 'Recency', 'Frequency', 'Monetary', 'churn_probability', 'predicted_churn']].to_csv(
    "data/churn_predictions.csv", index=False
)

print("✅ Churn prediction complete. Output saved to 'data/churn_predictions.csv'")
