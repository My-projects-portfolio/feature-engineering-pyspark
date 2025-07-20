
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Ensure assets folder exists
os.makedirs("assets", exist_ok=True)

# Load the Parquet output from Spark
df = pd.read_parquet("data/loan_features")

# Print sample data
print("Sample engineered data:")
print(df.head())

# -------------------------------
# 📊 1. Histogram of Average Loan Amount
plt.figure(figsize=(8, 5))
plt.hist(df['avg_amount'], bins=30, color='skyblue', edgecolor='black')
plt.title("Distribution of Average Loan Amount")
plt.xlabel("Average Loan Amount")
plt.ylabel("Number of Customers")
plt.grid(True)
plt.tight_layout()
plt.savefig("assets/avg_loan_amount.png")
plt.show()

# -------------------------------
# 📊 2. Scatter plot: Loan-to-Income Ratio vs Average Loan Amount
plt.figure(figsize=(8, 5))
plt.scatter(df['avg_loan_to_income'], df['avg_amount'], alpha=0.6)
plt.title("Loan-to-Income Ratio vs Average Loan Amount")
plt.xlabel("Average Loan-to-Income Ratio")
plt.ylabel("Average Loan Amount")
plt.grid(True)
plt.tight_layout()
plt.savefig("assets/loan_vs_income.png")
plt.show()

# -------------------------------
# 📊 3. Bar chart: Top 10 Customers by Number of Loans
top_customers = df.sort_values(by="num_loans", ascending=False).head(10)
plt.figure(figsize=(8, 5))
plt.bar(top_customers["customer_ID"].astype(str), top_customers["num_loans"], color='orange')
plt.title("Top 10 Customers by Number of Loans")
plt.xlabel("Customer ID")
plt.ylabel("Number of Loans")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("assets/top_customers.png")
plt.show()

# -------------------------------
# 📊 4. Default Rate Distribution
plt.figure(figsize=(8, 5))
plt.hist(df["default_rate"], bins=20, color="crimson", edgecolor="black")
plt.title("Distribution of Default Rate")
plt.xlabel("Average Default Rate per Customer")
plt.ylabel("Number of Customers")
plt.grid(True)
plt.tight_layout()
plt.savefig("assets/default_rate.png")
plt.show()

# -------------------------------
# 📊 5. Correlation Heatmap
numeric_cols = ["avg_amount", "avg_fee", "avg_loan_to_income", "default_rate"]
corr_matrix = df[numeric_cols].corr()

plt.figure(figsize=(6, 5))
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Heatmap of Engineered Features")
plt.tight_layout()
plt.savefig("assets/correlation_heatmap.png")
plt.show()

# -------------------------------
# 📊 6. Group Customers by Risk Level
df["risk_level"] = df["default_rate"].apply(lambda x: "high_risk" if x >= 0.5 else "low_risk")

risk_counts = df["risk_level"].value_counts()

plt.figure(figsize=(6, 6))
plt.pie(risk_counts, labels=risk_counts.index, autopct="%1.1f%%", startangle=90, colors=["red", "green"])
plt.title("Customer Risk Levels Based on Default Rate")
plt.tight_layout()
plt.savefig("assets/risk_pie.png")
plt.show()
