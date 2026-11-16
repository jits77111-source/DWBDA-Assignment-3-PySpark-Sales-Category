# DWBDA Assignment 3: Apache Spark – Products Sold in Each Category

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/jits77111-source/DWBDA-Assignment-3-PySpark-Sales-Category/blob/main/Assignment_3_PySpark_Sales_Category.ipynb)

**Author:** Jit Sarkar  
**Course:** Data Warehousing, Big Data & Analytics  
**Implementation:** Apache Spark (PySpark DataFrame API & Spark SQL)  

---

## 📌 Problem Overview
Demonstrates distributed batch data analytics using **Apache Spark** to aggregate sales records and compute:
1. Total quantity of products sold per category.
2. Total sales revenue and average unit prices across retail categories.
3. Declarative Spark SQL queries.

---

## 📂 Repository Contents
```
DWBDA-Assignment-3-PySpark-Sales-Category/
├── Assignment_3_PySpark_Sales_Category.ipynb   # Interactive Jupyter Notebook (Colab Ready)
├── assignment_3_spark_sales.py                 # Standalone executable PySpark script
├── retail_store_sales.csv                      # Retail transaction dataset
├── spark_category_sales_summary.csv            # Output summary table
├── .gitignore                                  # Git ignore file
└── README.md                                   # Documentation
```

---

## 🚀 How to Run

### Method 1: 1-Click Run in Google Colab (Recommended)
Click the badge above or visit:  
👉 [Open in Google Colab](https://colab.research.google.com/github/jits77111-source/DWBDA-Assignment-3-PySpark-Sales-Category/blob/main/Assignment_3_PySpark_Sales_Category.ipynb)

### Method 2: Run Locally
```bash
# 1. Clone repository
git clone https://github.com/jits77111-source/DWBDA-Assignment-3-PySpark-Sales-Category.git
cd DWBDA-Assignment-3-PySpark-Sales-Category

# 2. Install dependencies
pip install pyspark pandas matplotlib

# 3. Execute script
python assignment_3_spark_sales.py
```
