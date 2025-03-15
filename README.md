# **Cloud-Driven Financial Insights: An Integrated Data Pipeline**

![ba882_team9](https://github.com/user-attachments/assets/9ceb29a6-9bcd-4c0d-983a-b44af1b9762e)

**Group Members: Unice(Yu-Fang) Liao, Honglin Jiang, Yuchen Li, Raiymbek Ordabayev**
# SEC 10-K Filing & Financial Data Pipeline and ML Models

## Overview

This project involves the development and deployment of an ETL pipeline, real-time price prediction models, and a web application to improve financial data accessibility and forecasting for portfolio management. The solution leverages multiple technologies including Google Cloud Functions, Prefect for pipeline orchestration, machine learning models (Logit, XGBoost), and Streamlit for building an interactive web application.

### Key Features:
- **ETL Pipeline** for SEC 10-K filing and stock pricing data.
- **Real-time Price Prediction** using Logit and XGBoost models.
- **Tableau Dashboard** for daily market summaries and long-term trends.
- **Streamlit Web App** for querying SEC 10-K filings with natural language via GenAI-powered RAG and text-to-SQL functionality.

## Technologies Used
- **Google Cloud Platform** (GCP) for deploying cloud functions and managing services.
- **SEC API** for retrieving 10-K filings.
- **Prefect** for pipeline orchestration.
- **PowerBI** for building interactive dashboards.
- **XGBoost** and **Logit** models for machine learning predictions.
- **Streamlit** for building a web application.
- **GenAI** for enhancing the querying process with RAG and text-to-SQL features.

## Data Overview

### 1. SEC 10-K Filings:
The project ingests SEC 10-K filings for company financial data, which are retrieved using the SEC API. This data is essential for understanding a company’s financial health and is processed through an ETL pipeline for analysis.

### 2. Stock Pricing Data:
Historical pricing data is fetched using APIs from relevant sources. This data is then transformed and aggregated for use in model training and dashboard visualizations.

## Pipeline Architecture

### ETL Process:
- **Extraction:** Data is fetched using the SEC API for 10-K filings and pricing data from various financial sources.
- **Transformation:** The data undergoes cleaning, normalization, and transformation into a usable format.
- **Load:** Processed data is loaded into a cloud database (e.g., Google Cloud SQL) for reporting and analytics.

The entire ETL process is orchestrated using **Prefect**, which ensures data is available on time and can be monitored for successful execution.

### Machine Learning Models:
- **Logit and XGBoost Models**: These models are trained on historical stock data and financial information from the 10-K filings to predict future stock prices in real-time. The models are integrated into a GCP-deployed pipeline, allowing for fast predictions.
- **31% Improvement**: The integration of XGBoost achieved a 31% improvement in benchmark accuracy compared to the previous model.

## How It Works

1. **ETL Pipeline**: 
   - The pipeline fetches data from the SEC API and external financial sources, processes it, and loads it into a cloud-based database for reporting and analysis.
   
2. **Machine Learning Models**: 
   - The models are trained on historical pricing and financial data. The trained models are deployed in the cloud for real-time price prediction.

3. **Tableau Dashboard**: 
   - The dashboard pulls data from the cloud database and displays various key performance indicators (KPIs) and stock trends in an interactive format.

4. **Streamlit Web App**: 
   - Users can interact with 10-K filings using natural language queries, powered by GenAI for text-to-SQL translation.


---


## **1. Links**
### **PowerBI Dashboard:** 
[Click Here](https://app.powerbi.com/view?r=eyJrIjoiMjg2ZWViYjQtNWM5YS00MWNlLWJhNDgtZTcyOWQxYjg3ZjYwIiwidCI6IjAxZjNkMGExLTNkYzgtNDBkNy04YjUyLTI0YjM4YmEzM2JjYiIsImMiOjl9)

<img width="1308" alt="Screenshot 2024-12-10 at 5 00 58 PM" src="https://github.com/user-attachments/assets/43a0d3cb-60c6-4478-a45a-b744c29065c6">
A **Tableau dashboard** was developed to provide stakeholders with:
- **Daily Market Summaries**: Quick overviews of portfolio stock movements.
- **Long-Term Trends**: Visualizations of stock performance over time.
- **Time Series Predictions**: Forecasts of future stock prices based on the machine learning model's output.

This dashboard enables stakeholders to make data-driven investment decisions by visualizing trends and potential future outcomes.

### **Streamlit Apps:** 
[Click Here - 1](https://streamlit-genai-apps-1076180164120.us-central1.run.app/)
[Click Here - 2](https://streamlit-rag-app-1076180164120.us-central1.run.app/)
A **Streamlit web app** was developed to allow stakeholders to query SEC 10-K filings using natural language. The app integrates with **GenAI-powered RAG** and **text-to-SQL** functionality, allowing users to ask questions such as:
- "What is the revenue for the last fiscal year?"
- "Show me the total liabilities and assets."

This approach makes the 10-K filings accessible to non-technical stakeholders, improving their ability to make informed decisions.
---


## **2. ERD**

![image](https://github.com/user-attachments/assets/80be583f-b024-4b3e-a7fe-479f682796f8)


---
## **3. Demo**
### **Streamlit App - Text-to-SQL with Google Cloud Vertex AI:**
<img width="1048" alt="Screenshot 2024-12-10 at 5 09 23 PM" src="https://github.com/user-attachments/assets/3ecd7309-46e6-469f-a3d6-e83780aedf1b">
<img width="1357" alt="Screenshot 2024-12-10 at 5 10 00 PM" src="https://github.com/user-attachments/assets/64361cca-db31-4615-a843-622ee9433227">

### **Streamlit App - Yearly MD&A Sentiment-Driven Stock Insights:**

<img width="1483" alt="Screenshot 2024-12-10 at 5 17 15 PM" src="https://github.com/user-attachments/assets/6bcc7d71-4e25-4a9a-8a3d-d027285b5f91">



### **Streamlit App - Daily News Sentiment and Stock Analysis:**
<img width="1458" alt="Screenshot 2024-12-10 at 5 18 07 PM" src="https://github.com/user-attachments/assets/b8ab4782-bb27-46c6-9c66-d28cc1589221">
<img width="1467" alt="Screenshot 2024-12-10 at 5 18 19 PM" src="https://github.com/user-attachments/assets/ce5db086-d755-429a-a4f5-d87fd512dfe0">



### **Streamlit App - News Topic Modeling and Sentiment Analysis:**
<img width="1491" alt="Screenshot 2024-12-10 at 5 20 19 PM" src="https://github.com/user-attachments/assets/52ed6988-adeb-4113-9f79-ba41a8693f1e">
<img width="1503" alt="Screenshot 2024-12-10 at 5 20 38 PM" src="https://github.com/user-attachments/assets/b5df3a48-f327-40e1-b85f-2f32a1a19378">



### **Streamlit App - RAG for MD&A**
<img width="1421" alt="Screenshot 2024-12-10 at 5 20 59 PM" src="https://github.com/user-attachments/assets/b3cf7b09-6de7-4ba3-ac9f-58d3e146b375">



