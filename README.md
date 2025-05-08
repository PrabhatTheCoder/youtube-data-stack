# 📊 Data Engineering YouTube Analysis Project

**Author:** Prabhat Kumar

## 🧭 Overview

This project focuses on ingesting, transforming, and analyzing structured and semi-structured YouTube trending video data. It is designed to be scalable, secure, cloud-native, and optimized for business intelligence insights based on video categories and trending metrics.

## 🎯 Project Goals

- **Data Ingestion**: Ingest data from multiple sources efficiently.
- **ETL System**: Transform raw data into clean, queryable formats.
- **Data Lake**: Centralize all ingested data using Amazon S3.
- **Scalability**: Ensure the system can handle increasing data volume.
- **Cloud Deployment**: Leverage AWS for distributed and serverless data processing.
- **Reporting**: Visualize insights via dashboards to answer key analytical questions.

## 🛠️ AWS Services Used

| Service        | Description                                                                 |
|----------------|-----------------------------------------------------------------------------|
| **Amazon S3**  | Centralized data lake for scalable object storage.                         |
| **AWS IAM**    | Secure access management across AWS resources.                             |
| **AWS Glue**   | Serverless ETL and data cataloging service.                                |
| **AWS Lambda** | Serverless compute to trigger ETL or events.                               |
| **Amazon Athena** | Serverless SQL queries directly on S3 data.                           |
| **Amazon QuickSight** | Cloud-native BI tool for creating interactive dashboards.     |

## 📂 Dataset

📎 [YouTube Trending Dataset on Kaggle](https://www.kaggle.com/datasets/datasnaek/youtube-new)

## 📌 Future Enhancements

- Add real-time data streaming using Kafka.
- Implement Airflow for pipeline orchestration.
- Enable automated testing and CI/CD pipelines.

---

✅ Built with AWS and passion for scalable data solutions.
