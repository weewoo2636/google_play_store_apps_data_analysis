
# Google Play Store Apps Data Analysis 

This project was made to help a fictional app development studio to familiarize themselves with the Google Play Store platfrom, by analyzing the data of apps on the platform. The project also includes ETL before the data analysis.


## Background

The Google Play Store hosts millions of apps, and understanding user reviews, ratings, and performance metrics is critical for developers and businesses. However, the vast amount of data makes manual analysis inefficient. This project is motivated by:

- App Developers: To gain insights into how users perceive their apps and identify areas for improvement.
- Market Trends: To analyze market competition, identify trending apps, and determine which factors contribute to app success.
- Business Analysts: To track app performance over time and uncover opportunities for marketing and user retention strategies.


## Acknowledgements

- [Dataset](https://www.kaggle.com/datasets/bhavikjikadara/google-play-store-applications)    


## Methods & Tech Stack

- **ETL:** Python, Airflow, ElasticSearch, Postgres

- **Data Monitoring/Visualization:** Kibana

- **Data Validation:** GreatExpectation (Python)


## Workflow
Problem Identification -> Data Understanding -> Data Preparation -> ETL -> EDA -> Conclusion   

  
## Output/Screenshots

![ss_1](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/introduction%20&%20objective.png?raw=true)

![ss_2](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/plot%20&%20insight%2001.png?raw=true)

![ss_3](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/plot%20&%20insight%2002.png?raw=true)

![ss_4](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/plot%20&%20insight%2003.png?raw=true)

![ss5](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/plot%20&%20insight%2004.png?raw=true)

![ss6](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/plot%20&%20insight%2005.png?raw=true)

![ss7](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/plot%20&%20insight%2006.png?raw=true)

![ss8](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/plot%20&%20insight%2007.png?raw=true)

![ss9](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/plot%20&%20insight%2008.png?raw=true)

![ss10](https://github.com/weewoo2636/google_play_store_apps_data_analysis/blob/main/images/kesimpulan.png?raw=true)


## Files Overview

1. **`DAG.py`**: Contains the Directed Acyclic Graph (DAG) used by Apache Airflow to orchestrate and schedule the Extract, Transform, Load (ETL) pipeline, handling data flow and task dependencies for automating the process.

2. **`GX.ipynb`**: A Jupyter Notebook utilizing the Great Expectations (GX) framework for validating data quality. It ensures that the raw data conforms to specific criteria before loading it into Elasticsearch.

3. **`data_raw.csv`**: The raw dataset containing unprocessed Google Play Store app data, serving as the input for the ETL pipeline.

4. **`data_clean.csv`**: A cleaned version of the dataset, produced after transforming and preprocessing the raw data to make it ready for analysis and loading into Elasticsearch.

5. **`ddl.txt.txt`**: This file contains Data Definition Language (DDL) statements, defining the schema and structure of the PostgreSQL tables used for storing and managing the Google Play Store apps' data.

6. **`images/`**: A directory containing images, such as screenshots or visualizations, showing the final Kibana dashboards or insights derived from the analysis.

7. **`README.md`**: The main documentation file for the project. It provides an overview of the project, detailing the ETL pipeline from PostgreSQL to Elasticsearch, and instructions for running the analysis and visualizations using Kibana. It also includes the project’s objectives, technology stack, and setup instructions. 


## Authors

- [@weewoo2636](https://www.github.com/weewoo2636)

