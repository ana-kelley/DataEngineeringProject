

## **Real-Time Traffic Incident Visualization Using TomTom API**
**Author**: Anastasia Kelley | ECE 5984 Data Engineering Project

### **Project’s Function**

This project builds a data pipeline to collect, process into a structured format, and visualize real-time traffic incidents using the TomTom Traffic API. It provides actionable insights into traffic conditions through Tableau dashboards, handling real-time updates, as well as helping users analyze incident trends and manage traffic effectively.

---
### **Dataset**

The data is sourced from the TomTom Traffic API, which provides detailed incident reports such as accidents, road closures, and traffic congestion. Each dataset entry contains information on incident type, location, timestamps, severity, and more. The data is ingested as JSON files and processed into structured `.pkl` files for downstream use.

---

### **Pipeline / Architecture**

The pipeline uses Python and Kafka to collect raw JSON data, which is saved in S3 (`s3/raw/`), transforms it into structured formats in S3 (`s3/transformed/`), and loads it into a MySQL database. Only newly added files in S3 are processed. Historical data is preserved, and active events are updated to ensure real-time relevance. Data is visualized using Tableau dashboards.

###### Tools used:
- **[TomTom Traffic API](https://developer.tomtom.com/traffic-api/api-explorer)**: Provides real-time traffic incident data.
- **[Apache Kafka](https://kafka.apache.org/)**: Streams real-time data for processing.
- **[AWS S3](https://aws.amazon.com/s3/)**: Stores raw JSON files (`/raw/`) and transformed `.pkl` files (`/transformed/`).
- **[Python](https://www.python.org/)**: Manages data ingestion, transformation, and loading.
- **Pandas**: Transforms raw data into structured tabular formats.
- **[SQLAlchemy](https://www.sqlalchemy.org/)**: Connects and loads data into MySQL.
- **[MySQL](https://www.mysql.com/)**: Stores traffic data for analysis.
- **[AWS RDS](https://aws.amazon.com/rds/)**: Hosts the MySQL database.
- **[Tableau](https://www.tableau.com/)**: Visualizes traffic data in dashboards.
- **[Airflow](https://airflow.apache.org/)**: Automates and orchestrates pipeline workflows.

---

### **Data Quality Assessment**

The pipeline ensures data completeness by handling missing fields and flattening nested JSON structures during transformation. Deduplication is performed at the database level to ensure unique records. Data timeliness is maintained by appending only new records, with processed files tracked in logs. Data quality was assessed through schema validation and inspection of transformed samples.

---
### **Data Provenance & Transformation**

#### **Data Provenance**

The pipeline ensures that each stage of data ingestion, processing, and storage is logged and traceable. Key steps include:

1. Raw data originates from the TomTom Traffic API, providing incident reports like accidents and road closures.
2. Each API call retrieves "present" incidents, which are stored in S3 under the `raw/` folder for historical reference and analysis.
3. A processed log in S3 ensures only new files are transformed and loaded into the MySQL database.

#### **Data Transformation Models Used**

The pipeline processes raw nested JSON from the TomTom API into structured formats for analysis and storage. Data models:

* **Raw** - Nested JSON with fields like id, coordinates, and events.
* **Transformed** - Flattened data with extracted longitude, latitude, and event details, along with derived time-based fields (start_date, start_hour). Stored as .pkl files in S3.
* **Database** -  Data in MySQL tracks "present" incidents and marks missing ones as "inactive," enabling real-time and historical analysis.

**Special Instructions:** Execution requires signing up for a TomTom Developer account in order to obtain the API key. Additionally, AWS S3 credentials may need to be configured.

---

### **Infographic**
#### Pipeline architecture:
![Pipeline architecture](https://github.com/user-attachments/assets/d7f0fd19-ffc1-4217-b964-8689b78958a1)

#### Final Result:
![Tableau Project Screenshot](https://github.com/user-attachments/assets/c6b10a44-4be8-4a67-84de-687432091d13)

This visualization demonstrates the pipeline's results by identifying **current incident locations**, their **severity based on delay and reports**, and the **trend of incidents over time**. The pipeline automates data ingestion, transformation, and storage, delivering an efficient and scalable solution for traffic monitoring and analysis.

---

### **Code**

GitHub repository containing the full pipeline code: [GitHub Repository](https://github.com/ana-kelley/DataEngineeringProject).

---

### **Thorough Investigation**

The pipeline handles real-time data ingestion, processing, and visualization while retaining historical records. It queries the API for "present" incidents, processes only new files using S3 logs, and updates the database by adding new incidents and marking missing ones as "inactive." This reduces API calls, adheres to rate limits, and ensures scalability.

#### **Innovativeness**:  
The project combines real-time monitoring with historical data retention. Processing only new data and marking missing incidents as "inactive" limits API calls while maintaining comprehensive records, efficiently managing API rate limits.

#### **Scaling Recommendation**:

- Use a streaming architecture (e.g., Kafka Streams) for higher data volumes and lower latency.

#### **Concerns**:

- API rate limits and schema changes.
- Increasing AWS costs for S3 and RDS.
- Dependence on error handling for reliability.

#### **Next Steps**:

- Add predictive traffic models.
- Implement real-time alerts for incidents.
- Enhance Tableau dashboards with clustering and heatmaps.
