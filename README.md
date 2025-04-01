UK_AIRPORTS_OPS ✈️
<img width="735" alt="420696385-a880f61f-4d4c-4e75-bdeb-3c05eefc105e" src="https://github.com/user-attachments/assets/09988643-6603-4eb9-b432-ddbb0716faa8" />


OVERVIEW 
This project provides a real-time operational overview for 44 UK airports, combining airport geolocation data with live weather conditions. The goal is to support airport operators and planners with accurate, timely environmental insights that influence flight schedules, ground operations, and the overall passenger experience.

By integrating real-time weather data from the OpenWeatherMap API and detailed metadata for each airport — including latitude/longitude, ICAO/IATA codes, city, and country — this system enables early detection of weather-related risks.


Key international hubs in the dataset include:

London Heathrow (IATA: LHR, ICAO: EGLL)

London Gatwick (IATA: LGW, ICAO: EGKK)

Manchester (IATA: MAN, ICAO: EGCC)

London Stansted (IATA: STN, ICAO: EGSS)

Birmingham (IATA: BHX, ICAO: EGBB)

London Luton (IATA: LTN, ICAO: EGGW)


The insights are presented through an interactive PowerBI dashboard to show real-time departure and arrival flight updates and how weather will affect those flights.



Problem Statement
The airport requires an interactive dashboard that provides real-time insights into flight performance and the impact of weather on current and future operations.
An automated data pipeline is essential to ensure continuous updates on flight schedules and weather conditions.


Objectives
Gather and analyze daily airport operations to assess overall performance and identify any emerging issues.
Monitor upcoming weather conditions to anticipate potential disruptions to airport operations.
Develop a user-friendly dashboard that provides quick and easy access to essential flight and weather information.


Tech Stack
Python 🐍 (Data fetching, processing, and transformation)

Apache Spark (PySpark) ⚡ (Data handling and transformations)

Databricks 📊 (Data processing, storage, orchestration)

Power BI 📈 – Interactive dashboards and real-time visualizations for weather insights and airport analytics


Data Sources
1. AviationStack API 🛬
About: This API offers a simple, free way of accessing global flight tracking data in real-time. It provides an extensive set of aviation data, including real-time flight status, flight schedules, airline routes, airports, and aircrafts. It updates data every 30-60 seconds. This included information from 250+ countries and 13000+ airlines.
Need: This API provided the base for the project and the rest of the project is built on top of this.
Data includes: Airline IATA, Airline name, Flight number, Depart/Arrival location, Flight status, Flight delay amount, Scheduled time, Estimated time. AviationStack data divides into Departure and Arrival flight information for UK airport.
Data problems: There were limits to the free version of their API of 100 requests each month.

2. OpenWeatherMap API ⛅
About: OpenWeatherMap provides real-time and forecasted weather data through a flexible and well-documented API. It offers current weather, hourly forecasts, and 7-day forecasts for any global location based on latitude and longitude.
Need: Weather data from OpenWeatherMap was integrated to assess how weather events could impact airport operations — particularly delays and safety measures at UK airports.
Data Includes:

Temperature

Wind speed and direction

Humidity and pressure

Weather conditions (e.g., rain, snow, fog)

Short and long-term forecasts
Limitations: Some detailed features (like historical data or advanced forecasts) require a paid subscription, and the free tier has rate limits per minute/hour.
3. Global Airport Database 📍
About: This Global Airport Database provided location information on 9300 large and small airports all around the world.
Need: The AviationStack API data did not include location information for flights. Another data source was needed to get detailed airport information to be able to map flight locations.
Data includes: ICAO code, IATA code, airport name, country, city, latitude, longitude, altitude


Architecture and Methodology 📝
Medallion Architecture was implemented to enhance data quality, organization, and reliability throughout the data pipeline.

In the bronze layer, raw data was ingested in its original form, serving as the foundational data source. Before transitioning to the silver layer, rigorous data processing techniques were applied, including data cleansing, transformation, deduplication, and filtering, to ensure consistency and accuracy. Finally, before promoting data to the gold layer, comprehensive unit tests were conducted to validate data integrity, preventing bad or incomplete data from reaching production-level tables.

This structured approach ensures that only high-quality, reliable data is used for analysis and decision-making.

Data Pipeline Architecture


![image](https://github.com/user-attachments/assets/91031a04-f589-45b5-9a8e-a285ab690c1d)



Data Model Design ⚙️

![image](https://github.com/user-attachments/assets/3e97f312-d2c7-46a6-932b-fb6b65e6b699)


In this project, data was intentionally not normalized because the primary use case is visualization via a PowerBI dashboard. By maintaining a denormalized structure, we reduce the need for complex joins and improve query performance when retrieving insights. This approach ensures that airport operations and weather data can be accessed quickly and efficiently without unnecessary overhead.


Streaming Processing, Ingestion, & Storage 💾
Implemented Databricks Delta Live Tables to stream real-time departure and arrival data from the AviationStack API.

Configured a Databricks workflow to run continuously, integrating a Delta Live pipeline for seamless data ingestion and processing.
Batch Processing
Configured a Databricks workflow to schedule hourly updates, ensuring the dashboard uses the latest Open Weather data.
The Global Airport Database lacked documentation on data refresh intervals, so no automated workflow was implemented for its updates.


Data Quality 🔢
To maintain the integrity and reliability of the gold-level tables, unit tests were implemented to validate the data. These tests ensured:

No null values were present in critical fields.
All expected columns existed in the dataset.
Duplicate records were identified and removed.


Orchestration
![uk_dept_+_arrival_job_run_every_min](https://github.com/user-attachments/assets/7a19a245-16c8-4224-b7b1-391576c9a7dc)

![uk_airport_weather_job_run](https://github.com/user-attachments/assets/cb3ad9b1-6d60-44a3-b63e-5eba5a6da818)


Used Databricks workflow to schedule continuous a updates
OpenWeatherMap API workflow was scheduled for 30 minutes after the hour


Visualizations 📊

Arrivals page:


![Arrival's Dashboard](https://github.com/user-attachments/assets/c7da1842-d83d-4ec3-b549-5f1f983305d5)


Weather Page :
![Weather_Dashboard](https://github.com/user-attachments/assets/44f27549-4eec-4a04-b6f9-4d0b7dba29d7)

Departure Page:

![Depature_dashboard](https://github.com/user-attachments/assets/44b054a3-a1d3-4731-8935-47230037bc53)

Arrival + Depart Delay 
![Arrival+Dept_delay_info](https://github.com/user-attachments/assets/aec73c76-39ac-438c-8f74-5fc97943e8c7)


Future Enhancements
Extend data coverage to include all airports for a more comprehensive analysis.
Enrich the dataset with additional insights relevant to airport operators, such as flight safety information and historical flight data.
Incorporate UV weather data to enhance weather-related decision-making.











