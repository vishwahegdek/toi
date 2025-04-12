# Times of India News Scraper

This project is a Python-based web scraper that periodically extracts news headlines and articles from the Times of India website (`https://timesofindia.indiatimes.com/`). The scraped data is processed into JSON format, sent to a Kafka topic, and logged to a PostgreSQL database for tracking.

## Features
- Scrapes headlines and articles from sections like Top News, Metro Cities, Business, and World News.
- Extracts article content and image URLs.
- Pushes data to a Kafka topic for downstream processing.
- Logs scraping success/failure to a PostgreSQL database.
- Runs on a configurable schedule.

## Prerequisites
- **Python 3.x**
- **Libraries**: Install via `pip`:
  ```bash
  pip install requests beautifulsoup4 kafka-python psycopg2-binary configparser