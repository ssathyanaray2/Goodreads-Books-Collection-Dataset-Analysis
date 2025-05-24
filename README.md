**Goodreads NoSQL Analytics – Project Overview**

This project explores the Goodreads Top 100 Books dataset (1980–2023) using MongoDB to extract insights about trends in book genres, popularity, and content characteristics. We performed data cleaning, transformation, and analysis on 4,400 book records to uncover correlations between metadata (like publication date, genre, ratings) and reader engagement metrics (current readers, want-to-read).

**Features:**

1) **Data Preprocessing**: Cleaned and transformed nested and unstructured fields like genre, publication_date, and description for better query performance.

2) **Aggregation Queries:** Wrote MongoDB aggregation pipelines to:

   Analyze genre quality over decades using rating-based metrics.

   Track popularity trends (based on reader interest) by genre and decade.

   Study the correlation between title/description quality and popularity.

3) **Sentiment Analysis:** Applied a simple rule-based method to categorize book descriptions into positive, neutral, or negative sentiments.

4) **Performance Awareness:** Refactored fields (e.g., converting genre strings to arrays) to make queries more efficient and meaningful.

This project highlights how NoSQL databases like MongoDB can be effectively used for semi-structured data exploration, sentiment-based analysis, and historical trend discovery.


