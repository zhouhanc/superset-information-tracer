A multi-narrative, multi-platform information tracing dashboard
=========

- A dashboard built on top of Information Tracer data 
- Useful to monitor multiple narratives over multiple platforms
- Real-world use case: monitor and compare popularity and sentiments of 4 candidates during the 2023 Coahuila Election  

[**How to use this dashboard**](#how-to-use-this-dashboard) | 
[**Installation and Configuration**](#installation-and-configuration) |

## General Functions of the Dashboard
- Hover on graphs to see statistics in details.
- Customize timeframe by dragging the blue time bar below each graph.
- Click on legends to show or hide a candidate.
- Click "Grouped" or "Stacked" to change bar plot orientation.
- Search bar: enter keywords to filter records.
- Drag columns to change the order of table.
- Click on column names to sort tables.
- Hover on an edge or node to check weight and highlight part of the network.


## How to use this dashboard?

### Content 
- **Interactions**
- **Sentiment Analysis**
- **Principal Actor and Word Cloud**
- **Network Analysis**


### Interactions

**How do we define interactions?**<br>
.....

**1. Total Interactions**<br>
This graph shows how total number of daily interactions (aggregated statistics from Twitter, Facebook, Instagram, Youtube) change by time for different candidates.
 
**2. Interactions by Platform**<br>
  - **Total Interactions** <br>
This graph shows how daily interactions change by time on a specific platform for different candidates.

  - **Cumulative Interactions**<br>
This graph shows how cumulative interactions change by time on a specific platform for different candidates. It's useful for identifying long-term trends and overall patterns in the data.

  - **Interaction Percentage**<br>
This graph shows daily interactions as a percentage on a specific platform for different candidates. <br>
$$\frac{\text{candidate}_i \text{ interaction}}{\sum\text{candidate}_i \text{ interaction}}$$




### Sentiment Analysis
We work with three types of sentiment: Positive, Neutral, Negative. To improve the accuracy of sentiment analysis, we parse each post into individual sentences and assign each sentence the interaction of that post.<br>
<br>
**1. Sentence Sentiment Distribution**


**2. Sentence Sentiment Time Series**


**3. Interaction Sentiment Distribution**



**4. Interaction Sentiment Time Series**



### Principal Actor and Word Cloud

### Network Analysis




## Architecture
Database Schema,
update mechanism, etc

## Installation and Configuration
[TODO]

## License
MIT

