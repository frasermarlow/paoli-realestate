# Woodgate Development Sales Price Monitoring System

## Project Overview

Monitor real estate price estimates and actual sales data for 60 similar residential units in the Woodgate development in Paoli, PA. Build a longitudinal dataset comparing Zillow Zestimates and Redfin estimates against actual sale prices over time to evaluate model accuracy.

## Objectives

1. Systematically scrape property estimate data from Zillow and Redfin for all 60 units
2. Track actual sale prices as they occur
3. Build a time-series database of estimates vs. actual sales
4. Generate longitudinal analysis showing how accurate each model is, whether estimates lead/lag actual sales, and systematic over/under-estimation patterns

## Data to Capture

For each property:
- Property address (unit identifier)
- Zillow Zestimate
- Redfin estimate
- Listed asking price (if property is for sale)
- Actual sale price (when transaction occurs)
- Timestamp of data capture
- Date range of estimate validity

## Implementation Approach

### Phase 1: Setup & Data Collection Infrastructure

1. **Database Schema**
   - Create SQLite or PostgreSQL database with tables for:
     - `properties` (address, unit_id, development_id, etc.)
     - `estimates` (property_id, source, estimated_price, timestamp)
     - `sales` (property_id, sale_price, sale_date, asking_price)
   - Ensure timestamps on all records for longitudinal analysis

2. **Web Scraper Development**
   - Build Python script using BeautifulSoup and requests
   - Target URLs: Zillow property pages and Redfin property pages
   - Extract key data points from HTML (Zestimate, Redfin estimate, listing status)
   - Implement rate limiting (1-2 second delays between requests)
   - Add error handling for missing data or failed requests
   - Log scraping activity for debugging

3. **Property List**
   - Compile list of all 60 Woodgate unit addresses in standard format
   - Create lookup table mapping addresses to URLs on Zillow and Redfin
   - Validate addresses and fix any formatting issues upfront

### Phase 2: Automated Collection

1. **Scheduling**
   - Set up daily scraper to run on 4-6 properties per day
   - Rotate through all 60 units every 10-15 days
   - Target monthly data collection cycle
   - Use cron job or task scheduler to automate

2. **Manual Sales Tracking**
   - Monitor Woodgate development for actual sales (check MLS if possible, or monitor listing removals with "Sold" status)
   - Manually log actual sale prices with dates in database when transactions close
   - Cross-reference with scraped data to create before/after estimate comparisons

3. **Data Validation**
   - Check for scraping errors or missing values
   - Validate that estimates are reasonable (within range of other estimates)
   - Alert if a property shows unusual price jumps that might indicate data error

### Phase 3: Analysis & Reporting

1. **Longitudinal Analysis**
   - Calculate median error between Zestimate and actual sale price
   - Calculate median error between Redfin estimate and actual sale price
   - Compare which model is more accurate overall
   - Analyze whether estimates tend to be high or low (systematic bias)
   - Track how estimates change over time for properties approaching sale

2. **Visualizations**
   - Time-series plot: estimate vs. actual price over time
   - Scatter plot: Zestimate accuracy (estimated vs. actual) with trend line
   - Scatter plot: Redfin accuracy (estimated vs. actual) with trend line
   - Box plots: distribution of errors by model
   - Comparison chart: Zillow vs. Redfin error rates

3. **Reporting**
   - Summary statistics: mean error, median error, standard deviation by model
   - Case studies: highlight properties where models were notably accurate or inaccurate
   - Findings: which model is more reliable, any patterns in over/under-estimation

## Technical Stack Recommendations

- **Language**: Python 3.8+
- **Web Scraping**: BeautifulSoup4, requests, or Selenium (if dynamic content loading needed)
- **Database**: SQLite (simple, file-based) or PostgreSQL (if scaling needed later)
- **Data Analysis**: pandas, numpy
- **Visualization**: matplotlib or plotly
- **Scheduling**: APScheduler or system cron
- **Error Handling**: logging module, email alerts for failures

## Important Considerations

1. **Terms of Service**: Verify that scraping Zillow and Redfin complies with their ToS. Both generally allow moderate scraping but have restrictions on commercial use. Keep requests respectful with rate limiting.

2. **robots.txt**: Check and respect robots.txt files on both sites.

3. **Data Freshness**: Estimates can change daily. Timestamps are crucial for tracking how stale estimates are when sales occur.

4. **Sample Size**: With only 60 units and sales happening sporadically, the dataset will grow slowly. Plan for at least 6-12 months to get meaningful sample size (likely 8-15 actual sales).

5. **Confounding Factors**: Be aware that estimates may legitimately change between capture and sale due to market conditions, property improvements, or model updates.

## Deliverables

1. Working scraper code with error handling
2. Database schema and populated data
3. Monthly data collection reports
4. Final longitudinal analysis with visualizations
5. Summary findings document comparing Zillow vs. Redfin accuracy

## Success Criteria

- Successfully collect estimate data for all 60 properties each month
- Achieve &lt;5% error rate on scraping (data extraction accuracy)
- Maintain database with clean, timestamped records
- Gather actual sales data for at least 8-12 properties over study period
- Generate actionable findings on model accuracy differences
