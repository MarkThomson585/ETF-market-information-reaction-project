# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import boto3
import requests
import yfinance as yf
import pandas as pd
import datetime
from bs4 import BeautifulSoup
av_api_key = '35L7LADM48POJPK8'
av_api_key2 = 'F5NA73AD6MF3CASY'
import pytz
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os
os.makedirs('/app/output', exist_ok=True)

''' plan:
    look back a week for key market events
    take event days and time
    for each sector record the timeseries of performacne during that day, top 10 holdings and when their EPS is
    plot a timeseries for each sector for each important day with these key events plotted '''
    
def DE_ETF_workflow():
    
    key_events_list = ['Job Openings and Labor Turnover Survey','Housing Inventory Core Metrics','Employment Situation','Consumer Price Index','Sticky Price CPI','Producer Price Index','Gross Domestic Product','House Price Index']

    release_times = {
        "Job Openings and Labor Turnover Survey": "10:00:00",
        "Housing Inventory Core Metrics": "08:30:00",
        "Employment Situation": "08:30:00",
        "Consumer Price Index": "08:30:00",
        "Sticky Price CPI": "08:30:00",
        "Producer Price Index": "08:30:00",
        "Gross Domestic Product": "08:30:00",
        "House Price Index": "09:00:00"
    }

    def get_release_datetime(date_str, indicator):
        if indicator in release_times:
            time_str = release_times[indicator]
            # Combine the date and time into a datetime object
            release_datetime = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
            return release_datetime
        else:
            raise ValueError(f"Release time for {indicator} is not available.")
    
    ''' Get Key Macroeconomic Dates '''
    fred_api_key = 'c680cbe3680ece3169d45ffe04079642'

    today = datetime.today()
    #today = datetime(2024,8,28,10,24,53)
    # Find out what day of the week today is (Monday=0, Sunday=6)
    today_weekday = today.weekday()

    # Calculate how many days to subtract to get to the previous Monday (one week back)
    days_to_subtract = today_weekday + 7 + 1  # Adding 7 ensures we go back one full week
    
    # Get the previous Monday's date
    previous_monday = today - timedelta(days=days_to_subtract)

    # Get the following Friday's date by adding 4 days to the previous Monday
    previous_friday = previous_monday + timedelta(days=4)
    previous_monday_str = previous_monday.strftime('%Y-%m-%d')
    previous_friday_str = previous_friday.strftime('%Y-%m-%d')
    url = f'https://api.stlouisfed.org/fred/releases/dates?api_key={fred_api_key}&realtime_start={previous_monday_str}&realtime_end={previous_friday_str}&file_type=json'
    data_pull = requests.get(url)
    data = data_pull.json()
    extracted_data = [{"date": entry["date"], "release_name": entry["release_name"]} for entry in data["release_dates"]]
    df = pd.DataFrame(extracted_data)
    df = df[df['release_name'].isin(key_events_list)]
    
    df['Time of Day'] = df['release_name'].map(release_times)
    df['date'] = df['date'] + ' ' + df['Time of Day']

# Convert the combined string into a datetime object
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S')

# Drop the 'Time of Day' column as it's no longer needed
    df = df.drop(columns=['Time of Day'])
    key_events = df

    ''' go through each sector ETF and plot key market event timing '''
    
    sector_etfs = {
        'XLV': 'Healthcare',
        'XLF': 'Financials',
        'XLY': 'Consumer Discretionary',
        'XLP': 'Consumer Staples',
        'XLE': 'Energy',
        'XLU': 'Utilities',
        'XLI': 'Industrials',
        'XLB': 'Materials',
        'XLRE': 'Real Estate',
        'XLC': 'Communication Services',
        'XLK': 'Technology'
    }

    for etf in sector_etfs:
        url = f'https://finance.yahoo.com/quote/{etf}'

    # Send a GET request to the URL
        response = requests.get(url)

    # Check if the request was successful
        if response.status_code == 200:
        # Parse the page content using BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the section containing the top 10 holdings by looking for the h4 tag with the specific text
            holdings_section = soup.find('h4', string=lambda text: text and "Top 10 Holdings" in text)

            if holdings_section:
            # Navigate to the parent container that holds the relevant data
                holdings_container = holdings_section.find_next('div', {'class': 'container'})
            
            # Extract all the individual holding entries
                holdings = holdings_container.find_all('div', {'class': 'content'})

            # Initialize lists to store the data
                tickers = []
                companies = []
                weights = []
                eps_df_list = []
                prev_eps = pd.DataFrame()
                upcoming_eps = pd.DataFrame()
                prev_earning_date = []
                upcoming_earning_date = []
                prev_reported_EPS = []
                prev_EPS_estimate = []
                upcoming_EPS_estimate = []
                prev_EPS_surprise = []

            # Loop through the extracted rows
                for holding in holdings[:10]:  # Limit to the first 10 holdings
                    ticker = holding.find('span', {'class': 'symbol'}).text
                    company = holding.find('span', {'class': 'name'}).text
                    weight = holding.find('span', {'class': 'data'}).text

                    tickers.append(ticker)
                    companies.append(company)
                    weights.append(weight)


            else:
                print("Failed to find the holdings section.")
        else:
            print(f"Failed to retrieve data: {response.status_code}")

        for tkr in tickers:
            stock = yf.Ticker(tkr)
            print(tkr)
            # Get the earnings data as a DataFrame
            earnings_df_raw = stock.earnings_dates
            earnings_df = earnings_df_raw.reset_index()

            # Ensure the 'Earnings Date' is in datetime format
            #earnings_df['Earnings Date'] = pd.to_datetime(earnings_df['Earnings Date'])

            # Get the current date
            current_date = datetime.today()
            pst_timezone = pytz.timezone('America/Los_Angeles')
            datetime_obj_pst = pst_timezone.localize(current_date)

            # Convert the PST datetime to EST
            est_timezone = pytz.timezone('America/New_York')
            current_date = datetime_obj_pst.astimezone(est_timezone)
            past_earnings = earnings_df[earnings_df['Earnings Date'] < current_date]
            most_recent_earnings = past_earnings.iloc[0] if not past_earnings.empty else None

            # Filter for the next upcoming earnings report after the current date
            future_earnings = earnings_df[earnings_df['Earnings Date'] > current_date]
            upcoming_earnings = future_earnings.iloc[-1] if not future_earnings.empty else None
            

            # Combine the most recent and upcoming earnings reports into a single DataFrame
            prev_earning_date.append(most_recent_earnings[0])
            prev_reported_EPS.append(most_recent_earnings[2])
            prev_EPS_estimate.append(most_recent_earnings[1])
            #prev_EPS_surprise.append[most_recent_earnings[3]]
            if upcoming_earnings is not None and len(upcoming_earnings) > 0:
                upcoming_earning_date.append(upcoming_earnings[0])
                upcoming_EPS_estimate.append(upcoming_earnings[1])
            else:
                # If there's no upcoming earnings data, append None
                upcoming_earning_date.append(None)
                upcoming_EPS_estimate.append(None)
            
            
        prev_eps['Ticker'] = tickers
        prev_eps['Most Recent Earnings Report'] = prev_earning_date
        prev_eps['EPS Estimate'] = prev_EPS_estimate
        prev_eps['Reported EPS'] = prev_reported_EPS
        prev_eps['EPS % Difference'] = ((prev_eps['Reported EPS'] - prev_eps['EPS Estimate']) / prev_eps['EPS Estimate']) * 100
        upcoming_eps['Ticker'] = tickers
        upcoming_eps['Upcoming Earnings Report'] = upcoming_earning_date
        upcoming_eps['EPS Estimate'] = upcoming_EPS_estimate
        etf_frame = pd.DataFrame()
        etf_frame['Ticker'] = tickers
        etf_frame['Company'] = companies
        etf_frame['Weight'] = weights
        key_events = key_events.reset_index(drop=True)
        '''make graphs'''
        for i in range(len(key_events)):
            date = key_events['date'][i].date()
            time_of_event = key_events['date'][i]
            event = key_events['release_name'][i]
            stock = yf.Ticker(etf)
            
            # Adjust the start date to 3:00 PM the day before
            start_date = pd.to_datetime(date) - pd.DateOffset(days=1)
            start_date = start_date.replace(hour=15, minute=0, second=0)
            start_date = start_date.tz_localize('America/New_York')
            end_date = pd.to_datetime(date) + pd.DateOffset(days=1)
            end_date = end_date.tz_localize('America/New_York')
            # Fetch historical data with after-hours (pre-market and post-market) data included
            data = stock.history(interval='1m', start=start_date, end= end_date, prepost=True)
            
            # Convert the time_of_event (which is a 'YYYY-MM-DD HH:MM' string) into a datetime object
            event_time = pd.to_datetime(time_of_event) + timedelta(hours=4)
            
            # Plot the stock price
            plt.figure(figsize=(10, 6))
            plt.plot(data.index, data['Close'], label='Close Price')

            # Add the vertical line at the exact time of the event
            
            '''filter for earnings reports within the start and end dates and then plot them here'''
            filtered_df = prev_eps[(prev_eps['Most Recent Earnings Report'] >= start_date) & 
                           (prev_eps['Most Recent Earnings Report'] <= end_date)]
            filtered_df = filtered_df.reset_index(drop=True)
            text_offset = 0
            if not filtered_df.empty:
                for i in range(len(filtered_df)):
                    plt.axvline(x=filtered_df['Most Recent Earnings Report'][i], color='blue', linestyle='--', label=f'{filtered_df["Ticker"][i]} ({round(filtered_df["EPS % Difference"][i],2)})')
                    plt.text(filtered_df['Most Recent Earnings Report'][i], data['Close'].max() - text_offset, filtered_df['Ticker'][i], color='blue', ha='left') 
                    text_offset += 1.3
            
            plt.axvline(x=event_time, color='red', linestyle='--', label=event)

            # Add a label at the top of the vertical line
            plt.text(event_time, data['Close'].max(), event, color='red', ha='left')

            # Show the plot with grid, labels, and legend
            plt.grid(True)
            plt.ylabel('Price USD')
            plt.xlabel('Time of Day (EST)')
            plt.legend()
            plt.title(f'({etf}) {sector_etfs.get(etf)} sector performance on {date}')
            plt.savefig(f'/app/output/({etf}) {sector_etfs.get(etf)} sector performance on {date}.png')
        key_events.to_csv(f'/app/output/key_events.csv', index=False)
        etf_frame.to_csv(f'/app/output/etf_frame.csv', index=False)
        prev_eps.to_csv(f'/app/output/prev_eps.csv', index=False)
        upcoming_eps.to_csv(f'/app/output/upcoming_eps.csv', index=False)
    
    return [key_events, etf_frame,prev_eps,upcoming_eps]


proj_test = DE_ETF_workflow()

s3 = boto3.client('s3')

# Define your S3 bucket name
BUCKET_NAME = 'my-etf-png-bucket'

# Directory inside your Docker volume where PNG files are stored
PNG_DIR = '/app/data'

def upload_files_to_s3():
    # List all files in the directory
    for file_name in os.listdir(PNG_DIR):
        if file_name.endswith('.png'):
            file_path = os.path.join(PNG_DIR, file_name)
            # Upload each PNG file to the S3 bucket
            s3.upload_file(file_path, BUCKET_NAME, file_name)
            print(f'Uploaded {file_name} to S3')

if __name__ == "__main__":
    # Your existing logic here to generate the PNGs

    # After PNGs are generated, upload them to S3
    upload_files_to_s3()
