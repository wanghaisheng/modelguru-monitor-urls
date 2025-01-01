import requests
import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns

# Cloudflare D1 API URL and headers
D1_API_URL = "https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/sql/query"
API_TOKEN = "YOUR_CLOUDFLARE_API_TOKEN"

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# SQL query to fetch app ranking data for the past month
query = """
    SELECT * FROM app_data
    WHERE updateAt BETWEEN '2024-01-01' AND '2024-12-31'
    ORDER BY updateAt, rank;
"""

# Function to fetch data from Cloudflare D1
def fetch_data():
    payload = {"query": query}
    response = requests.post(D1_API_URL, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        data = response.json()['result']
        return pd.DataFrame(data)
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

# Function to analyze rank changes and top movers
def analyze_rank_changes(df):
    df['updateAt'] = pd.to_datetime(df['updateAt'])

    # Rank change per app
    df['rank_change'] = df.groupby('appid')['rank'].diff().fillna(0)

    # Find significant changes (more than 10 rank change)
    significant_changes = df[abs(df['rank_change']) > 10]
    
    # Top gainers (apps with the largest rank drops)
    top_gainers = df[df['rank_change'] < 0].groupby('appid').agg(
        total_drop=('rank_change', 'sum'),
        total_days=('appid', 'count')
    ).reset_index()

    top_gainers = top_gainers.sort_values(by='total_drop', ascending=True)

    return significant_changes, top_gainers

# Function to analyze category-level trends
def analyze_category_trends(df):
    category_trends = df.groupby(['updateAt', 'type']).agg(
        top_apps_count=('appid', 'count')
    ).reset_index()

    category_trends_pivot = category_trends.pivot(index='updateAt', columns='type', values='top_apps_count')
    
    return category_trends_pivot

# Function to generate and save the report
def generate_report():
    # Step 1: Fetch the data from D1
    df = fetch_data()

    if df is not None:
        # Step 2: Rank Change Analysis
        significant_changes, top_gainers = analyze_rank_changes(df)
        
        # Step 3: Category Trends Analysis
        category_trends_pivot = analyze_category_trends(df)

        # Step 4: Save the data to CSV
        significant_changes.to_csv("significant_rank_changes.csv", index=False)
        top_gainers.to_csv("top_gainers.csv", index=False)
        category_trends_pivot.to_csv("category_trends.csv", index=True)

        # Step 5: Visualize and save category trends
        plt.figure(figsize=(12, 6))
        category_trends_pivot.plot()
        plt.title('Top Apps Count per Category Over Time')
        plt.xlabel('Date')
        plt.ylabel('Number of Top Apps')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('category_trends.png')
        plt.close()

        # Step 6: Visualize top gainers
        plt.figure(figsize=(10, 6))
        top_gainers.head(10).plot(kind='bar', x='appid', y='total_drop', legend=False, color='green')
        plt.title('Top 10 Apps with the Largest Rank Gains')
        plt.xlabel('App ID')
        plt.ylabel('Total Rank Drop')
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig('top_gainers.png')
        plt.close()

        print("Report generated successfully! Files saved as .csv and .png")
    else:
        print("Failed to fetch data, skipping report generation.")

# Main function to run the script
if __name__ == "__main__":
    generate_report()
