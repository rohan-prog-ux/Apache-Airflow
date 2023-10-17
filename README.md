How to get all US States: You will use this list for Recursion on API to get all states Data.

list_of_states=list(requests.get("https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json").json().keys())

2. API Hit URL

url = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={size}&date_received_max={}&date_received_min={}&state={}'

3. Example Scrapping of Data for Washington State for Today (Remember : You have to put a Loop through all States)

from datetime import date, datetime, timedelta

size=500
time_delta=365
max_date = (date.today()).strftime("%Y-%m-%d")
min_date = (date.today() - timedelta(days=time_delta)).strftime("%Y-%m-%d")

state='WA'

print(url.format(size,max_date, min_date, state))

import requests

results=requests.get(url.format(size,max_date, min_date, state)).json()

4. Dumping Data into Google Sheets : See Mentioned Video and Code Repo