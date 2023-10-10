import requests
from datetime import date, datetime, timedelta
import json
import pandas as pd


def task1():
    url  = base_url =  "https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf"

    payload = {}
    headers = {'Content-Type': 'application/json'}

    list_of_state = json.loads(requests.request(
        "GET", url, headers=headers, data=payload).text)
    size = 500
    time_delta = 365
    max_date = (date.today()).strftime("%Y-%m-%d")
    min_date = (date.today() - timedelta(days=time_delta)).strftime("%Y-%m-%d")
    response = []
    for key, value in list_of_state.items():
        url = f'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={size}&date_received_max={max_date}&date_received_min={min_date}&state={key}'

        response.append(requests.get(url).json())

    return response
