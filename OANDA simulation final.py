# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 13:54:48 2023

@author: agaaz
"""
# Data Engineering Spring 2023 Homework 4

# Atul Manjunath Bharadwaj (am11449) and Agaaz Oberoi (ao2460)

import time
import pandas as pd
from datetime import datetime, timedelta
from oandapyV20 import API
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.pricing import PricingInfo
from time import sleep
import pytz

api_key = "57694390ebf17607dab1aa18a815fefa-80ed719161214e8d9013d6645dbb0bec"
account_id = "101-001-25498349-003"
client = API(access_token=api_key)

def get_current_price(instrument):
    params = {"instruments": instrument}
    endpoint = PricingInfo(account_id, params=params)
    response = client.request(endpoint)
    price = float(response["prices"][0]["bids"][0]["price"])
    return price

orders = [
    {"instrument": "EUR_USD", "side": "buy", "total_amount": 100000},
    {"instrument": "GBP_CHF", "side": "sell", "total_amount": 100000},
]

now = datetime.utcnow()
local_tz = pytz.timezone('America/New_York')
now_local = now.replace(tzinfo=pytz.utc).astimezone(local_tz)

execution_sets = [
    (1, now_local.replace(hour=15, minute=0, second=0, microsecond=0), now_local.replace(hour=17, minute=0, second=0, microsecond=0)),
    (1, now_local.replace(hour=19, minute=0, second=0, microsecond=0), now_local.replace(hour=22, minute=0, second=0, microsecond=0)),
    (1, now_local.replace(hour=23, minute=0, second=0, microsecond=0), (now_local + timedelta(days=1)).replace(hour=1, minute=0, second=0, microsecond=0)),
    (1, (now_local + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0), (now_local + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)),
]

data_storage = {order["instrument"]: [] for order in orders}

def send_order(order, units):
    order_data = {
        "order": {
            "instrument": order["instrument"],
            "units": str(units) if order["side"] == "buy" else str(-units),
            "type": "MARKET",
            "positionFill": "DEFAULT",
        }
    }
    endpoint = OrderCreate(account_id, data=order_data)
    response = client.request(endpoint)
    return response

start_time = datetime.now()
print(f"Script started at {start_time}")

response = None

while True:
    now = datetime.now(tz=local_tz)
    elapsed_time = datetime.now() - start_time
    if elapsed_time >= timedelta(hours=15):
        break

    for execution_set in execution_sets:
        fraction, start, end = execution_set

        if start <= now <= end:
            # Calculate the duration of the current execution set in hours
            duration = (end - start).seconds / 3600

            # Calculate the number of orders per hour and the units per order
            orders_per_hour = 10
            total_units = 10000

            for order in orders:
                current_price = get_current_price(order["instrument"])
                units_per_order = int(total_units / current_price)

                response = send_order(order, units_per_order)

                if "orderFillTransaction" in response:
                    # Save order data
                    data_storage[order["instrument"]].append({
                        "timestamp": datetime.now(),
                        "order_id": response["orderFillTransaction"]["orderID"],
                        "instrument": order["instrument"],
                        "side": order["side"],
                        "amount": units_per_order,
                        "price": response["orderFillTransaction"]["price"],
                    })
                else:
                    print("Order was not filled.")

                # Print the order response details
                print(f"Order response: {response}")

            # Sleep for 6 minutes (360 seconds) before the next execution within the same execution set
            sleep(360)

    if now > execution_sets[-1][2]:
        break

# Save data to CSV files
for instrument, data in data_storage.items():
    df = pd.DataFrame(data)
    df.to_csv(f"{instrument}_orders.csv", index=False)