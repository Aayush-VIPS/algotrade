import logging
import json
import csv
import io
import requests
from flask import Flask, request, jsonify, abort
from dhanhq import dhanhq

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# DhanHQ credentials
DHAN_CLIENT_ID = "1103141889"
DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQ0NDc2ODEwLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMzE0MTg4OSJ9.wMeUYEuX0U2txEfiyXnIssR4fepBOSuXEduH3CChCNS6MHV4Gy_qTDj3FRf5rKv4r1airtfEOq13T3QNWLuHPA"

dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)

# Load instrument list
instrument_lookup = {}
remote_csv_url = "https://drive.google.com/uc?export=download&id=1HHUmaD3xL3hnVgDDqE2Rt98R5N7olLTs"

def load_instrument_list():
    global instrument_lookup
    try:
        response = requests.get(remote_csv_url)
        response.raise_for_status()
        csvfile = io.StringIO(response.content.decode('utf-8'))
        reader = csv.DictReader(csvfile)
        for row in reader:
            symbol = row.get("SYMBOL_NAME") or row.get("DISPLAY_NAME")
            sec_id = row.get("SECURITY_ID")
            if symbol and sec_id:
                instrument_lookup[symbol.strip().upper()] = sec_id.strip()
        logging.info("Instrument list loaded successfully.")
    except Exception as e:
        logging.error("Failed to load instrument list: %s", e)

load_instrument_list()

# Exchange & Product Mappings
EXCHANGE_MAP = {"NSE_FNO": dhan.NSE_FNO}
PRODUCT_MAP = {"INTRA": dhan.INTRA}
ORDER_TYPE_MAP = {"MARKET": dhan.MARKET, "LIMIT": dhan.LIMIT}

# Fetch correct security ID for options
def get_option_security_id(symbol, expiry, strike, option_type):
    try:
        option_chain = dhan.option_chain(under_security_id=13, under_exchange_segment="IDX_I", expiry=expiry)
        for option in option_chain['data']:
            if (option['strikePrice'] == float(strike) and option['optionType'] == option_type):
                return option['securityId']
    except Exception as e:
        logging.error("Failed to fetch option chain: %s", e)
    return None

@app.route('/webhook', methods=['POST'])
def webhook():
    logging.info("Received request from IP: %s", request.remote_addr)
    
    try:
        data = request.get_json()
    except Exception as e:
        logging.error("Invalid JSON payload: %s", e)
        abort(400, "Invalid JSON payload")
    
    logging.info("Received webhook data: %s", data)
    action, quantity = data.get("action"), data.get("quantity")
    if not action or quantity is None:
        logging.error("Missing required fields: 'action' or 'quantity'")
        abort(400, "Missing required fields")
    
    symbol, expiry, strike, option_type = data.get("symbol"), data.get("expiry"), data.get("strike"), data.get("option_type")
    security_id = instrument_lookup.get(symbol.upper()) if symbol else None
    
    if not security_id and symbol and expiry and strike and option_type:
        security_id = get_option_security_id(symbol, expiry, strike, option_type)
    
    if not security_id:
        logging.error("Security ID not found.")
        abort(400, "Security ID not found")
    
    transaction_type = dhan.BUY if action.upper() == "BUY" else dhan.SELL
    exchange_segment = EXCHANGE_MAP.get(data.get("exchange_segment", "NSE_FNO"), dhan.NSE_FNO)
    product_type = PRODUCT_MAP.get(data.get("product_type", "INTRA"), dhan.INTRA)
    order_type = ORDER_TYPE_MAP.get(data.get("order_type", "MARKET"), dhan.MARKET)
    price = data.get("price", 0)
    
    try:
        order_response = dhan.place_order(
            security_id=security_id,
            exchange_segment=exchange_segment,
            transaction_type=transaction_type,
            quantity=quantity,
            order_type=order_type,
            product_type=product_type,
            price=price
        )
        logging.info("Order placed successfully: %s", order_response)
        return jsonify({"status": "success", "order_response": order_response}), 200
    except Exception as e:
        logging.error("Order placement failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
