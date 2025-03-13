import logging
import json
import csv
import io
import requests
from flask import Flask, request, jsonify, abort
from dhanhq import dhanhq  # Official DhanHQ Python client

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Allowed TradingView IP addresses (for testing, include '127.0.0.1')
ALLOWED_IPS = {
    "127.0.0.1",
    "52.89.214.238",
    "34.212.75.30",
    "54.218.53.128",
    "52.32.178.7"
}

# DhanHQ credentials – update these with your actual details
DHAN_CLIENT_ID = "1103141889"   # e.g., "1000000003"
DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQ0NDc2ODEwLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMzE0MTg4OSJ9.wMeUYEuX0U2txEfiyXnIssR4fepBOSuXEduH3CChCNS6MHV4Gy_qTDj3FRf5rKv4r1airtfEOq13T3QNWLuHPA"       # Your JWT access token

# Create a DhanHQ client instance
dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)

# Global dictionary to map a symbol (DISPLAY_NAME / SYMBOL_NAME) -> SECURITY_ID
instrument_lookup = {}

def load_instrument_list_from_url(url):
    """
    Loads the instrument list CSV from a remote URL and builds a lookup dictionary.
    The CSV is expected to have the headers: DISPLAY_NAME (or SYMBOL_NAME) and SECURITY_ID.
    """
    global instrument_lookup
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an error for bad status codes
        content = response.content.decode('utf-8')
        csvfile = io.StringIO(content)
        reader = csv.DictReader(csvfile)
        count = 0
        for row in reader:
            # Use DISPLAY_NAME if available; otherwise, fallback to SYMBOL_NAME.
            symbol = row.get("DISPLAY_NAME") or row.get("SYMBOL_NAME")
            sec_id = row.get("SECURITY_ID")
            if symbol and sec_id:
                instrument_lookup[symbol.strip().upper()] = sec_id.strip()
                count += 1
        logging.info("Loaded %d instruments from remote CSV.", count)
    except Exception as e:
        logging.error("Failed to load instrument list from URL: %s", e)

# Convert the provided Google Drive link to a direct download URL.
remote_csv_url = "https://drive.google.com/uc?export=download&id=1HHUmaD3xL3hnVgDDqE2Rt98R5N7olLTs"
load_instrument_list_from_url(remote_csv_url)

# Mapping dictionaries for converting string values to official DhanHQ constants.
EXCHANGE_MAP = {
    "NSE": dhan.NSE,
    "NSE_FNO": dhan.NSE_FNO,
    "BSE": dhan.BSE_FNO,
    "MCX": dhan.MCX,
    # Add other mappings if needed.
}

PRODUCT_MAP = {
    "INTRA": dhan.INTRA,
    "CNC": dhan.CNC,
    "MARGIN": dhan.MARGIN,
    "CO": dhan.CO,
    "BO": dhan.BO,
    # Add more if needed.
}

ORDER_TYPE_MAP = {
    "MARKET": dhan.MARKET,
    "LIMIT": dhan.LIMIT,
    # Add other mappings if needed.
}

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Webhook endpoint for TradingView alerts.
    Expects a JSON payload with at least:
      - "action" (BUY or SELL)
      - "quantity"
    Optionally:
      - "security_id" or "symbol"
      - "exchange_segment"
      - "product_type"
      - "order_type"
      - "price"
    """
    # Validate the source IP address.
    if request.remote_addr not in ALLOWED_IPS:
        logging.warning("Unauthorized IP: %s", request.remote_addr)
        abort(403, description="Forbidden: Unauthorized IP")
    
    # Parse the JSON payload.
    if request.is_json:
        data = request.get_json()
    else:
        try:
            data = json.loads(request.data.decode('utf-8'))
        except Exception as e:
            logging.error("Invalid JSON payload: %s", e)
            abort(400, description="Invalid JSON payload")
    
    logging.info("Received webhook data: %s", data)
    
    # Extract required fields.
    action = data.get("action")
    quantity = data.get("quantity")
    
    if not action or quantity is None:
        logging.error("Missing required fields: 'action' or 'quantity'")
        abort(400, description="Missing required fields: 'action' and 'quantity' are required")
    
    # Determine security_id: use provided security_id or look up via symbol.
    security_id = data.get("security_id")
    if not security_id:
        symbol = data.get("symbol")
        if not symbol:
            logging.error("Neither 'security_id' nor 'symbol' provided.")
            abort(400, description="Either 'security_id' or 'symbol' must be provided")
        security_id = instrument_lookup.get(symbol.strip().upper())
        if not security_id:
            logging.error("Symbol '%s' not found in instrument lookup.", symbol)
            abort(400, description=f"Symbol '{symbol}' not found in instrument list")
    
    # Map action to official client constants.
    if action.upper() == "BUY":
        transaction_type = dhan.BUY
    elif action.upper() == "SELL":
        transaction_type = dhan.SELL
    else:
        logging.error("Invalid action value: %s", action)
        abort(400, description="Invalid 'action' value. Must be 'BUY' or 'SELL'")
    
    # Map optional parameters using mapping dictionaries.
    exchange_segment_str = data.get("exchange_segment", "NSE")
    product_type_str = data.get("product_type", "INTRA")
    order_type_str = data.get("order_type", "MARKET")
    price = data.get("price", 0)
    
    exchange_segment = EXCHANGE_MAP.get(exchange_segment_str.upper(), dhan.NSE)
    product_type = PRODUCT_MAP.get(product_type_str.upper(), dhan.INTRA)
    order_type = ORDER_TYPE_MAP.get(order_type_str.upper(), dhan.MARKET)
    
    try:
        # Place the order using the official DhanHQ client.
        order_response = dhan.place_order(
            security_id=security_id,
            exchange_segment=exchange_segment,
            transaction_type=transaction_type,
            quantity=quantity,
            order_type=order_type,
            product_type=product_type,
            price=price
        )
    except Exception as e:
        logging.error("Order placement failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500
    
    logging.info("Order placed successfully: %s", order_response)
    return jsonify({"status": "success", "order_response": order_response}), 200

if __name__ == '__main__':
    # Run the Flask app on host 0.0.0.0 so it's externally accessible.
    app.run(host='0.0.0.0', port=5000)
