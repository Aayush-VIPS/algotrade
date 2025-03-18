import json
import logging
import requests
from flask import Flask, request
from dhanhq import DhanHQ
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Dhan API Setup
DHAN_API_KEY = "your_dhan_api_key_here"
dhan = DhanHQ(DHAN_API_KEY)

# Load instrument list
instrument_file = "api-scrip-master-detailed.csv"
instrument_df = pd.read_csv(instrument_file)
logging.info("Instrument list loaded successfully.")

def get_security_id(symbol, expiry, option_type, strike_price):
    """Fetch the correct security_id for index options."""
    df = instrument_df
    df_filtered = df[(df['UNDERLYING_SYMBOL'] == symbol) &
                     (df['SM_EXPIRY_DATE'] == expiry) &
                     (df['OPTION_TYPE'] == option_type) &
                     (df['STRIKE_PRICE'] == float(strike_price))]
    
    if df_filtered.empty:
        logger.error(f"No matching security_id found for {symbol} {expiry} {option_type} {strike_price}")
        return None
    
    return df_filtered.iloc[0]['SECURITY_ID']

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json
        logger.info(f"Received webhook data: {data}")
        
        action = data.get("action").upper()
        symbol = data.get("security_id")
        quantity = int(data.get("quantity", 0))
        exchange_segment = data.get("exchange_segment", "NSE_FNO")
        order_type = data.get("order_type", "MARKET")
        product_type = data.get("product_type", "INTRA")
        price = float(data.get("price", 0))
        
        # Extract expiry, option type, and strike price from symbol
        expiry = "2024-10-31"  # Update dynamically if needed
        option_type = "CE" if "C" in symbol else "PE"
        strike_price = symbol[-5:]
        
        # Get correct security_id
        security_id = get_security_id("NIFTY", expiry, option_type, strike_price)
        if not security_id:
            return json.dumps({"status": "failure", "message": "Invalid security_id"}), 400
        
        # Place order
        order_payload = {
            "transaction_type": action,
            "security_id": security_id,
            "quantity": quantity,
            "exchange_segment": exchange_segment,
            "order_type": order_type,
            "product_type": product_type,
            "price": price
        }
        
        logger.info(f"Placing order: {order_payload}")
        response = dhan.place_order(order_payload)
        logger.info(f"Order response: {response}")
        
        return json.dumps(response), 200
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return json.dumps({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
