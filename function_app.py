import os
import json
import logging
import urllib
import azure.functions as func
from sqlalchemy import create_engine, text
from datetime import datetime

# ✅ Hardcoded Azure SQL credentials (FOR TESTING ONLY)
AZURE_SQL_SERVER = "kei-azure-server.database.windows.net"
AZURE_SQL_DATABASE = "KEI_DATABASE"
AZURE_SQL_USER = "adminuser"
AZURE_SQL_PASSWORD = "Kingston#1234"
AZURE_SQL_DRIVER = "ODBC Driver 18 for SQL Server"

# ✅ Build ODBC Connection String
connection_string = (
    f"DRIVER={AZURE_SQL_DRIVER};"
    f"SERVER={AZURE_SQL_SERVER};"
    f"DATABASE={AZURE_SQL_DATABASE};"
    f"UID={AZURE_SQL_USER};"
    f"PWD={AZURE_SQL_PASSWORD};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
)
params = urllib.parse.quote_plus(connection_string)
DATABASE_URL = f"mssql+pyodbc:///?odbc_connect={params}"

# ✅ Create SQLAlchemy Engine
engine = create_engine(DATABASE_URL)

# ✅ Allowed table names for security
VALID_TABLES = {
    "answered_outbound_calls",
    "answered_inbound_calls",
    "missed_outbound_calls",
    "missed_inbound_calls"
}

def insert_into_db(table_name, data):
    """ Inserts call data into Azure SQL database """
    if table_name not in VALID_TABLES:
        logging.error(f"❌ Invalid table name: {table_name}")
        return {"error": "Invalid table name"}, 400

    try:
        with engine.connect() as connection:
            sql = text(f"""
            INSERT INTO {table_name} (
                callID, dispnumber, caller_id, start_time, answer_stamp, end_time,
                callType, call_duration, destination, status, resource_url, missedFrom, hangup_cause
            ) VALUES (:callID, :dispnumber, :caller_id, :start_time, :answer_stamp, :end_time,
                      :callType, :call_duration, :destination, :status, :resource_url, :missedFrom, :hangup_cause)
            """)

            # Convert datetime fields
            data["start_time"] = parse_datetime(data.get("start_time"))
            data["answer_stamp"] = parse_datetime(data.get("answer_stamp"))
            data["end_time"] = parse_datetime(data.get("end_time"))

            connection.execute(sql, **data)
            logging.info(f"✅ Data inserted into {table_name}")
            return {"message": f"Data inserted into {table_name}"}, 200

    except Exception as e:
        logging.error(f"❌ Database Error: {str(e)}")
        return {"error": str(e)}, 500

def parse_datetime(value):
    """ Parses datetime string into Python datetime object """
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value else None
    except (ValueError, TypeError):
        logging.warning(f"⚠ Invalid datetime format: {value}")
        return None

# ✅ Azure Function HTTP Trigger
app = func.FunctionApp()

def handle_call_data(req: func.HttpRequest, table_name: str) -> func.HttpResponse:
    """ Generic function to insert Tata Tele call data into the right table """
    logging.info(f"📩 Data received for {table_name}")

    try:
        data = req.get_json()
        if not data:
            return func.HttpResponse(json.dumps({"error": "Invalid request data"}), status_code=400)

        response, status = insert_into_db(table_name, data)
        return func.HttpResponse(json.dumps(response), status_code=status)

    except Exception as e:
        logging.error(f"❌ Error processing request: {str(e)}")
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500)

# ✅ Separate Endpoints for Tata Tele Triggers
@app.function_name(name="AnsweredOutboundCalls")
@app.route(route="answered-outbound", methods=["POST"])
def answered_outbound_handler(req: func.HttpRequest) -> func.HttpResponse:
    return handle_call_data(req, "answered_outbound_calls")

@app.function_name(name="AnsweredInboundCalls")
@app.route(route="answered-inbound", methods=["POST"])
def answered_inbound_handler(req: func.HttpRequest) -> func.HttpResponse:
    return handle_call_data(req, "answered_inbound_calls")

@app.function_name(name="MissedOutboundCalls")
@app.route(route="missed-outbound", methods=["POST"])
def missed_outbound_handler(req: func.HttpRequest) -> func.HttpResponse:
    return handle_call_data(req, "missed_outbound_calls")

@app.function_name(name="MissedInboundCalls")
@app.route(route="missed-inbound", methods=["POST"])
def missed_inbound_handler(req: func.HttpRequest) -> func.HttpResponse:
    return handle_call_data(req, "missed_inbound_calls")
