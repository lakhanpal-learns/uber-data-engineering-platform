from flask import Flask, jsonify, render_template
from databricks import sql
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Create Flask application
app = Flask(__name__)


# ============================================================
# DATABRICKS CONNECTION
# ============================================================

def get_connection():

    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )


# ============================================================
# EXECUTE SQL QUERY
# ============================================================

def execute_query(query):

    connection = get_connection()
    cursor = connection.cursor()

    try:

        cursor.execute(query)

        # Get column names
        columns = [
            column[0]
            for column in cursor.description
        ]

        # Get query results
        rows = cursor.fetchall()

        # Convert rows into dictionaries
        return [
            dict(zip(columns, row))
            for row in rows
        ]

    finally:

        cursor.close()
        connection.close()


# ============================================================
# HOME PAGE
# ============================================================

@app.route("/")
def home():

    return render_template("dashboard.html")


# ============================================================
# KPI API
# ============================================================

@app.route("/api/kpis")
def kpis():

    query = """
    SELECT
        COUNT(*) AS total_rides,
        ROUND(SUM(total_fare), 2) AS total_revenue,
        ROUND(AVG(total_fare), 2) AS average_fare,
        ROUND(AVG(rating), 2) AS average_rating,
        ROUND(SUM(tip_amount), 2) AS total_tips,
        ROUND(AVG(distance_miles), 2) AS average_distance,
        ROUND(AVG(duration_minutes), 2) AS average_duration
    FROM uber.bronze.fact
    """

    result = execute_query(query)

    return jsonify(result[0])


# ============================================================
# VEHICLE ANALYTICS
# ============================================================

@app.route("/api/vehicles")
def vehicles():

    query = """
    SELECT
        v.vehicle_type,
        COUNT(*) AS total_rides,
        ROUND(SUM(f.total_fare), 2) AS revenue
    FROM uber.bronze.fact f

    LEFT JOIN uber.bronze.dim_vehicle v
        ON f.vehicle_id = v.vehicle_id

    GROUP BY v.vehicle_type

    ORDER BY revenue DESC
    """

    return jsonify(execute_query(query))


# ============================================================
# PAYMENT ANALYTICS
# ============================================================

@app.route("/api/payments")
def payments():

    query = """
    SELECT
        p.payment_method,
        COUNT(*) AS total_rides,
        ROUND(SUM(f.total_fare), 2) AS revenue
    FROM uber.bronze.fact f

    LEFT JOIN uber.bronze.dim_payment p
        ON f.payment_method_id = p.payment_method_id

    GROUP BY p.payment_method

    ORDER BY revenue DESC
    """

    return jsonify(execute_query(query))


# ============================================================
# CITY ANALYTICS
# ============================================================

@app.route("/api/cities")
def cities():

    query = """
    SELECT
        l.pickup_city,
        COUNT(*) AS total_rides,
        ROUND(SUM(f.total_fare), 2) AS revenue
    FROM uber.bronze.fact f

    LEFT JOIN uber.bronze.dim_location l
        ON f.pickup_city_id = l.pickup_city_id

    GROUP BY l.pickup_city

    ORDER BY revenue DESC

    LIMIT 10
    """

    return jsonify(execute_query(query))


# ============================================================
# DRIVER ANALYTICS
# ============================================================

@app.route("/api/drivers")
def drivers():

    query = """
    SELECT
        d.driver_name,
        COUNT(*) AS total_rides,
        ROUND(SUM(f.total_fare), 2) AS revenue,
        ROUND(AVG(f.rating), 2) AS average_rating
    FROM uber.bronze.fact f

    LEFT JOIN uber.bronze.dim_driver d
        ON f.driver_id = d.driver_id

    GROUP BY d.driver_name

    ORDER BY revenue DESC

    LIMIT 10
    """

    return jsonify(execute_query(query))


# ============================================================
# APPLICATION START
# ============================================================

if __name__ == "__main__":

    app.run(
        host="127.0.0.1",
        port=5000,
        debug=True
    )