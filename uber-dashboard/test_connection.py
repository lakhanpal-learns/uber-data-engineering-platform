from databricks import sql
from dotenv import load_dotenv
import os

load_dotenv()

print("Connecting to Databricks...")

connection = sql.connect(
    server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_TOKEN")
)

cursor = connection.cursor()

cursor.execute("SELECT 1")

result = cursor.fetchone()

print("Connection successful!")
print("Result:", result)

cursor.close()
connection.close()