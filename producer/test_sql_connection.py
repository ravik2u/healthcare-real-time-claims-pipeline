import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=RAVI\\SQLEXPRESS;"
    "DATABASE=Healthcare_Claims_DB;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM Claims")

count = cursor.fetchone()[0]

print(f"Claims count = {count}")

conn.close()