import happybase
import pandas as pd
import time

def connectHbase(host, port):
  try:
     connection = happybase.Connection(host, port)
     return connection
  except Exception as ex:
    print("Coonection failed:", str(ex))
    raise

def createTable(connection, tableName):
  try:
    if tableName.encode("utf-8") not in connection.tables():
      connection.create_table(
        tableName,
        {
          "cf": dict()
        }
      )
      print("Created table")
  except Exception as ex:
    print("Error creating table", str(ex))
    raise

def readData():
  return pd.read_csv("./files/bigdata_task3.csv")

def insertData(connection, tableName, df):
  try:
    table = connection.table(tableName)
    for index, row in df.iterrows():
      key = str(index)
      data = {
        "cf:date": str(row["Date"]),
        "cf:symbol": str(row["Symbol"]),
        "cf:open": str(row["Open"]),
        "cf:high": str(row["High"]),
        "cf:low": str(row["Low"]),
        "cf:close": str(row["Close"]),
        "cf:volume": str(row["Volume"]),
        "cf:currency": str(row["Currency"])
      }
      table.put(key, data)
      print(f"insert: key={key}, data={data}")
  except Exception as ex:
    print("Error inserting data", str(ex))
    raise

def insertOperation(connection, tableName, data):
  try:
    table = connection.table(tableName)
    key = str(int(time.time() * 1000))
    table.put(key, data)
    print(f"insert: key={key}, data={data}")
  except Exception as ex:
    print("Error inserting data", str(ex))
    raise

def updateOperation(connection, tableName, key, data):
  try:
    table = connection.table(tableName)
    table.put(key, data)
    print(f"Updated: key={key}, data={data}")
  except Exception as ex:
    print("Error inserting data", str(ex))
    raise

def deleteOperation(connection, tableName, key):
  try:
    table = connection.table(tableName)
    table.delete(key)
    print(f"Record deleted with key {key}")
  except Exception as ex:
    print("Error inserting data", str(ex))
    raise

def selectOperation(connection, tableName, limit):
  try:
    table = connection.table(tableName)
    rows = table.scan(limit=limit)
    for index, row in rows:
      print(f"Key {index}, Data {row}")
  except Exception as ex:
    print("Error select", str(ex))
    raise

def iterationOperation(connection, tableName, open, close):
  try:
    table = connection.table(tableName)
    rows = table.scan()
    for index, row in rows:
      openValue = float(row.get(b'cf:open', b'0').decode())
      closeValue = float(row.get(b'cf:close', b'0').decode())
      if openValue > open and closeValue < close:
        print(f"Key {index}, Data {row}")
  except Exception as ex:
    print("Error select", str(ex))
    raise

def filterOperation(connection, tableName, valueFilter):
  try:
    table = connection.table(tableName)
    rows = table.scan()
    for index, row in rows:
      highValue = float(row.get(b'cf:high', b'0').decode())
      if highValue >= valueFilter:
        print(f"Key {index}, Data {row}")
  except Exception as ex:
    print("Error select", str(ex))
    raise

def run():
  tableName = 'fuel_prices'
  key = str(1732308741864)
  connection = connectHbase(host="localhost", port=9090)
  connection.open()
  #createTable(connection=connection, tableName=tableName)
  df = readData()
  #insertData(connection=connection, tableName=tableName, df=df)
  dataInsert = {
    "cf:date": str("2003-01-20"),
    "cf:symbol": str("XOM"),
    "cf:open": str("25.10"),
    "cf:high": str("39.43"),
    "cf:low": str("45.91"),
    "cf:close": str("36.57"),
    "cf:volume": str("13873916"),
    "cf:currency": str("USD")
  }
  #insertOperation(connection=connection, tableName=tableName, data=dataInsert)
  #updateOperation(connection=connection, tableName=tableName, key=key, data=dataInsert)
  #deleteOperation(connection=connection, tableName=tableName, key=key)
  #selectOperation(connection=connection, tableName=tableName, limit=6)
  #iterationOperation(connection=connection, tableName=tableName, open=25.10, close=40.00)
  filterOperation(connection=connection, tableName=tableName, valueFilter=39.50)
  connection.close()

if __name__ == '__main__':
    run()