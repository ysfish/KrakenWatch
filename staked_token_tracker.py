#!/usr/bin/env python3

import mariadb
import requests
from credentials import *
from config import *
from datetime import datetime

def main():
  conn = new_func()
  cur = conn.cursor()

  query="CREATE TABLE IF NOT EXISTS staked_tokens (TimeStamp bigint, TotalStaked float, BSCStaked float, ETHStaked float, POLYStaked float, FTMStaked float, AVAXStaked float);"
  cur.execute(query)

  current_time = datetime.utcnow()
  print("Current Time:        ",current_time)
  time_stamp = datetime.timestamp(current_time)
  print("Timestamp:           ",time_stamp)

  print("Getting data from EverRise Stats...")
  everrise_stats_data_json = requests.get(EVERRISE_STATS).json()
  cur.execute("INSERT INTO staked_tokens VALUES (?, ?, ?, ?, ?, ?, ?)", (time_stamp, everrise_stats_data_json["unified"]["current"]["staked"], everrise_stats_data_json["bsc"]["current"]["staked"], everrise_stats_data_json["eth"]["current"]["staked"], everrise_stats_data_json["poly"]["current"]["staked"], everrise_stats_data_json["ftm"]["current"]["staked"], everrise_stats_data_json["avax"]["current"]["staked"]))
  conn.commit()

def new_func():
    print("Connecting to remote MariaDB Instance...")
    try:
      conn = mariadb.connect(
      user=MARIADB_USER,
      password=MARIADB_PW,
      host=MARIADB_HOST,
      port=3306,
      database=MARIADB_DB
    )
      print("Success!")
    except mariadb.Error as e:
      print(f"Error connecting to MariaDB Platform: {e}")
    return conn

if __name__ == "__main__":
    main()
