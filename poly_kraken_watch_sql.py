#!/usr/bin/env python3

import asyncio
import json
import telegram
import time
import tweepy
import mariadb
import requests
from credentials import *
from datetime import datetime
from polygonscan import PolygonScan
from config import *

async def main():
  print("Connecting to local MariaDB Instance...")
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

  print("Connecting to remote MariaDB Instance...")
  try:
    conn_remote = mariadb.connect(
      user=MARIADB_REMOTE_USER,
      password=MARIADB_REMOTE_PW,
      host=MARIADB_REMOTE_HOST,
      port=3306,
      database=MARIADB_REMOTE_DB
    )
    print("Success!")
  except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")

  cur = conn.cursor()
  cur.execute("CREATE TABLE IF NOT EXISTS poly_kraken_watch (TimeStamp bigint, BlockNumber bigint, MATICValue float, TransactionHash text);")
  cur.execute("CREATE TABLE IF NOT EXISTS poly_kraken_rewards (TimeStamp bigint, MATICRewards float);")

  cur_remote = conn_remote.cursor()
  cur_remote.execute("CREATE TABLE IF NOT EXISTS poly_kraken_watch (TimeStamp bigint, BlockNumber bigint, MATICValue float, TransactionHash text);")
  cur_remote.execute("CREATE TABLE IF NOT EXISTS poly_kraken_rewards (TimeStamp bigint, MATICRewards float);")

  query = f"SELECT BlockNumber FROM poly_kraken_watch;"

#  cur.execute(query)
#  poly_block_numbers = cur.fetchall()

  cur_remote.execute(query)
  poly_block_numbers = cur_remote.fetchall()

  poly_start_block_tuple = poly_block_numbers[-1]
  poly_start_block = poly_start_block_tuple[0] + 1

  print("Getting Kraken data from PolygonScan...")
  with PolygonScan(POLY_API_KEY,False) as poly_client:
    poly_internal_txns = poly_client.get_internal_txs_by_address(CONTRACT_ADDRESS,startblock=poly_start_block,endblock=999999999,sort="asc")

  print("Connecting to Telegram Bot...")
  try:
    bot = telegram.Bot(token=TELEGRAM_API_KEY)
    print("Success!")
  except telegram.Error as e:
    print(f"Error connecting to Telegram: {e}")

  print("Connecting to Twitter...")
  auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  api = tweepy.API(auth)
  print("Success!")

  current_time = datetime.utcnow()
  print("Current Time:        ",current_time)

  print("Getting EverRise data...")
  everrise_data = requests.get("https://everrise.azurewebsites.net/stats")
  everrise_data_json = everrise_data.json()
  everrise_data_poly = everrise_data_json["poly"]
  everrise_data_poly_current = everrise_data_poly["current"]
  everrise_rewards_poly = everrise_data_poly_current["rewards"]
  print(everrise_rewards_poly)

  query = f"SELECT TimeStamp,MATICRewards from poly_kraken_rewards ORDER BY TimeStamp desc limit 1"
  cur_remote.execute(query)
  previous_rewards_poly = cur_remote.fetchall()
  print(previous_rewards_poly)
#  current_rewards_bsc = everrise_rewards_bsc - previous_rewards_bsc

  for item in poly_internal_txns:
    if item['from'] == '0xc17c30e98541188614df99239cabd40280810ca3':
      if item['to'] == '0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff':
        poly_kraken_found = 1
      else:
        poly_kraken_found = 0
    else:
      poly_kraken_found = 0
    if poly_kraken_found == 1:
      poly_value = int(item['value'])/1000000000000000000
      poly_kaboom_datetime = datetime.utcfromtimestamp(int(item['timeStamp']))
      print("Date/Time of Kaboom: ",poly_kaboom_datetime)
      print("Block Number:        ",item['blockNumber'])
      print("Poly Value:          ",poly_value)

      cur.execute("INSERT INTO poly_kraken_watch (TimeStamp,BlockNumber,MATICValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],poly_value,item['hash']))
      conn.commit()

      cur_remote.execute("INSERT INTO poly_kraken_watch (TimeStamp,BlockNumber,MATICValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],poly_value,item['hash']))
      conn_remote.commit()

      cur.execute("INSERT INTO poly_kraken_rewards (TimeStamp,MATICRewards) VALUES (?, ?)", (item['timeStamp'],everrise_rewards_poly))
      conn.commit()

      cur_remote.execute("INSERT INTO poly_kraken_rewards (TimeStamp,MATICRewards) VALUES (?, ?)", (item['timeStamp'],everrise_rewards_poly))
      conn_remote.commit()


      poly_message = "*Poly Kraken Sighting:*\n*Date/Time of Kaboom:* " + str(poly_kaboom_datetime) + "\n*Block Number:*               " + str(item['blockNumber'])+ "\n*MATIC Value:*                  " + str(poly_value)
      bot.send_message(chat_id=CHAT_ID, text=poly_message, parse_mode=telegram.ParseMode.MARKDOWN)

      poly_message = "#EverRise $RISE\n\nPoly Kraken Sighting:\n\nDate/Time of Kaboom: " + str(poly_kaboom_datetime) + "\nPoly Block Number:           " + str(item['blockNumber']) + "\nMATIC Value:                   " + str(poly_value)
      api.update_status(status=poly_message)

if __name__ == "__main__":
  asyncio.run(main())
