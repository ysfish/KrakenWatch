#!/usr/bin/env python3

import asyncio
import json
import telegram
import time
import tweepy
import requests
import mariadb
import requests
from credentials import *
from datetime import datetime
from ftmscan import ftmScan
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
  cur.execute("CREATE TABLE IF NOT EXISTS ftm_kraken_watch (TimeStamp bigint, BlockNumber bigint, FTMValue float, TransactionHash text);")
  cur.execute("CREATE TABLE IF NOT EXISTS ftm_kraken_rewards (TimeStamp bigint, FTMRewards float);")

  cur_remote = conn_remote.cursor()
  cur_remote.execute("CREATE TABLE IF NOT EXISTS ftm_kraken_watch (TimeStamp bigint, BlockNumber bigint, FTMValue float, TransactionHash text);")
  cur_remote.execute("CREATE TABLE IF NOT EXISTS ftm_kraken_rewards (TimeStamp bigint, FTMRewards float);")

  query = f"SELECT BlockNumber FROM ftm_kraken_watch;"

#  cur.execute(query)
#  ftm_block_numbers = cur.fetchall()

  cur_remote.execute(query)
  ftm_block_numbers = cur_remote.fetchall()

  ftm_start_block_tuple = ftm_block_numbers[-1]
  ftm_start_block = ftm_start_block_tuple[0] + 1

  print("Getting Kraken data from FTMScan...")
  txns_request_url = "https://api.ftmscan.com/api?module=account&action=txlist&address=" + cross_chain_buyback_address + "&startblock=" + str(ftm_start_block) + "&endblock=999999999&sort=asc&apikey=" + FTM_API_KEY
  ftm_txns = requests.get(txns_request_url)
  ftm_txns_json = ftm_txns.json()
  ftm_txns_result = ftm_txns_json["result"]

  print("Connecting to Telegram Bot...")
  try:
    bot = telegram.Bot(token='5196129266:AAE4O7naJ5rj6ewu_FnSByipQWswbAPmvxA')
    print("Success!")
  except telegram.Error as e:
    print(f"Error connecting to Telegram: {e}")

  print("Connecting to Twitter...")
  auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  api = tweepy.API(auth)
  print("Success!")

  current_time = datetime.utcnow()
  print('Current Time:        ',current_time)

  print("Getting EverRise data...")
  everrise_data = requests.get("https://everrise.azurewebsites.net/stats")
  everrise_data_json = everrise_data.json()
  everrise_data_ftm = everrise_data_json["ftm"]
  everrise_data_ftm_current = everrise_data_ftm["current"]
  everrise_rewards_ftm = everrise_data_ftm_current["rewards"]
  print(everrise_rewards_ftm)

  query = f"SELECT TimeStamp,FTMRewards from ftm_kraken_rewards ORDER BY TimeStamp desc limit 1"
  cur_remote.execute(query)
  previous_rewards_ftm = cur_remote.fetchall()
  print(previous_rewards_ftm)
#  current_rewards_ftm = everrise_rewards_ftm - previous_rewards_ftm

  for item in ftm_txns_result:
    if item['from'] == '0x3bb730e6f651007f2963b627c0d992cfc120a352':
      ftm_kraken_found = 1
    else:
      ftm_kraken_found = 0
    if ftm_kraken_found == 1:
      txn_hash = item['hash']
      print("Kraken Found!")
      ftm_internal_txns_url = "https://api.ftmscan.com/api?module=account&action=txlistinternal&txhash=" + txn_hash + "&apikey=" + FTM_API_KEY
      ftm_internal_txns = requests.get(ftm_internal_txns_url)
      ftm_internal_txns_json = ftm_internal_txns.json()
      ftm_internal_txns_result = ftm_internal_txns_json["result"]
      print(ftm_internal_txns_result)
      if ftm_internal_txns_result == []:
        ftm_value = 0
      else:
        ftm_value = int(ftm_internal_txns_result[0]['value'])/1000000000000000000
      ftm_kaboom_datetime = datetime.utcfromtimestamp(int(item['timeStamp']))
      print('Date/Time of Kaboom: ',ftm_kaboom_datetime)
      print('Block Number:        ',item['blockNumber'])
      print('FTM Value:          ',ftm_value)
      print("Transaction Hash:      ",item['hash'])

      cur.execute("INSERT INTO ftm_kraken_watch (TimeStamp,BlockNumber,FTMValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],ftm_value,item['hash']))
      conn.commit()

      cur_remote.execute("INSERT INTO ftm_kraken_watch (TimeStamp,BlockNumber,FTMValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],ftm_value,item['hash']))
      conn_remote.commit()

      cur.execute("INSERT INTO ftm_kraken_rewards (TimeStamp,FTMRewards) VALUES (?, ?)", (item['timeStamp'],everrise_rewards_ftm))
      conn.commit()

      cur_remote.execute("INSERT INTO ftm_kraken_rewards (TimeStamp,FTMRewards) VALUES (?, ?)", (item['timeStamp'],everrise_rewards_ftm))
      conn_remote.commit()


      ftm_message = "*FTM Kraken Sighting:*\n*Date/Time of Kaboom:* " + str(ftm_kaboom_datetime) + "\n*Block Number:*               " + str(item['blockNumber']) + "\n*FTM Value:*                      " + str(ftm_value)
      bot.send_message(chat_id=CHAT_ID, text=ftm_message, parse_mode=telegram.ParseMode.MARKDOWN)

      ftm_message = "#EverRise $RISE\n\nFTM Kraken Sighting:\n\nDate/Time of Kaboom: " + str(ftm_kaboom_datetime) + "\nFTM Block Number:           " + str(item['blockNumber']) + "\nFTM Value:                     " + str(ftm_value)
      api.update_status(status=ftm_message)

if __name__ == "__main__":
  asyncio.run(main())
