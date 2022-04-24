#!/usr/bin/env python3

import asyncio
import json
import telegram
import time
import requests
import tweepy
import mariadb
from credentials import *
from datetime import datetime
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
  cur.execute("CREATE TABLE IF NOT EXISTS avax_kraken_watch (TimeStamp bigint, BlockNumber bigint, AVAXValue float, TransactionHash text);")
  cur.execute("CREATE TABLE IF NOT EXISTS avax_kraken_rewards (TimeStamp bigint, AVAXRewards float);")

  cur_remote = conn_remote.cursor()
  cur_remote.execute("CREATE TABLE IF NOT EXISTS avax_kraken_watch (TimeStamp bigint, BlockNumber bigint, AVAXValue float, TransactionHash text);")
  cur_remote.execute("CREATE TABLE IF NOT EXISTS avax_kraken_rewards (TimeStamp bigint, AVAXRewards float);")


  query = f"SELECT BlockNumber FROM avax_kraken_watch;"

#  cur.execute(query)
#  avax_block_numbers = cur.fetchall()

  cur_remote.execute(query)
  avax_block_numbers = cur_remote.fetchall()
  avax_start_block_tuple = avax_block_numbers[-1]
  avax_start_block = avax_start_block_tuple[0] + 1

  print("Getting Kraken data from SnowTrace...")

  txns_request_url = "https://api.snowtrace.io/api?module=account&action=txlist&address=" + cross_chain_buyback_address + "&startblock=" + str(avax_start_block) + "&endblock=999999999&sort=asc&apikey=" + AVAX_API_KEY
  avax_txns = requests.get(txns_request_url)
  avax_txns_json = avax_txns.json()

  avax_txns_result = avax_txns_json["result"]

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
  everrise_data_avax = everrise_data_json["avax"]
  everrise_data_avax_current = everrise_data_avax["current"]
  everrise_rewards_avax = everrise_data_avax_current["rewards"]
  everrise_staked_avax = everrise_data_avax_current["staked"]
  everrise_multiplier_avax = everrise_data_avax_current["aveMultiplier"]

  query = f"SELECT TimeStamp,AVAXRewards from avax_kraken_rewards ORDER BY TimeStamp desc limit 1"
  cur_remote.execute(query)
  previous_rewards_avax_list = cur_remote.fetchall()
  previous_rewards_avax_tuple = previous_rewards_avax_list[0]
  previous_rewards_avax = previous_rewards_avax_tuple[0]
  current_rewards_avax = float(everrise_rewards_avax) - float(previous_rewards_avax)
  staker_percentage_avax = (float(current_rewards_avax) * 36) / (float(everrise_staked_avax) * float(everrise_multiplier_avax)) * 100

  for item in avax_txns_result:
    if item['from'] == '0x3bb730e6f651007f2963b627c0d992cfc120a352':
      avax_kraken_found = 1
    else:
      avax_kraken_found = 0
    if avax_kraken_found == 1:
      txn_hash = item['hash']
      print("\nKraken Found!\nTransaction Hash=",txn_hash)
      internal_txns_url = "https://api.snowtrace.io/api?module=account&action=txlistinternal&txhash=" + txn_hash + "&apikey=" + AVAX_API_KEY
      avax_internal_txns = requests.get(internal_txns_url)
      avax_internal_txns_json = avax_internal_txns.json()
      avax_internal_txns_result = avax_internal_txns_json["result"]
      print(avax_internal_txns_result)
      if avax_internal_txns_result == []:
        avax_value = 0
      else:
        avax_value = int(avax_internal_txns_result[0]['value'])/1000000000000000000
      avax_kaboom_datetime = datetime.utcfromtimestamp(int(item['timeStamp']))
      print('\nDate/Time of Kaboom: ',avax_kaboom_datetime)
      print('Block Number:        ',item['blockNumber'])
      print('AVAX Value:          ',avax_value)

      cur.execute("INSERT INTO avax_kraken_watch (TimeStamp,BlockNumber,AVAXValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],avax_value,item['hash']))
      conn.commit()

      cur_remote.execute("INSERT INTO avax_kraken_watch (TimeStamp,BlockNumber,AVAXValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],avax_value,item['hash']))
      conn_remote.commit()

      cur.execute("INSERT INTO avax_kraken_rewards (TimeStamp,AVAXRewards) VALUES (?, ?)", (item['timeStamp'],everrise_rewards_avax))
      conn.commit()

      cur_remote.execute("INSERT INTO avax_kraken_rewards (TimeStamp,AVAXRewards) VALUES (?, ?)", (item['timeStamp'],everrise_rewards_avax))
      conn_remote.commit()

      avax_message = "*AVAX Kraken Sighting:*\n*Date/Time of Kaboom:* " + str(avax_kaboom_datetime) + "\n*Block Number:*               " + str(item['blockNumber'])+ "\n*AVAX Value:*                    " + str(avax_value) + "\n*RISE Rewards Distributed:*      " + str(current_rewards_avax) + " RISE\n*36x Stake Performance:*          " + str(staker_percentage_avax) + "%"

      bot.send_message(chat_id=CHAT_ID, text=avax_message, parse_mode=telegram.ParseMode.MARKDOWN)

      avax_message = "#EverRise $RISE\n\nAVAX Kraken Sighting:\n\nDate/Time of Kaboom: " + str(avax_kaboom_datetime) + "\nAVAX Block Number:          " + str(item['blockNumber']) + "\nAVAX Value:                     " + str(avax_value)
      api.update_status(status=avax_message)

if __name__ == "__main__":
  asyncio.run(main())
