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
from bscscan import BscScan
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
  cur.execute("CREATE TABLE IF NOT EXISTS bsc_kraken_watch (TimeStamp bigint, BlockNumber bigint, BNBValue float, TransactionHash text);")
  cur.execute("CREATE TABLE IF NOT EXISTS bsc_kraken_rewards (TimeStamp bigint, BSCRewards float);")

  cur_remote = conn_remote.cursor()
  cur_remote.execute("CREATE TABLE IF NOT EXISTS bsc_kraken_watch (TimeStamp bigint, BlockNumber bigint, BNBValue float, TransactionHash text);")
  cur_remote.execute("CREATE TABLE IF NOT EXISTS bsc_kraken_rewards (TimeStamp bigint, BSCRewards float);")

  query = f"SELECT BlockNumber FROM bsc_kraken_watch;"

#  cur.execute(query)
#  bsc_block_numbers = cur.fetchall()

  cur_remote.execute(query)
  bsc_block_numbers = cur_remote.fetchall()

  bsc_start_block_tuple = bsc_block_numbers[-1]
  bsc_start_block = bsc_start_block_tuple[0] + 1

  async with BscScan(BSC_API_KEY) as bsc_client:
    print("Getting Kraken data from BSCScan...")
    bsc_internal_txns = await bsc_client.get_internal_txs_by_address(CONTRACT_ADDRESS,startblock=bsc_start_block,endblock=999999999,sort="asc")
    print("Success!")

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
  everrise_data_bsc = everrise_data_json["bsc"]
  everrise_data_bsc_current = everrise_data_bsc["current"]
  everrise_rewards_bsc = everrise_data_bsc_current["rewards"]
  everrise_staked_bsc = everrise_data_bsc_current["staked"]
  everrise_multiplier_bsc = everrise_data_bsc_current["aveMultiplier"]
  print(everrise_rewards_bsc)
  print(everrise_staked_bsc)
  print(everrise_multiplier_bsc)


  print("Getting Previous Rewards from database...")
  query = f"SELECT BSCRewards from bsc_kraken_rewards ORDER BY TimeStamp desc limit 1"
  cur_remote.execute(query)
  previous_rewards_bsc_list = cur_remote.fetchall()
  previous_rewards_bsc_tuple = previous_rewards_bsc_list[0]
  previous_rewards_bsc = previous_rewards_bsc_tuple[0]
  current_rewards_bsc = float(everrise_rewards_bsc) - float(previous_rewards_bsc)
  staker_percentage_bsc = (float(current_rewards_bsc) * 36) / (float(everrise_staked_bsc) * float(everrise_multiplier_bsc)) * 100
  print(staker_percentage_bsc)

  for item in bsc_internal_txns:
    if item['from'] == '0xc17c30e98541188614df99239cabd40280810ca3':
      if item['to'] == '0x10ed43c718714eb63d5aa57b78b54704e256024e':
        bsc_kraken_found = 1
      else:
        bsc_kraken_found = 0
    else:
      bsc_kraken_found = 0
    if bsc_kraken_found == 1:
      bnb_value = int(item['value'])/1000000000000000000
      bsc_kaboom_datetime = datetime.utcfromtimestamp(int(item['timeStamp']))
      print("Date/Time of Kaboom:   ",bsc_kaboom_datetime)
      print("TimeStamp:             ",item['timeStamp'])
      print("Block Number:          ",item['blockNumber'])
      print("BNB Value:             ",bnb_value)
      print("Transaction Hash:      ",item['hash'])

      cur.execute("INSERT INTO bsc_kraken_watch (TimeStamp,BlockNumber,BNBValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],bnb_value,item['hash']))
      conn.commit()

      cur_remote.execute("INSERT INTO bsc_kraken_watch (TimeStamp,BlockNumber,BNBValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],bnb_value,item['hash']))
      conn_remote.commit()

      cur.execute("INSERT INTO bsc_kraken_rewards (TimeStamp,BSCRewards) VALUES (?, ?)", (item['timeStamp'],float(everrise_rewards_bsc)))
      conn.commit()

      cur_remote.execute("INSERT INTO bsc_kraken_rewards (TimeStamp,BSCRewards) VALUES (?, ?)", (item['timeStamp'],float(everrise_rewards_bsc)))
      conn_remote.commit()

      bsc_message = "*BSC Kraken Sighting:*\n*Date/Time of Kaboom:* " + str(bsc_kaboom_datetime) + "\n*Block Number:*               " + str(item['blockNumber']) + "\n*BNB Value:*                      " + str(bnb_value) + "\n*RISE Rewards Distributed:*      " + str(current_rewards_bsc) + " RISE\n*36x Stake Performance:*          " + str(staker_percentage_bsc) + "%"
      bot.send_message(chat_id=CHAT_ID, text=bsc_message, parse_mode=telegram.ParseMode.MARKDOWN)

      bsc_message = "#EverRise $RISE\n\nBSC Kraken Sighting:\n\nDate/Time of Kaboom: " + str(bsc_kaboom_datetime) + "\nBSC Block Number:        " + str(item['blockNumber']) + "\nBNB Value:                     " + str(bnb_value)
      api.update_status(status=bsc_message)


if __name__ == "__main__":
  asyncio.run(main())
