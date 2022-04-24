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
from etherscan import Etherscan
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
  cur.execute("CREATE TABLE IF NOT EXISTS eth_kraken_watch (TimeStamp bigint, BlockNumber bigint, ETHValue float, TransactionHash text);")
  cur.execute("CREATE TABLE IF NOT EXISTS eth_kraken_rewards (TimeStamp bigint, ETHRewards float);")

  cur_remote = conn_remote.cursor()
  cur_remote.execute("CREATE TABLE IF NOT EXISTS eth_kraken_watch (TimeStamp bigint, BlockNumber bigint, ETHValue float, TransactionHash text);")
  cur_remote.execute("CREATE TABLE IF NOT EXISTS eth_kraken_rewards (TimeStamp bigint, ETHRewards float);")


  query = f"SELECT BlockNumber FROM eth_kraken_watch;"

#  cur.execute(query)
#  eth_block_numbers = cur.fetchall()

  cur_remote.execute(query)
  eth_block_numbers = cur_remote.fetchall()

  eth_start_block_tuple = eth_block_numbers[-1]
  eth_start_block = eth_start_block_tuple[0] + 1

  print("Getting Kraken data from EtherScan...")
  eth_client = Etherscan(ETH_API_KEY)
  eth_internal_txns = eth_client.get_internal_txs_by_address(CONTRACT_ADDRESS,startblock=eth_start_block,endblock=999999999,sort="asc")
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
  print('Current Time:        ',current_time)

  print("Getting EverRise data...")
  everrise_data = requests.get("https://everrise.azurewebsites.net/stats")
  everrise_data_json = everrise_data.json()
  everrise_data_eth = everrise_data_json["eth"]
  everrise_data_eth_current = everrise_data_eth["current"]
  everrise_rewards_eth = everrise_data_eth_current["rewards"]
  everrise_staked_eth = everrise_data_eth_current["staked"]
  everrise_multiplier_eth = everrise_data_eth_current["aveMultiplier"]
  print(everrise_rewards_eth)
  print(everrise_staked_eth)
  print(everrise_multiplier_eth)

  query = f"SELECT ETHRewards from eth_kraken_rewards ORDER BY TimeStamp desc limit 1"
  cur_remote.execute(query)
  previous_rewards_eth_list = cur_remote.fetchall()
  previous_rewards_eth_tuple = previous_rewards_eth_list[0]
  previous_rewards_eth = previous_rewards_eth_tuple[0]
  current_rewards_eth = float(everrise_rewards_eth) - float(previous_rewards_eth)
  staker_percentage_eth = (float(current_rewards_eth) * 36) / (float(everrise_staked_eth) * float(everrise_multiplier_eth)) * 100

  print(staker_percentage_eth)

  for item in eth_internal_txns:
    if item['from'] == '0xc17c30e98541188614df99239cabd40280810ca3':
      if item['to'] == '0x7a250d5630b4cf539739df2c5dacb4c659f2488d':
        eth_kraken_found = 1
      else:
        eth_kraken_found = 0
    else:
      eth_kraken_found = 0
    if eth_kraken_found == 1:
      eth_value = int(item['value'])/1000000000000000000
      eth_kaboom_datetime = datetime.utcfromtimestamp(int(item['timeStamp']))
      print("Date/Time of Kaboom: ",eth_kaboom_datetime)
      print("Block Number:        ",item['blockNumber'])
      print("ETH Value:           ",eth_value)

      cur.execute("INSERT INTO eth_kraken_watch (TimeStamp,BlockNumber,ETHValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],eth_value,item['hash']))
      conn.commit()

      cur_remote.execute("INSERT INTO eth_kraken_watch (TimeStamp,BlockNumber,ETHValue,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],eth_value,item['hash']))
      conn_remote.commit()

      cur.execute("INSERT INTO eth_kraken_rewards (TimeStamp,ETHRewards) VALUES (?, ?)", (item['timeStamp'],everrise_rewards_eth))
      conn.commit()

      cur_remote.execute("INSERT INTO eth_kraken_rewards (TimeStamp,ETHRewards) VALUES (?, ?)", (item['timeStamp'],everrise_rewards_eth))
      conn_remote.commit()


      eth_message = "*ETH Kraken Sighting:*\n*Date/Time of Kaboom:* " + str(eth_kaboom_datetime) + "\n*Block Number:*               " + str(item['blockNumber']) + "\n*ETH Value:*                      " + str(eth_value)
      bot.send_message(chat_id=CHAT_ID, text=eth_message, parse_mode=telegram.ParseMode.MARKDOWN)

      eth_message = "#EverRise $RISE\n\nETH Kraken Sighting:\n\nDate/Time of Kaboom: " + str(eth_kaboom_datetime) + "\nETH Block Number:        " + str(item['blockNumber']) + "\nETH Value:                      " + str(eth_value)
      api.update_status(status=eth_message)


if __name__ == "__main__":
  asyncio.run(main())
