#!/usr/bin/env python3

import asyncio
import json
import telegram
import time
import tweepy
import mariadb
import requests
from credentials import *
from statistics import mean, stdev
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
  cur_remote = conn_remote.cursor()

  current_time = datetime.now()
  time_stamp = int(round(current_time.timestamp()))

  print("Getting Current Block data from BSCScan...")
  async with BscScan(BSC_API_KEY) as bsc_client:
    bsc_block_number = await bsc_client.get_block_number_by_timestamp(time_stamp,"before")
    if bsc_block_number == "Error! No closest block found":
      bsc_error = 1
    else:
      bsc_error = 0
    if bsc_error == 0:
      print("Success!")

  print("Current Time:        ",current_time)
  print("Time Stamp:          ",time_stamp)
  print("BSC Block Number:    ",bsc_block_number)

  everrise_data = requests.get("https://everrise.azurewebsites.net/stats")
  everrise_data_json = everrise_data.json()

  print("\neverrise_data_json:\n")
  print(everrise_data_json)

#  everrise_data_unified = everrise_data_json["unified"]
#  print("\neverrise_data_unified:\n")
#  print(everrise_data_unified)

#  everrise_data_unified_24hour = everrise_data_unified["history24hrs"]
#  print("\neverrise_data_unified_24hour:\n")
#  print(everrise_data_unified_24hour)

#  everrise_data_unified_current = everrise_data_unified["current"]
#  print("\neverrise_data_unified_current:\n")
#  print(everrise_data_unified_current)

#  everrise_volume_trade_24hour = everrise_data_unified_24hour["volumeTrade"]
#  print("\neverrise_volume_trade_24hour:\n")
#  print(everrise_volume_trade_24hour)

#  everrise_usd_reserves_balance = everrise_data_unified_24hour["usdReservesBalance"]
#  print("\neverrise_usd_reserves_balance:\n")
#  print(everrise_usd_reserves_balance)

  everrise_data_bsc = everrise_data_json["bsc"]
  print("\neverrise_data_bsc:\n")
  print(everrise_data_bsc)

  everrise_data_bsc_current = everrise_data_bsc["current"]
  print("\neverrise_data_bsc_current:\n")
  print(everrise_data_bsc_current)

#  print("Connecting to Telegram Bot...")
#  try:
#    bot = telegram.Bot(token=TELEGRAM_API_KEY)
#    print("Success!")
#  except telegram.Error as e:
#    print(f"Error connecting to Telegram: {e}")

#  print("Connecting to Twitter...")
#  auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
#  auth.set_access_token(access_token, access_token_secret)
#  api = tweepy.API(auth)
#  print("Success!")

#  bsc_telegram_message = "\n*Average BSC Blocks Per Kaboom:*     " + str(round(bsc_block_average,1)) + "\n*BSC Blocks Since Last Kaboom:*        " + str(bsc_blocks_elapsed) + "\n*BSC Blocks Left:*                                     " + str(bsc_blocks_left) + "\n*BSC Blocks Per Hour:*                            " + str(bsc_block_rate) + "\n*Hours Until Next Kaboom:*                    " + str(time_until_kaboom) + "\n\n*Total Tokens Burned:*            " + str(bsc_burn_total)
#  bot.send_message(chat_id=CHAT_ID, text=bsc_telegram_message, parse_mode=telegram.ParseMode.MARKDOWN)

#  bsc_twitter_message = "#EverRise $RISE\n\nAverage BSC Blocks Per Kaboom:     " + str(round(bsc_block_average,1)) + "\nBSC Blocks Since Last Kaboom:        " + str(bsc_blocks_elapsed) + "\nBSC Blocks Left:                                   " + str(bsc_blocks_left) + "\nBSC Blocks Per Hour:                           " + str(bsc_block_rate) + "\nHours Until Next Kaboom:                   " + str(time_until_kaboom)
#  api.update_status(status=bsc_twitter_message)




if __name__ == "__main__":
  asyncio.run(main())
