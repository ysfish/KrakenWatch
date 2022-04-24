#!/usr/bin/env python3

import asyncio
import json
import telegram
import time
import tweepy
import mariadb
import requests
from credentials import *
from config import *
from statistics import mean, stdev
from datetime import datetime
from bscscan import BscScan
from etherscan import Etherscan
from polygonscan import PolygonScan

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

  query = f"SELECT BlockNumber FROM bsc_kraken_watch;"

  cur.execute(query)
  cur_remote.execute(query)

#  bsc_block_numbers = cur.fetchall()
  bsc_block_numbers = cur_remote.fetchall()

  bsc_start_block_tuple = bsc_block_numbers[-1]
  bsc_start_block = bsc_start_block_tuple[0] + 1

  cur.execute("CREATE TABLE IF NOT EXISTS status_update (TimeStamp bigint, BlockNumber bigint);")
  cur.execute("CREATE TABLE IF NOT EXISTS burn_stats (TimeStamp bigint, TotalBurned bigint);")
  cur.execute("CREATE TABLE IF NOT EXISTS kraken_average (TimeStamp bigint, BlockNumber bigint, TimeStampAverage float, BlockNumberAverage float);")
  cur.execute("CREATE TABLE IF NOT EXISTS kraken_belly (TimeStamp bigint, BlockNumber bigint, BNBValue float, ETHValue float, MATICValue float, FTMValue float, AVAXValue float,TotalUSDValue float);")
  cur.execute("CREATE TABLE IF NOT EXISTS bsc_block_rate (TimeStamp bigint, BSCBlockRate bigint);")

  cur_remote.execute("CREATE TABLE IF NOT EXISTS status_update (TimeStamp bigint, BlockNumber bigint);")
  cur_remote.execute("CREATE TABLE IF NOT EXISTS burn_stats (TimeStamp bigint, TotalBurned bigint);")
  cur_remote.execute("CREATE TABLE IF NOT EXISTS kraken_average (TimeStamp bigint, BlockNumber bigint, TimeStampAverage float, BlockNumberAverage float);")
  cur_remote.execute("CREATE TABLE IF NOT EXISTS kraken_belly (TimeStamp bigint, BlockNumber bigint, BNBValue float, ETHValue float, MATICValue float, FTMValue float, AVAXValue float,TotalUSDValue float);")
  cur_remote.execute("CREATE TABLE IF NOT EXISTS bsc_block_rate (TimeStamp bigint, BSCBlockRate bigint);")

  async with BscScan(BSC_API_KEY) as bsc_client:
    print("Getting Kraken data from BSCScan...")
    bsc_kraken_belly = await bsc_client.get_bnb_balance(address=CONTRACT_ADDRESS)
    bsc_kraken_belly = float(bsc_kraken_belly) / 1000000000000000000
    print("Success!")

  print("Getting Kraken data from EtherScan...")
  eth_client = Etherscan(ETH_API_KEY)
  eth_kraken_belly = eth_client.get_eth_balance(address=CONTRACT_ADDRESS)
  eth_kraken_belly = float(eth_kraken_belly) / 1000000000000000000
  print("Success!")

  print("Getting Kraken data from PolyonScan...")
  with PolygonScan(POLY_API_KEY,False) as poly_client:
    poly_kraken_belly = poly_client.get_matic_balance(address=CONTRACT_ADDRESS)
  poly_kraken_belly = float(poly_kraken_belly) / 1000000000000000000
  print("Success!")

  print("Getting Kraken data from FTMScan...")
  ftm_request_url = "https://api.ftmscan.com/api?module=account&action=balance&address=" + CONTRACT_ADDRESS + "&tag=latest&apikey=" + FTM_API_KEY
  ftm_kraken_belly = requests.get(ftm_request_url)
  ftm_kraken_belly_json = ftm_kraken_belly.json()
  ftm_kraken_belly_result = float(ftm_kraken_belly_json["result"]) / 1000000000000000000
  print("Success!")

  print("Getting Kraken data from SnowTrace...")
  avax_request_url = "https://api.snowtrace.io/api?module=account&action=balance&address=" + CONTRACT_ADDRESS + "&tag=latest&apikey=" + AVAX_API_KEY
  avax_kraken_belly = requests.get(avax_request_url)
  avax_kraken_belly_json = avax_kraken_belly.json()
  avax_kraken_belly_result = float(avax_kraken_belly_json["result"]) / 1000000000000000000
  print("Success!")

  print("Getting data from EverRise Stats...")
  everrise_data = requests.get("https://everrise.azurewebsites.net/stats")
  everrise_data_json = everrise_data.json()
  everrise_data_unified = everrise_data_json["unified"]
  everrise_data_unified_current = everrise_data_unified["current"]
  everrise_data_unified_24hour = everrise_data_unified["history24hrs"]
  everrise_volume_trade_24hour = everrise_data_unified_24hour["volumeTrade"]
  everrise_usd_reserves_balance = everrise_data_unified_current["usdReservesBalance"]
  everrise_usd_reserves_balance = round(float(everrise_usd_reserves_balance),2)
  everrise_usd_reserves_balance_formatted = "{:,}".format(everrise_usd_reserves_balance)

  async with BscScan(BSC_API_KEY) as bsc_client:
    print("Getting Burn data from BSCScan...")
    bsc_burn_total = int(await bsc_client.get_acc_balance_by_token_contract_address(CONTRACT_ADDRESS,BURN_ADDRESS))/1000000000000000000
    bsc_burn_total_millions = bsc_burn_total / 1000000
    print("Success!")

    current_time = datetime.now()
    time_stamp = int(round(current_time.timestamp()))
    print("Getting Current Block data from BSCScan...")
    bsc_block_number = await bsc_client.get_block_number_by_timestamp(time_stamp,"before")
    if bsc_block_number == "Error! No closest block found":
      bsc_error = 1
    else:
      bsc_error = 0
    if bsc_error == 0:
      print("Success!")

  cur.execute("INSERT INTO kraken_belly (TimeStamp,BlockNumber,BNBValue,ETHValue,MATICValue,FTMValue,AVAXValue,TotalUSDValue) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (time_stamp,bsc_block_number,bsc_kraken_belly,eth_kraken_belly,poly_kraken_belly,ftm_kraken_belly_result,avax_kraken_belly_result,everrise_usd_reserves_balance))
  cur.execute("INSERT INTO burn_stats (TimeStamp,TotalBurned) VALUES (?, ?)", (time_stamp,bsc_burn_total))
  conn.commit()

  cur_remote.execute("INSERT INTO kraken_belly (TimeStamp,BlockNumber,BNBValue,ETHValue,MATICValue,FTMValue,AVAXValue,TotalUSDValue) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (time_stamp,bsc_block_number,bsc_kraken_belly,eth_kraken_belly,poly_kraken_belly,ftm_kraken_belly_result,avax_kraken_belly_result,everrise_usd_reserves_balance))
  cur_remote.execute("INSERT INTO burn_stats (TimeStamp,TotalBurned) VALUES (?, ?)", (time_stamp,bsc_burn_total))
  conn_remote.commit()


  query = f"SELECT TimeStamp,BlockNumber FROM bsc_kraken_watch ORDER BY TimeStamp desc limit 50"

  cur.execute(query)
  cur_remote.execute(query)

#  bsc_kabooms = cur.fetchall()
  bsc_kabooms = cur_remote.fetchall()

  bsc_kaboom_timestamps, bsc_kaboom_blocks = zip(*bsc_kabooms)
  bsc_kaboom_timestamp_difference = [float(bsc_kaboom_timestamps[i])-float(bsc_kaboom_timestamps[i+1]) for i in range(len(bsc_kaboom_timestamps)-1)]
  bsc_timestamp_average = mean(bsc_kaboom_timestamp_difference)
  bsc_kaboom_block_difference = [int(bsc_kaboom_blocks[i]-int(bsc_kaboom_blocks[i+1])) for i in range(len(bsc_kaboom_blocks)-1)]
  bsc_block_average = mean(bsc_kaboom_block_difference)

  cur.execute("INSERT INTO kraken_average (TimeStamp,BlockNumber,TimeStampAverage,BlockNumberAverage) VALUES (?, ?, ?, ?)", (time_stamp,bsc_block_number,bsc_timestamp_average,bsc_block_average))
  conn.commit()

  cur_remote.execute("INSERT INTO kraken_average (TimeStamp,BlockNumber,TimeStampAverage,BlockNumberAverage) VALUES (?, ?, ?, ?)", (time_stamp,bsc_block_number,bsc_timestamp_average,bsc_block_average))
  conn_remote.commit()

  query = f"SELECT TimeStamp,BlockNumber FROM status_update ORDER BY TimeStamp desc limit 1"

  cur.execute(query)
  bsc_status_update = cur.fetchall()

  cur_remote.execute(query)
  bsc_status_update = cur_remote.fetchall()

  bsc_timestamp_prev, bsc_block_number_prev = zip(*bsc_status_update)
  bsc_timestamp_previous = bsc_timestamp_prev[0]
  bsc_block_number_previous = bsc_block_number_prev[0]
  print(bsc_timestamp_previous)
  print(bsc_block_number_previous)

  bsc_timestamp_delta = int(time_stamp) - int(bsc_timestamp_previous)
  bsc_block_delta = int(bsc_block_number) - int(bsc_block_number_previous)
  bsc_block_rate = int(bsc_block_delta/bsc_timestamp_delta*3600)

  cur.execute("INSERT INTO bsc_block_rate (TimeStamp,BSCBlockRate) VALUES (?, ?)", (time_stamp,bsc_block_rate))
  cur.execute("INSERT INTO status_update (TimeStamp,BlockNumber) VALUES (?, ?)", (time_stamp,bsc_block_number))
  conn.commit()

  cur_remote.execute("INSERT INTO bsc_block_rate (TimeStamp,BSCBlockRate) VALUES (?, ?)", (time_stamp,bsc_block_rate))
  cur_remote.execute("INSERT INTO status_update (TimeStamp,BlockNumber) VALUES (?, ?)", (time_stamp,bsc_block_number))
  conn_remote.commit()


  bsc_blocks_elapsed = int(bsc_block_number) - int(bsc_start_block)
  bsc_blocks_left = int(bsc_block_average) - int(bsc_blocks_elapsed)
  if bsc_blocks_left < 0:
    hours_until_kaboom = 0
    minutes_until_kaboom = 0
  else:
    hours_until_kaboom , blocks_until_kaboom = divmod(int(bsc_blocks_left),int(bsc_block_rate))
    minutes_until_kaboom = round(int(blocks_until_kaboom)/int(bsc_block_rate)*60)

  time_until_kaboom = str(hours_until_kaboom) + ":" + str(minutes_until_kaboom).zfill(2)

  print("Current Time:                      ",current_time)
  print("BSC Time Stamp:                    ",time_stamp)
  print("Previous BSC Timestamp:            ",bsc_timestamp_previous)
  print("BSC Timestamp Delta:               ",bsc_timestamp_delta)
  print("BSC Block Number:                  ",bsc_block_number)
  print("Previous BSC Block:                ",bsc_block_number_previous)
  print("BSC Block Delta:                   ",bsc_block_delta)
  print("BSC Block Rate:                    ",bsc_block_rate)
  print("BSC Block Average Between Kabooms: ",round(bsc_block_average,1))
  print("Time Until Next Kaboom:            ",time_until_kaboom)

  print("Total Tokens Burned:               ",bsc_burn_total)

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

  bsc_telegram_message = "\n*Average BSC Blocks Per Kaboom:*     " + str(round(bsc_block_average)) + "\n*BSC Blocks Since Last Kaboom:*        " + str(bsc_blocks_elapsed) + "\n*BSC Blocks Left:*                                     " + str(bsc_blocks_left) + "\n*BSC Blocks Per Hour:*                            " + str(bsc_block_rate) + "\n*Hours Until Next Kaboom:*                    " + str(time_until_kaboom) + "\n\n*BSC Kraken Belly:*                         " + str(round(bsc_kraken_belly)) + " BNB\n*ETH Kraken Belly:*                        " + str(round(eth_kraken_belly,2)) + " ETH\n*MATIC Kraken Belly:*            " + str(round(poly_kraken_belly)) + " MATIC\n*FTM Kraken Belly:*                    " + str(round(ftm_kraken_belly_result)) + " FTM\n*AVAX Kraken Belly:*                    " + str(round(avax_kraken_belly_result)) + " AVAX\n*Kraken Total Reserves:*         $" + str(everrise_usd_reserves_balance_formatted) + "\n\n*Total Tokens Burned:*                 " + str(round(bsc_burn_total_millions,1)) + " Million"
  bot.send_message(chat_id=CHAT_ID, text=bsc_telegram_message, parse_mode=telegram.ParseMode.MARKDOWN)

  bsc_twitter_message1 = "#EverRise $RISE\n\nAverage BSC Blocks Per Kaboom:     " + str(round(bsc_block_average,1)) + "\nBSC Blocks Since Last Kaboom:        " + str(bsc_blocks_elapsed) + "\nBSC Blocks Left:                                   " + str(bsc_blocks_left) + "\nBSC Blocks Per Hour:                           " + str(bsc_block_rate) + "\nHours Until Next Kaboom:                   " + str(time_until_kaboom)
  api.update_status(status=bsc_twitter_message1)

#  bsc_twitter_message2 = "BSC Kraken Belly:                         " + str(round(bsc_kraken_belly)) + " BNB\nETH Kraken Belly:                        " + str(round(eth_kraken_belly,2)) + " ETH\nMATIC Kraken Belly:            " + str(round(poly_kraken_belly)) + " MATIC\nFTM Kraken Belly:                    " + str(round(ftm_kraken_belly_result)) + " FTM\nAVAX Kraken Belly:                    " + str(round(avax_kraken_belly_result)) + " AVAX\nKraken Total Reserves:         $" + str(everrise_usd_reserves_balance_formatted)
#  api.update_status(status=bsc_twitter_message2)




if __name__ == "__main__":
  asyncio.run(main())
