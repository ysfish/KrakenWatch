#!/usr/bin/env python3

import telegram
import tweepy
import mariadb
import requests
from credentials import *
from config import *
from statistics import mean, stdev
from datetime import datetime

def main():
  conn = connectToMariaDB()
  cur = conn.cursor()

  cur.execute(f"SELECT BlockNumber FROM bsc_kraken_watch;")
  bsc_start_block = cur.fetchall()[-1][0] + 1

  cur.execute("CREATE TABLE IF NOT EXISTS status_update (TimeStamp bigint, BlockNumber bigint);")
  cur.execute("CREATE TABLE IF NOT EXISTS burn_stats (TimeStamp bigint, TotalBurned bigint);")
  cur.execute("CREATE TABLE IF NOT EXISTS kraken_average (TimeStamp bigint, BlockNumber bigint, TimeStampAverage float, BlockNumberAverage float);")
  cur.execute("CREATE TABLE IF NOT EXISTS kraken_belly (TimeStamp bigint, BlockNumber bigint, BNBValue float, ETHValue float, MATICValue float, FTMValue float, AVAXValue float,TotalUSDValue float);")
  cur.execute("CREATE TABLE IF NOT EXISTS bsc_block_rate (TimeStamp bigint, BSCBlockRate bigint);")

  print("Getting data from EverRise Stats...")
  request_url = "https://everrise.azurewebsites.net/stats"
  everrise_data_json = requests.get(request_url).json()
  everrise_data_unified = requests.get(request_url).json()["unified"]
  
  everrise_data_unified_current = everrise_data_unified["current"]
  everrise_data_unified_24hour = everrise_data_unified["history24hrs"]
  everrise_volume_trade_24hour = everrise_data_unified_24hour["volumeTrade"]
  everrise_usd_reserves_balance = everrise_data_unified_current["usdReservesBalance"]

  current_time = datetime.now()
  time_stamp = int(round(current_time.timestamp()))
  
  print("Getting Burn data from BSCScan...")
  request_url = "https://api.bscscan.com/api?module=account&action=tokenbalance&contractaddress=" + CONTRACT_ADDRESS + "&address=" + BURN_ADDRESS + "&tag=latest&apikey=" + BSC_API_KEY
  bsc_burn_total = int(requests.get(request_url).json()["result"])/(10 ** DECIMALS)
  bsc_burn_total_millions = bsc_burn_total / (10 ** 6)
  print("Success!")
  cur.execute("INSERT INTO burn_stats (TimeStamp,TotalBurned) VALUES (?, ?)", (time_stamp,bsc_burn_total))
  conn.commit()

  print("Getting Current Block data from BSCScan...")
  request_url = "https://api.bscscan.com/api?module=block&action=getblocknobytime&timestamp=" + str(time_stamp) + "&closest=before&apikey=" + BSC_API_KEY
  bsc_block_number = requests.get(request_url).json()["result"]
  print(bsc_block_number)

  for chain in CHAINS:
    globals()[f"{chain}_kraken_belly"] = everrise_data_json[chain]["current"]["reservesCoinBalance"]
  cur.execute("INSERT INTO kraken_belly (TimeStamp,BlockNumber,BNBValue,ETHValue,MATICValue,FTMValue,AVAXValue,TotalUSDValue) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (time_stamp,bsc_block_number,bsc_kraken_belly,eth_kraken_belly,poly_kraken_belly,ftm_kraken_belly,avax_kraken_belly,round(float(everrise_usd_reserves_balance),2)))
  conn.commit()

  cur.execute(f"SELECT TimeStamp,BlockNumber FROM bsc_kraken_watch ORDER BY TimeStamp desc limit 50")
  bsc_kabooms = cur.fetchall()
  bsc_kaboom_timestamps, bsc_kaboom_blocks = zip(*bsc_kabooms)
  bsc_kaboom_timestamp_difference = [float(bsc_kaboom_timestamps[i])-float(bsc_kaboom_timestamps[i+1]) for i in range(len(bsc_kaboom_timestamps)-1)]
  bsc_timestamp_average = mean(bsc_kaboom_timestamp_difference)
  bsc_kaboom_block_difference = [int(bsc_kaboom_blocks[i]-int(bsc_kaboom_blocks[i+1])) for i in range(len(bsc_kaboom_blocks)-1)]
  bsc_block_average = mean(bsc_kaboom_block_difference)

  cur.execute("INSERT INTO kraken_average (TimeStamp,BlockNumber,TimeStampAverage,BlockNumberAverage) VALUES (?, ?, ?, ?)", (time_stamp,bsc_block_number,bsc_timestamp_average,bsc_block_average))
  conn.commit()
  
  cur.execute(f"SELECT TimeStamp,BlockNumber FROM status_update ORDER BY TimeStamp desc limit 1")
  bsc_status_update = cur.fetchall()
  bsc_timestamp_prev, bsc_block_number_prev = zip(*bsc_status_update)
  bsc_timestamp_previous = bsc_timestamp_prev[0]
  bsc_block_number_previous = bsc_block_number_prev[0]

  bsc_timestamp_delta = int(time_stamp) - int(bsc_timestamp_previous)
  bsc_block_delta = int(bsc_block_number) - int(bsc_block_number_previous)
  bsc_block_rate = int(bsc_block_delta/bsc_timestamp_delta*3600)

  cur.execute("INSERT INTO bsc_block_rate (TimeStamp,BSCBlockRate) VALUES (?, ?)", (time_stamp,bsc_block_rate))
  cur.execute("INSERT INTO status_update (TimeStamp,BlockNumber) VALUES (?, ?)", (time_stamp,bsc_block_number))
  conn.commit()

  bsc_blocks_elapsed = int(bsc_block_number) - int(bsc_start_block)
  bsc_blocks_left = int(bsc_block_average) - int(bsc_blocks_elapsed)
  if bsc_blocks_left < 0:
    hours_until_kaboom = 0
    minutes_until_kaboom = 0
  else:
    hours_until_kaboom , blocks_until_kaboom = divmod(int(bsc_blocks_left),int(bsc_block_rate))
    minutes_until_kaboom = round(int(blocks_until_kaboom)/int(bsc_block_rate)*60)
    if minutes_until_kaboom == 60:
      minutes_until_kaboom = 0
      hours_until_kaboom += 1
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

  bot = connectToTelegram()

  api = connectToTwitter()

  if(TELEGRAM):
    telegram_message = "\n🐋 *Kraken Status Update*\n🐋 *Average BSC Blocks Per Kaboom:*     " + str(round(bsc_block_average)) + "\n🐋 *BSC Blocks Since Last Kaboom:*        " + str(bsc_blocks_elapsed) + "\n🐋 *BSC Blocks Left:*                                     " + str(bsc_blocks_left) + "\n🐋 *BSC Blocks Per Hour:*                            " + str(bsc_block_rate) + "\n🐋 *Hours Until Next Kaboom:*                    " + str(time_until_kaboom) + "\n\n🐋 [BSC Kraken Belly:](https://bscscan.com/address/0xc17c30e98541188614df99239cabd40280810ca3)                         " + str(round(float(bsc_kraken_belly))) + " BNB\n🐋 [ETH Kraken Belly:](https://etherscan.io/address/0xc17c30e98541188614df99239cabd40280810ca3)                            " + str(round(float(eth_kraken_belly))) + " ETH\n🐋 [POLY Kraken Belly:](https://polygonscan.com/address/0xc17c30e98541188614df99239cabd40280810ca3)                " + str(round(float(poly_kraken_belly))) + " MATIC\n🐋 [FTM Kraken Belly:](https://ftmscan.com/address/0xc17c30e98541188614df99239cabd40280810ca3)                     " + str(round(float(ftm_kraken_belly))) + " FTM\n🐋 [AVAX Kraken Belly:](https://snowtrace.io/address/0xc17c30e98541188614df99239cabd40280810ca3)                    " + str(round(float(avax_kraken_belly))) + " AVAX\n\n🐋 [Total Tokens Burned:](https://bscscan.com/token/0xc17c30e98541188614df99239cabd40280810ca3?a=0x000000000000000000000000000000000000dead)                 " + str(round(bsc_burn_total_millions,1)) + " Million"
    bot.send_message(chat_id=CHAT_ID, text=telegram_message, parse_mode=telegram.ParseMode.MARKDOWN, disable_web_page_preview=True)

  twitter_message1 = "🐋 #EverRise $RISE\n\n🐋 Average BSC Blocks Per Kaboom:     " + str(round(bsc_block_average,1)) + "\n🐋 BSC Blocks Since Last Kaboom:        " + str(bsc_blocks_elapsed) + "\n🐋 BSC Blocks Left:                                   " + str(bsc_blocks_left) + "\n🐋 BSC Blocks Per Hour:                           " + str(bsc_block_rate) + "\n🐋 Hours Until Next Kaboom:                   " + str(time_until_kaboom)
  if(TWITTER):
    api.update_status(status=twitter_message1)

  twitter_message2 = "🐋 #EverRise $RISE\n\n🐋 Kraken Reserves:\n🐋 BSC: " + str(round(float(globals()[f"{CHAINS[0]}_kraken_belly"]))) + " BNB\n🐋 ETH: " + str(round(float(globals()[f"{CHAINS[1]}_kraken_belly"]))) + " ETH\n🐋 MATIC: " + str(round(float(globals()[f"{CHAINS[2]}_kraken_belly"]))) + " MATIC\n🐋 FTM: " + str(round(float(globals()[f"{CHAINS[3]}_kraken_belly"]))) + " FTM\n🐋 AVAX: " + str(round(float(globals()[f"{CHAINS[4]}_kraken_belly"]))) + " AVAX\n🐋 Total USD Reserves: $" + str("{:,}".format(round(float(everrise_usd_reserves_balance),2))) + "\n\n🐋 Total Tokens Burned: " + str(round(bsc_burn_total_millions,1)) + " Million"
  if(TWITTER):
    api.update_status(status=twitter_message2)

def connectToTwitter():
    print("Connecting to Twitter...")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    print("Success!")
    return api

def connectToTelegram():
    print("Connecting to Telegram Bot...")
    try:
      bot = telegram.Bot(token=TELEGRAM_API_KEY)
      print("Success!")
    except telegram.Error as e:
      print(f"Error connecting to Telegram: {e}")
    return bot

def connectToMariaDB():
    print("Connecting to MariaDB Instance...")
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
