#!/usr/bin/env python3

import asyncio
import json
import telegram
import time
import tweepy
import mariadb
import plotly.express as px
import os
from credentials import *
from statistics import mean, stdev
from datetime import datetime
from bscscan import BscScan
from config import *

async def main():
  if not os.path.exists("/home/pi/kraken_watch/images"):
    os.mkdir("/home/pi/kraken_watch/images")

#  print("Connecting to local MariaDB Instance...")
#  try:
#    conn = mariadb.connect(
#      user=MARIADB_USER,
#      password=MARIADB_PW,
#      host=MARIADB_HOST,
#      port=3306,
#      database=MARIADB_DB
#    )
#    print("Success!")
#  except mariadb.Error as e:
#    print(f"Error connecting to MariaDB Platform: {e}")

  print("Connecting to remote MariaDB Instance...")
  try:
    conn = mariadb.connect(
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

  print("Querying database...")
  query = f"SELECT TimeStamp,BlockNumber,BNBValue FROM bsc_kraken_watch ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  bsc_kabooms = cur.fetchall()
  bsc_kaboom_timestamps, bsc_kaboom_blocknumbers, bsc_kaboom_bnbvalue = zip(*bsc_kabooms)

  bsc_kaboom_datetime = []
  for timestamp in bsc_kaboom_timestamps:
    bsc_kaboom_datetime.append(datetime.fromtimestamp(timestamp))

  print("Creating BNB Value graph...")
  fig = px.scatter(x=bsc_kaboom_datetime, y=bsc_kaboom_bnbvalue, trendline="lowess", title="BNB Value of Last 50 Kabooms", labels={"x": "Date and Time", "y": "BNB Value"})
  fig.write_image("/home/pi/kraken_watch/images/bnb_value.png")

  print("Querying database...")
  query = f"SELECT TimeStamp,BlockNumber,ETHValue FROM eth_kraken_watch ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  eth_kabooms = cur.fetchall()
  eth_kaboom_timestamps, eth_kaboom_blocknumbers, eth_kaboom_ethvalue = zip(*eth_kabooms)

  eth_kaboom_datetime = []
  for timestamp in eth_kaboom_timestamps:
    eth_kaboom_datetime.append(datetime.fromtimestamp(timestamp))

  print("Creating ETH Value graph...")
  fig = px.scatter(x=eth_kaboom_datetime, y=eth_kaboom_ethvalue, trendline="lowess", title="ETH Value of Last 50 Kabooms", labels={"x": "Date and Time", "y": "ETH Value"})
  fig.write_image("/home/pi/kraken_watch/images/eth_value.png")

  print("Querying database...")
  query = f"SELECT TimeStamp,BlockNumber,MATICValue FROM poly_kraken_watch ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  poly_kabooms = cur.fetchall()
  poly_kaboom_timestamps, poly_kaboom_blocknumbers, poly_kaboom_maticvalue = zip(*poly_kabooms)

  poly_kaboom_datetime = []
  for timestamp in poly_kaboom_timestamps:
    poly_kaboom_datetime.append(datetime.fromtimestamp(timestamp))

  print("Creating MATIC Value graph...")
  fig = px.scatter(x=poly_kaboom_datetime, y=poly_kaboom_maticvalue, trendline="lowess", title="MATIC Value of Last 50 Kabooms", labels={"x": "Date and Time", "y": "MATIC Value"})
  fig.write_image("/home/pi/kraken_watch/images/poly_value.png")

  print("Querying database...")
  query = f"SELECT TimeStamp,BlockNumber,FTMValue FROM ftm_kraken_watch ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  ftm_kabooms = cur.fetchall()
  ftm_kaboom_timestamps, ftm_kaboom_blocknumbers, ftm_kaboom_ftmvalue = zip(*ftm_kabooms)

  ftm_kaboom_datetime = []
  for timestamp in ftm_kaboom_timestamps:
    ftm_kaboom_datetime.append(datetime.fromtimestamp(timestamp))

  print("Creating FTM Value graph...")
  fig = px.scatter(x=ftm_kaboom_datetime, y=ftm_kaboom_ftmvalue, trendline="lowess", title="FTM Value of Last 50 Kabooms", labels={"x": "Date and Time", "y": "FTM Value"})
  fig.write_image("/home/pi/kraken_watch/images/ftm_value.png")

  print("Querying database...")
  query = f"SELECT TimeStamp,BlockNumber,AVAXValue FROM avax_kraken_watch ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  avax_kabooms = cur.fetchall()
  avax_kaboom_timestamps, avax_kaboom_blocknumbers, avax_kaboom_avaxvalue = zip(*avax_kabooms)

  avax_kaboom_datetime = []
  for timestamp in avax_kaboom_timestamps:
    avax_kaboom_datetime.append(datetime.fromtimestamp(timestamp))

  print("Creating AVAX Value graph...")
  fig = px.scatter(x=avax_kaboom_datetime, y=avax_kaboom_avaxvalue, trendline="lowess", title="AVAX Value of Last 50 Kabooms", labels={"x": "Date and Time", "y": "AVAX Value"})
  fig.write_image("/home/pi/kraken_watch/images/avax_value.png")

  bsc_kaboom_timestamp_difference = [(bsc_kaboom_timestamps[i]-bsc_kaboom_timestamps[i+1])/3600 for i in range(len(bsc_kaboom_timestamps)-1)]
  bsc_kaboom_timestamp_difference.insert(0,0)

  print("Creating Kaboom BSC TimeStamp Difference graph...")
  fig = px.scatter(x=bsc_kaboom_datetime,y=bsc_kaboom_timestamp_difference,trendline="lowess", title="Time Between Last 50 Kabooms", labels={"x": "Date and Time", "y": "Hours Elapsed"})
  fig.write_image("/home/pi/kraken_watch/images/timestamp_deltas.png")

  bsc_kaboom_blocknumber_difference = [(bsc_kaboom_blocknumbers[i]-bsc_kaboom_blocknumbers[i+1]) for i in range(len(bsc_kaboom_blocknumbers)-1)]
  bsc_kaboom_blocknumber_difference.insert(0,0)

  print("Creating Kaboom BSC BlockNumber Difference graph...")
  fig = px.scatter(x=bsc_kaboom_datetime,y=bsc_kaboom_blocknumber_difference,trendline="lowess", title="BSC Blocks Between Last 50 Kabooms", labels={"x": "Date and Time", "y": "Blocks Elapsed"})
  fig.write_image("/home/pi/kraken_watch/images/blocknumber_deltas.png")

  print("Querying database...")
  query = f"SELECT TimeStamp,BlockNumber,TimeStampAverage,BlockNumberAverage FROM kraken_average"
  cur.execute(query)
  bsc_averages = cur.fetchall()
  bsc_timestamp, bsc_blocknumber, bsc_timestamp_average, bsc_blocknumber_average = zip(*bsc_averages)
  bsc_timestamp_average_hours = []
  for number in bsc_timestamp_average:
    bsc_timestamp_average_hours.append(number / 3600)

  bsc_datetime = []
  for timestamp in bsc_timestamp:
    bsc_datetime.append(datetime.fromtimestamp(timestamp))

  print("Creating Average Time Between Kabooms graph...")
  fig = px.line(x=bsc_datetime, y=bsc_timestamp_average_hours, title="Average Time Between Kabooms", labels={"x": "Date and Time", "y": "Hours Between Kabooms"})
  fig.write_image("/home/pi/kraken_watch/images/timestamp_datetime_average.png")

  print("Creating Average Blocks Between Kabooms graph...")
  fig = px.line(x=bsc_datetime, y=bsc_blocknumber_average, title="Average Blocks Between Kabooms", labels={"x": "Date and Time", "y": "BSC Blocks Between Kabooms"})
  fig.write_image("/home/pi/kraken_watch/images/blocknumber_average.png")

  print("Querying database...")
  query = f"SELECT TimeStamp,BNBValue,ETHValue,MATICValue,FTMValue,AVAXValue,TotalUSDValue FROM kraken_belly"
  cur.execute(query)
  kraken_belly = cur.fetchall()
  bsc_timestamp, bnb_value, eth_value, matic_value, ftm_value, avax_value, usd_value = zip(*kraken_belly)

  bsc_datetime = []
  for timestamp in bsc_timestamp:
    bsc_datetime.append(datetime.fromtimestamp(timestamp))

  print("Creating BSC Kraken value graph...")
  fig = px.line(x=bsc_datetime, y=bnb_value, title="BNB Value in Kraken", labels={"x": "Date and Time", "y": "BNB Value"})
  fig.write_image("/home/pi/kraken_watch/images/bsc_kraken.png")

  print("Creating ETH Kraken value graph...")
  fig = px.line(x=bsc_datetime, y=eth_value, title="ETH Value in Kraken", labels={"x": "Date and Time", "y": "ETH Value"})
  fig.write_image("/home/pi/kraken_watch/images/eth_kraken.png")

  print("Creating MATIC Kraken value graph...")
  fig = px.line(x=bsc_datetime, y=matic_value, title="MATIC Value in Kraken", labels={"x": "Date and Time", "y": "MATIC Value"})
  fig.write_image("/home/pi/kraken_watch/images/poly_kraken.png")

  print("Creating FTM Kraken value graph...")
  fig = px.line(x=bsc_datetime, y=ftm_value, title="FTM Value in Kraken", labels={"x": "Date and Time", "y": "FTM Value"})
  fig.write_image("/home/pi/kraken_watch/images/ftm_kraken.png")

  print("Creating AVAX Kraken value graph...")
  fig = px.line(x=bsc_datetime, y=avax_value, title="AVAX Value in Kraken", labels={"x": "Date and Time", "y": "AVAX Value"})
  fig.write_image("/home/pi/kraken_watch/images/avax_kraken.png")

  print("Creating USD total value graph...")
  fig = px.line(x=bsc_datetime, y=usd_value, title="Total USD Value in Kraken", labels={"x": "Date and Time", "y": "USD Value"})
  fig.write_image("/home/pi/kraken_watch/images/total_kraken.png")


  print("Querying database...")
  query = f"SELECT TimeStamp,BSCBlockRate FROM bsc_block_rate"
  cur.execute(query)
  bsc_block_rate_data = cur.fetchall()
  bsc_timestamp, bsc_block_rate = zip(*bsc_block_rate_data)

  bsc_datetime = []
  for timestamp in bsc_timestamp:
    bsc_datetime.append(datetime.fromtimestamp(timestamp))

  print("Creating BSC Block Rate graph...")
  fig = px.line(x=bsc_datetime, y=bsc_block_rate, title="BSC Block Rate", labels={"x": "Date and Time", "y": "BSC Blocks Per Hour"})
  fig.write_image("/home/pi/kraken_watch/images/bsc_block_rate.png")


#  bot.send_message(chat_id=CHAT_ID, text="*Time Between Kabooms, Last 50 Kabooms:*", parse_mode=telegram.ParseMode.MARKDOWN)
#  bot.send_document(chat_id=CHAT_ID, document=open('/home/pi/kraken_watch/images/timestamp_deltas.png', 'rb'))

#  bsc_telegram_message = "\n*Average BSC Blocks Per Kaboom:*     " + str(round(bsc_block_average,1)) + "\n*BSC Blocks Since Last Kaboom:*        " + str(bsc_blocks_elapsed) + "\n*BSC Blocks Left:*                                     " + str(bsc_blocks_left) + "\n*BSC Blocks Per Hour:*                            " + str(bsc_block_rate) + "\n*Hours Until Next Kaboom:*                    " + str(time_until_kaboom) + "\n\n*Total Tokens Burned:*            " + str(bsc_burn_total)
#  bot.send_message(chat_id=CHAT_ID, text=bsc_telegram_message, parse_mode=telegram.ParseMode.MARKDOWN)

#  bsc_twitter_message = "#EverRise $RISE\n\nAverage BSC Blocks Per Kaboom:     " + str(round(bsc_block_average,1)) + "\nBSC Blocks Since Last Kaboom:        " + str(bsc_blocks_elapsed) + "\nBSC Blocks Left:                                   " + str(bsc_blocks_left) + "\nBSC Blocks Per Hour:                           " + str(bsc_block_rate) + "\nHours Until Next Kaboom:                   " + str(time_until_kaboom)
#  api.update_status(status=bsc_twitter_message)




if __name__ == "__main__":
  asyncio.run(main())
