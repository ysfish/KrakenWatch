#!/usr/bin/env python3

import asyncio
import json
import telegram
import time
import tweepy
import mariadb
import plotly.express as px
import pandas as pd
import os
from credentials import *
from statistics import mean, stdev
from datetime import datetime
from bscscan import BscScan
from config import *

async def main():
  if not os.path.exists("/home/pi/kraken_watch/images"):
    os.mkdir("/home/pi/kraken_watch/images")

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

# BSC Stake Performance

  print("Querying database for Stake Performance Data...")
  query = f"SELECT TimeStamp,BSCRewards,BSCPerformance FROM bsc_kraken_rewards ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  bsc_kraken_rewards = cur.fetchall()
  bsc_timestamp, bsc_rewards, bsc_performance = zip(*bsc_kraken_rewards)

  bsc_datetime = []
  for timestamp in bsc_timestamp:
    bsc_datetime.append(datetime.fromtimestamp(timestamp))

#  print("Creating BSC Stake Performance graph...")
#  fig = px.line(x=bsc_datetime, y=bsc_performance, title="BSC Stake Performance", labels={"x": "Date and Time", "y": "36x Staker Return Percentage"})
#  fig.write_image("/home/pi/kraken_watch/images/bsc_performance.png")

# ETH Stake Performance

  print("Querying database...")
  query = f"SELECT TimeStamp,ETHRewards,ETHPerformance FROM eth_kraken_rewards ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  eth_kraken_rewards = cur.fetchall()
  eth_timestamp, eth_rewards, eth_performance = zip(*eth_kraken_rewards)

  eth_datetime = []
  for timestamp in eth_timestamp:
    eth_datetime.append(datetime.fromtimestamp(timestamp))

#  print("Creating ETH Stake Performance graph...")
#  fig = px.line(x=eth_datetime, y=eth_performance, title="ETH Stake Performance", labels={"x": "Date and Time", "y": "36x Staker Return Percentage"})
#  fig.write_image("/home/pi/kraken_watch/images/eth_performance.png")

# Polygon Stake Performance

  print("Querying database...")
  query = f"SELECT TimeStamp,MATICRewards,MATICPerformance FROM poly_kraken_rewards ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  poly_kraken_rewards = cur.fetchall()
  poly_timestamp, poly_rewards, poly_performance = zip(*poly_kraken_rewards)

  poly_datetime = []
  for timestamp in poly_timestamp:
    poly_datetime.append(datetime.fromtimestamp(timestamp))

#  print("Creating Poly Stake Performance graph...")
#  fig = px.line(x=poly_datetime, y=poly_performance, title="Poly Stake Performance", labels={"x": "Date and Time", "y": "36x Staker Return Percentage"})
#  fig.write_image("/home/pi/kraken_watch/images/poly_performance.png")

# FTM Stake Performance

  print("Querying database...")
  query = f"SELECT TimeStamp,FTMRewards,FTMPerformance FROM ftm_kraken_rewards ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  ftm_kraken_rewards = cur.fetchall()
  ftm_timestamp, ftm_rewards, ftm_performance = zip(*ftm_kraken_rewards)

  ftm_datetime = []
  for timestamp in ftm_timestamp:
    ftm_datetime.append(datetime.fromtimestamp(timestamp))

#  print("Creating FTM Stake Performance graph...")
#  fig = px.line(x=ftm_datetime, y=ftm_performance, title="FTM Stake Performance", labels={"x": "Date and Time", "y": "36x Staker Return Percentage"})
#  fig.write_image("/home/pi/kraken_watch/images/ftm_performance.png")

# AVAX Stake Performance

  print("Querying database...")
  query = f"SELECT TimeStamp,AVAXRewards,AVAXPerformance FROM avax_kraken_rewards ORDER BY TimeStamp desc limit 50"
  cur.execute(query)
  avax_kraken_rewards = cur.fetchall()
  avax_timestamp, avax_rewards, avax_performance = zip(*avax_kraken_rewards)

  avax_datetime = []
  for timestamp in avax_timestamp:
    avax_datetime.append(datetime.fromtimestamp(timestamp))

#  print("Creating AVAX Stake Performance graph...")
#  fig = px.line(x=avax_datetime, y=avax_performance, title="AVAX Stake Performance", labels={"x": "Date and Time", "y": "36x Staker Return Percentage"})
#  fig.write_image("/home/pi/kraken_watch/images/avax_performance.png")

  print("Creating MultiChain Performance graph...")
  multichain_data = {"DateTime":list(bsc_datetime),"BSC":list(bsc_performance),"ETH":list(eth_performance),"POLY":list(poly_performance),"FTM":list(ftm_performance),"AVAX":list(avax_performance)}
  df = pd.DataFrame(multichain_data)
  print(df)
  fig = px.line(df, x='DateTime', y=['BSC', 'ETH', 'POLY', 'FTM', 'AVAX'], title="36X Stake Percentage Performance Comparision", labels={"x": "Date and Time", "BSC": "36x Stake Percentage"})
  fig.write_image("/home/pi/kraken_watch/images/multichain_performance.png")


  print("Querying Database for Holder data...")
  query = f"SELECT TimeStamp,TotalHolders,HoldersBSC,HoldersETH,HoldersPOLY,HoldersFTM,HoldersAVAX FROM holders"
  cur.execute(query)
  holder_data = cur.fetchall()
  holder_timestamp, total_holders, holders_bsc, holders_eth, holders_poly, holders_ftm, holders_avax = zip(*holder_data)

  holder_datetime = []
  for timestamp in holder_timestamp:
    holder_datetime.append(datetime.fromtimestamp(timestamp))

  print("Creating Holders graph...")
  holders_data = {"TimeStamp":list(holder_datetime),"Total_Holders":list(total_holders),"BSC_Holders":list(holders_bsc),"ETH_Holders":list(holders_eth),"POLY_Holders":list(holders_poly),"FTM_Holders":list(holders_ftm),"AVAX_Holders":list(holders_avax)}
  holders_df = pd.DataFrame(holders_data)
  print(holders_df)

  fig = px.line(holders_df,x='TimeStamp',y=['Total_Holders','BSC_Holders','ETH_Holders','POLY_Holders','FTM_Holders','AVAX_Holders'],title="Holders")
  fig.write_image("/home/pi/kraken_watch/images/holders.png")

# DAILY DATA COLLECTION ANALYSIS

# UNIFIED DATA

  print("Querying database for Unified Kraken Data...")
  query = f"SELECT Timestamp,KrakenReservesUSD,TokenPrice,DailyVolumeUSD,DailyVolume,DailyKrakenTax,DailyRewards,KrakenFactor FROM unified_data"
  cur.execute(query)
  unified_result = cur.fetchall()
  unified_timestamp, kraken_reserves_usd, token_price, daily_volume_usd, daily_volume, daily_kraken_tax, daily_rewards, kraken_factor = zip(*unified_result)

  unified_datetime = []
  for timestamp in unified_timestamp:
    unified_datetime.append(datetime.fromtimestamp(timestamp))

  unified_data = {"DateTime":list(unified_datetime),"KrakenUSD":list(kraken_reserves_usd),"TokenPrice":list(token_price),"DailyVolumeUSD":list(daily_volume_usd),"DailyVolume":list(daily_volume),"DailyKrakenTax":list(daily_kraken_tax),"DailyRewards":list(daily_rewards),"KrakenFactor":list(kraken_factor)}
  unified_dataframe = pd.DataFrame(unified_data)
  print(unified_dataframe)

  fig1 = px.line(unified_dataframe,x='DateTime',y='KrakenUSD')
  fig1.write_image("/home/pi/kraken_watch/images/unified_kraken_usd.png")

  fig2 = px.line(unified_dataframe,x='DateTime',y='DailyVolumeUSD')
  fig2.write_image("/home/pi/kraken_watch/images/unfied_daily_volume.png")

  fig3 = px.line(unified_dataframe,x='DateTime',y='KrakenFactor')
  fig3.write_image("/home/pi/kraken_watch/images/unified_kraken_factor.png")


# BSC

  print("Querying database for BSC Kraken Data...")
  query = f"SELECT Timestamp,KrakenReservesBSC,TokenPriceBSC,CoinPriceBSC,DailyVolumeBSC,DailyVolumeBSCinUSD,DailyKrakenTaxBSC,DailyRewardsBSC,KrakenFactorBSC FROM bsc_data"
  cur.execute(query)
  bsc_result = cur.fetchall()
  bsc_timestamp, kraken_reserves_bsc, token_price_bsc, coin_price_bsc, daily_volume_bsc, daily_volume_bsc_usd, daily_kraken_tax_bsc, daily_rewards_bsc, kraken_factor_bsc = zip(*bsc_result)

  bsc_datetime = []
  for timestamp in bsc_timestamp:
    bsc_datetime.append(datetime.fromtimestamp(timestamp))

  bsc_data = {"DateTime":list(bsc_datetime),"KrakenReservesBSC":list(kraken_reserves_bsc),"TokenPriceBSC":list(token_price_bsc),"CoinPriceBSC":list(coin_price_bsc),"DailyVolumeBSC":list(daily_volume_bsc),"DailyVolumeBSCinUSD":list(daily_volume_bsc_usd),"DailyKrakenTaxBSC":list(daily_kraken_tax_bsc),"DailyRewardsBSC":list(daily_rewards_bsc),"KrakenFactorBSC":list(kraken_factor_bsc)}
  bsc_dataframe = pd.DataFrame(bsc_data)
  print(bsc_dataframe)

  fig1 = px.line(bsc_dataframe,x='DateTime',y='KrakenReservesBSC')
  fig1.write_image("/home/pi/kraken_watch/images/bsc_kraken_from_stats.png")

  fig2 = px.line(bsc_dataframe,x='DateTime',y='DailyVolumeBSCinUSD')
  fig2.write_image("/home/pi/kraken_watch/images/bsc_daily_volume.png")

  fig3 = px.line(bsc_dataframe,x='DateTime',y='KrakenFactorBSC')
  fig3.write_image("/home/pi/kraken_watch/images/bsc_kraken_factor.png")

# ETH

  print("Querying database for ETH Kraken Data...")
  query = f"SELECT Timestamp,KrakenReservesETH,TokenPriceETH,CoinPriceETH,DailyVolumeETH,DailyVolumeETHinUSD,DailyKrakenTaxETH,DailyRewardsETH,KrakenFactorETH FROM eth_data"
  cur.execute(query)
  eth_result = cur.fetchall()
  eth_timestamp, kraken_reserves_eth, token_price_eth, coin_price_eth, daily_volume_eth, daily_volume_eth_usd, daily_kraken_tax_eth, daily_rewards_eth, kraken_factor_eth = zip(*eth_result)

  eth_datetime = []
  for timestamp in eth_timestamp:
    eth_datetime.append(datetime.fromtimestamp(timestamp))

  eth_data = {"DateTime":list(eth_datetime),"KrakenReservesETH":list(kraken_reserves_eth),"TokenPriceETH":list(token_price_eth),"CoinPriceETH":list(coin_price_eth),"DailyVolumeETH":list(daily_volume_eth),"DailyVolumeETHinUSD":list(daily_volume_eth_usd),"DailyKrakenTaxETH":list(daily_kraken_tax_eth),"DailyRewardsETH":list(daily_rewards_eth),"KrakenFactorETH":list(kraken_factor_eth)}
  eth_dataframe = pd.DataFrame(eth_data)
  print(eth_dataframe)

  fig1 = px.line(eth_dataframe,x='DateTime',y='KrakenReservesETH')
  fig1.write_image("/home/pi/kraken_watch/images/eth_kraken_from_stats.png")

  fig2 = px.line(eth_dataframe,x='DateTime',y='DailyVolumeETHinUSD')
  fig2.write_image("/home/pi/kraken_watch/images/eth_daily_volume.png")

  fig3 = px.line(eth_dataframe,x='DateTime',y='KrakenFactorETH')
  fig3.write_image("/home/pi/kraken_watch/images/eth_kraken_factor.png")


# POLY

  print("Querying database for POLY Kraken Data...")
  query = f"SELECT Timestamp,KrakenReservesPOLY,TokenPricePOLY,CoinPricePOLY,DailyVolumePOLY,DailyVolumePOLYinUSD,DailyKrakenTaxPOLY,DailyRewardsPOLY,KrakenFactorPOLY FROM poly_data"
  cur.execute(query)
  poly_result = cur.fetchall()
  poly_timestamp, kraken_reserves_poly, token_price_poly, coin_price_poly, daily_volume_poly, daily_volume_poly_usd, daily_kraken_tax_poly, daily_rewards_poly, kraken_factor_poly = zip(*poly_result)

  poly_datetime = []
  for timestamp in poly_timestamp:
    poly_datetime.append(datetime.fromtimestamp(timestamp))

  poly_data = {"DateTime":list(poly_datetime),"KrakenReservesPOLY":list(kraken_reserves_poly),"TokenPricePOLY":list(token_price_poly),"CoinPricePOLY":list(coin_price_poly),"DailyVolumePOLY":list(daily_volume_poly),"DailyVolumePOLYinUSD":list(daily_volume_poly_usd),"DailyKrakenTaxPOLY":list(daily_kraken_tax_poly),"DailyRewardsPOLY":list(daily_rewards_poly),"KrakenFactorPOLY":list(kraken_factor_poly)}
  poly_dataframe = pd.DataFrame(poly_data)
  print(poly_dataframe)

  fig1 = px.line(poly_dataframe,x='DateTime',y='KrakenReservesPOLY')
  fig1.write_image("/home/pi/kraken_watch/images/poly_kraken_from_stats.png")

  fig2 = px.line(poly_dataframe,x='DateTime',y='DailyVolumePOLYinUSD')
  fig2.write_image("/home/pi/kraken_watch/images/poly_daily_volume.png")

  fig3 = px.line(poly_dataframe,x='DateTime',y='KrakenFactorPOLY')
  fig3.write_image("/home/pi/kraken_watch/images/poly_kraken_factor.png")

# FTM

  print("Querying database for FTM Kraken Data...")
  query = f"SELECT Timestamp,KrakenReservesFTM,TokenPriceFTM,CoinPriceFTM,DailyVolumeFTM,DailyVolumeFTMinUSD,DailyKrakenTaxFTM,DailyRewardsFTM,KrakenFactorFTM FROM ftm_data"
  cur.execute(query)
  ftm_result = cur.fetchall()
  ftm_timestamp, kraken_reserves_ftm, token_price_ftm, coin_price_ftm, daily_volume_ftm, daily_volume_ftm_usd, daily_kraken_tax_ftm, daily_rewards_ftm, kraken_factor_ftm = zip(*ftm_result)

  ftm_datetime = []
  for timestamp in ftm_timestamp:
    ftm_datetime.append(datetime.fromtimestamp(timestamp))

  ftm_data = {"DateTime":list(ftm_datetime),"KrakenReservesFTM":list(kraken_reserves_ftm),"TokenPriceFTM":list(token_price_ftm),"CoinPriceFTM":list(coin_price_ftm),"DailyVolumeFTM":list(daily_volume_ftm),"DailyVolumeFTMinUSD":list(daily_volume_ftm_usd),"DailyKrakenTaxFTM":list(daily_kraken_tax_ftm),"DailyRewardsFTM":list(daily_rewards_ftm),"KrakenFactorFTM":list(kraken_factor_ftm)}
  ftm_dataframe = pd.DataFrame(ftm_data)
  print(ftm_dataframe)

  fig1 = px.line(ftm_dataframe,x='DateTime',y='KrakenReservesFTM')
  fig1.write_image("/home/pi/kraken_watch/images/ftm_kraken_from_stats.png")

  fig2 = px.line(ftm_dataframe,x='DateTime',y='DailyVolumeFTMinUSD')
  fig2.write_image("/home/pi/kraken_watch/images/ftm_daily_volume.png")

  fig3 = px.line(ftm_dataframe,x='DateTime',y='KrakenFactorFTM')
  fig3.write_image("/home/pi/kraken_watch/images/ftm_kraken_factor.png")

# AVAX

  print("Querying database for AVAX Kraken Data...")
  query = f"SELECT Timestamp,KrakenReservesAVAX,TokenPriceAVAX,CoinPriceAVAX,DailyVolumeAVAX,DailyVolumeAVAXinUSD,DailyKrakenTaxAVAX,DailyRewardsAVAX,KrakenFactorAVAX FROM avax_data"
  cur.execute(query)
  avax_result = cur.fetchall()
  avax_timestamp, kraken_reserves_avax, token_price_avax, coin_price_avax, daily_volume_avax, daily_volume_avax_usd, daily_kraken_tax_avax, daily_rewards_avax, kraken_factor_avax = zip(*avax_result)

  avax_datetime = []
  for timestamp in avax_timestamp:
    avax_datetime.append(datetime.fromtimestamp(timestamp))

  avax_data = {"DateTime":list(avax_datetime),"KrakenReservesAVAX":list(kraken_reserves_avax),"TokenPriceAVAX":list(token_price_avax),"CoinPriceAVAX":list(coin_price_avax),"DailyVolumeAVAX":list(daily_volume_avax),"DailyVolumeAVAXinUSD":list(daily_volume_avax_usd),"DailyKrakenTaxAVAX":list(daily_kraken_tax_avax),"DailyRewardsAVAX":list(daily_rewards_avax),"KrakenFactorAVAX":list(kraken_factor_avax)}
  avax_dataframe = pd.DataFrame(avax_data)
  print(avax_dataframe)

  fig1 = px.line(avax_dataframe,x='DateTime',y='KrakenReservesAVAX')
  fig1.write_image("/home/pi/kraken_watch/images/avax_kraken_from_stats.png")

  fig2 = px.line(avax_dataframe,x='DateTime',y='DailyVolumeAVAX')
  fig2.write_image("/home/pi/kraken_watch/images/avax_daily_volume.png")

  fig3 = px.line(avax_dataframe,x='DateTime',y='KrakenFactorAVAX')
  fig3.write_image("/home/pi/kraken_watch/images/avax_kraken_factor.png")

#  bot.send_message(chat_id=CHAT_ID, text="*Time Between Kabooms, Last 50 Kabooms:*", parse_mode=telegram.ParseMode.MARKDOWN)
#  bot.send_document(chat_id=CHAT_ID, document=open('/home/pi/kraken_watch/images/timestamp_deltas.png', 'rb'))

#  bot.send_message(chat_id=CHAT_ID, text="*Time Between Kabooms, Last 50 Kabooms:*", parse_mode=telegram.ParseMode.MARKDOWN)
#  bot.send_document(chat_id=CHAT_ID, document=open('/home/pi/kraken_watch/images/timestamp_deltas.png', 'rb'))

#  bsc_twitter_message = "#EverRise $RISE\n\nTime Between Kabooms, Last 50 Kabooms:"
#  api.update_status(status=bsc_twitter_message)




if __name__ == "__main__":
  asyncio.run(main())
