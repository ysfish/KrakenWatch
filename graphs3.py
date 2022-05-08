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

# LIQUIDITY

  print("Querying database for Liquidity data...")
  query = f"SELECT Timestamp,LiquidityTokenBSC,LiquidityTokenETH,LiquidityTokenPOLY,LiquidityTokenFTM,LiquidityTokenAVAX FROM liquidity"
  cur.execute(query)
  liquidity_token_result = cur.fetchall()
  liquidity_token_timestamp, liquidity_token_bsc, liquidity_token_eth, liquidity_token_poly, liquidity_token_ftm, liquidity_token_avax = zip(*liquidity_token_result)

  liquidity_datetime = []
  for timestamp in liquidity_token_timestamp:
    liquidity_datetime.append(datetime.fromtimestamp(timestamp))

  liquidity_token_data = {"DateTime":list(liquidity_datetime),"BSC":list(liquidity_token_bsc),"ETH":list(liquidity_token_eth),"POLY":list(liquidity_token_poly),"FTM":list(liquidity_token_ftm),"AVAX":list(liquidity_token_avax)}
  liquidity_token_dataframe = pd.DataFrame(liquidity_token_data)
  print(liquidity_token_dataframe)

  fig = px.line(liquidity_token_dataframe,x='DateTime',y=['BSC','ETH','POLY','FTM','AVAX'],title="Liquidity in Tokens, Per Chain")
  fig.write_image("/home/pi/kraken_watch/images/liquidity_token.png")

# LIQUIDITY IN USD

  print("Querying database for Liquidity data in USD...")
  query = f"SELECT Timestamp,TotalLiquidityTokenUSD,TotalLiquidityCoinUSD,LiquidityTokenBSC,LiquidityCoinBSC,LiquidityTokenETH,LiquidityCoinETH,LiquidityTokenPOLY,LiquidityCoinPOLY,LiquidityTokenFTM,LiquidityCoinFTM,LiquidityTokenAVAX,LiquidityCoinAVAX FROM liquidity_usd"
  cur.execute(query)
  liquidity_usd_result = cur.fetchall()
  liquidity_usd_timestamp, total_liquidity_token_usd, total_liquidity_coin_usd, liquidity_token_bsc_usd, liquidity_coin_bsc_usd, liquidity_token_eth_usd, liquidity_coin_eth_usd, liquidity_token_poly_usd, liquidity_coin_poly_usd, liquidity_token_ftm_usd, liquidity_coin_ftm_usd, liquidity_token_avax_usd, liquidity_coin_avax_usd = zip(*liquidity_usd_result)
  print(total_liquidity_token_usd)

  liquidity_usd_datetime = []
  for timestamp in liquidity_usd_timestamp:
    liquidity_usd_datetime.append(datetime.fromtimestamp(timestamp))

  liquidity_token_usd_data = {"DateTime":list(liquidity_usd_datetime),"TotalLiquidityTokenUSD":list(total_liquidity_token_usd),"TotalLiquidityCoinUSD":list(total_liquidity_coin_usd),"BSCToken":list(liquidity_token_bsc_usd),"BSCCoin":list(liquidity_coin_bsc_usd),"ETHToken":list(liquidity_token_eth_usd),"ETHCoin":list(liquidity_coin_eth_usd),"POLYToken":list(liquidity_token_poly_usd),"POLYCoin":list(liquidity_coin_poly_usd),"FTMToken":list(liquidity_token_ftm_usd),"FTMCoin":list(liquidity_coin_ftm_usd),"AVAXToken":list(liquidity_token_avax_usd),"AVAXCoin":list(liquidity_coin_avax_usd)}
  liquidity_token_usd_dataframe = pd.DataFrame(liquidity_token_usd_data)
  print(liquidity_token_usd_dataframe)

  fig = px.line(liquidity_token_usd_dataframe,x='DateTime',y=['BSCToken','ETHToken','POLYToken','FTMToken','AVAXToken'],title="Liquidity in USD")
  fig.write_image("/home/pi/kraken_watch/images/liquidity_token_usd.png")

# PRICE DATA

  print("Querying database for price data...")
  query = f"SELECT Timestamp,TokenPriceBSC,CoinPriceBSC,TokenPriceETH,CoinPriceETH,TokenPricePOLY,CoinPricePOLY,TokenPriceFTM,CoinPriceFTM,TokenPriceAVAX,CoinPriceAVAX FROM price_data"
  cur.execute(query)
  price_data_result = cur.fetchall()
  price_timestamp, token_price_bsc, coin_price_bsc, token_price_eth, coin_price_eth, token_price_poly, coin_price_poly, token_price_ftm, coin_price_ftm, token_price_avax, coin_price_avax  = zip(*price_data_result)

  price_datetime = []
  for timestamp in price_timestamp:
    price_datetime.append(datetime.fromtimestamp(timestamp))

  price_data = {"DateTime":list(price_datetime),"BSC":list(token_price_bsc),"ETH":list(token_price_eth),"POLY":list(token_price_poly),"FTM":list(token_price_ftm),"AVAX":list(token_price_avax)}
  price_dataframe = pd.DataFrame(price_data)
  print(price_dataframe)
  fig = px.line(price_dataframe,title='Token Price',x='DateTime',y=['BSC','ETH','POLY','FTM','AVAX'])
  fig.write_image("/home/pi/kraken_watch/images/token_price_history.png")

# STAKED TOKEN TRACKING

  print("Querying database for staked token data...")
  query = f"SELECT Timestamp,TotalStaked,BSCStaked,ETHStaked,POLYStaked,FTMStaked,AVAXStaked FROM staked_tokens"
  cur.execute(query)
  staked_tokens_result = cur.fetchall()
  staked_tokens_timestamp, total_staked, bsc_staked, eth_staked, poly_staked, ftm_staked, avax_staked  = zip(*staked_tokens_result)

  staked_tokens_datetime = []
  for timestamp in staked_tokens_timestamp:
    staked_tokens_datetime.append(datetime.fromtimestamp(timestamp))

  staked_tokens_data = {"DateTime":list(staked_tokens_datetime),"Total":list(total_staked),"BSC":list(bsc_staked),"ETH":list(eth_staked),"POLY":list(poly_staked),"FTM":list(ftm_staked),"AVAX":list(avax_staked)}
  staked_tokens_dataframe = pd.DataFrame(staked_tokens_data)
  print(staked_tokens_dataframe)

# UNIFIED

  fig = px.line(staked_tokens_dataframe,title='Staked Tokens - Total',x='DateTime',y='Total')
  fig.write_image("/home/pi/kraken_watch/images/unified_staked_tokens.png")

# BSC

  fig = px.line(staked_tokens_dataframe,title='Staked Tokens - BSC',x='DateTime',y='BSC')
  fig.write_image("/home/pi/kraken_watch/images/bsc_staked_tokens.png")

# ETH

  fig = px.line(staked_tokens_dataframe,title='Staked Tokens - ETH',x='DateTime',y='ETH')
  fig.write_image("/home/pi/kraken_watch/images/eth_staked_tokens.png")

# POLY

  fig = px.line(staked_tokens_dataframe,title='Staked Tokens - Poly',x='DateTime',y='POLY')
  fig.write_image("/home/pi/kraken_watch/images/poly_staked_tokens.png")

# FTM

  fig = px.line(staked_tokens_dataframe,title='Staked Tokens - FTM',x='DateTime',y='FTM')
  fig.write_image("/home/pi/kraken_watch/images/ftm_staked_tokens.png")

# AVAX

  fig = px.line(staked_tokens_dataframe,title='Staked Tokens - AVAX',x='DateTime',y='AVAX')
  fig.write_image("/home/pi/kraken_watch/images/avax_staked_tokens.png")




#  bot.send_message(chat_id=CHAT_ID, text="*Time Between Kabooms, Last 50 Kabooms:*", parse_mode=telegram.ParseMode.MARKDOWN)
#  bot.send_document(chat_id=CHAT_ID, document=open('/home/pi/kraken_watch/images/timestamp_deltas.png', 'rb'))

#  bot.send_message(chat_id=CHAT_ID, text="*Time Between Kabooms, Last 50 Kabooms:*", parse_mode=telegram.ParseMode.MARKDOWN)
#  bot.send_document(chat_id=CHAT_ID, document=open('/home/pi/kraken_watch/images/timestamp_deltas.png', 'rb'))

#  bsc_twitter_message = "#EverRise $RISE\n\nTime Between Kabooms, Last 50 Kabooms:"
#  api.update_status(status=bsc_twitter_message)




if __name__ == "__main__":
  asyncio.run(main())
