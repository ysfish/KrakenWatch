#!/usr/bin/env python3

import mariadb
import requests
from credentials import *
from statistics import mean, stdev
from datetime import datetime
from config import *

def main():
  conn = connectToMariaDB()
  cur = conn.cursor()

  current_time = datetime.utcnow()
  time_stamp = int(round(current_time.timestamp()))
  print("Current Time:        ",current_time)
  print("Time Stamp:          ",time_stamp)

# CREATE DATABASE TABLES
  if(not READONLY):
    cur.execute("CREATE TABLE IF NOT EXISTS unified_data (TimeStamp bigint, KrakenReservesUSD float, TokenPrice float, DailyVolumeUSD float, DailyVolume float, DailyKrakenTax float, DailyRewards float, KrakenFactor float);")
    cur.execute("CREATE TABLE IF NOT EXISTS holders (TimeStamp bigint, TotalHolders bigint, HoldersBSC bigint, HoldersETH bigint, HoldersPOLY bigint, HoldersFTM bigint, HoldersAVAX bigint);")
    cur.execute("CREATE TABLE IF NOT EXISTS everswap_data (TimeStamp bigint, EverSwapTotalUSD float, EverSwapBSC float, EverSwapBSCinUSD float, EverSwapETH float, EverSwapETHinUSD float, EverSwapPOLY float, EverSwapPOLYinUSD float, EverSwapFTM float, EverSwapFTMinUSD float, EverSwapAVAX float, EverSwapAVAXinUSD float);")

# GET EVERRISE DATA #

  everrise_data = requests.get(EVERRISE_STATS).json()

# UNIFIED DATA #

  everrise_data_unified = everrise_data["unified"]
  everrise_data_unified_current = everrise_data_unified["current"]
  everrise_unified_price = everrise_data_unified_current["tokenPriceStable"]
  everrise_volume_trade_current = everrise_data_unified_current["volumeTrade"]
  everrise_volume_trade_current_usd = everrise_data_unified_current["usdVolumeTrade"]
  usd_liquidity_token = everrise_data_unified_current["usdLiquidityToken"]
  usd_liquidity_coin = everrise_data_unified_current["usdLiquidityCoin"]
  usd_liquidity_total = float(usd_liquidity_token) + float(usd_liquidity_coin)
  total_holders_current = everrise_data_unified_current["holders"]
  total_rewards_current = everrise_data_unified_current["rewards"]
  everrise_data_unified_24hour = everrise_data_unified["history24hrs"]
  everrise_volume_trade_24hour = everrise_data_unified_24hour["volumeTrade"]
  total_rewards_24hour_history = everrise_data_unified_24hour["rewards"]
  everrise_volume_trade_24hour_usd = everrise_data_unified_24hour["usdVolumeTrade"]
  everrise_usd_reserves_balance = everrise_data_unified_24hour["usdReservesBalance"]

  total_rewards_24hour = float(total_rewards_current) - float(total_rewards_24hour_history)

  everrise_volume_24hour = float(everrise_volume_trade_current) - float(everrise_volume_trade_24hour)
  everrise_volume_24hour_usd = float(everrise_volume_trade_current_usd) - float(everrise_volume_trade_24hour_usd)
  kraken_tax_24hour_unified = everrise_volume_24hour * 0.04

  kraken_delta_unified = float(kraken_tax_24hour_unified) - float(total_rewards_24hour)
  kraken_factor_unified = float(kraken_delta_unified) * float(everrise_unified_price) / float(everrise_usd_reserves_balance) * 100

  kraken_tax_24hour_unified_formatted = "{:,}".format(kraken_tax_24hour_unified)
  total_rewards_24hour_formatted = "{:,}".format(total_rewards_24hour)

  if(not READONLY):
    cur.execute("INSERT INTO unified_data (TimeStamp,KrakenReservesUSD,TokenPrice,DailyVolumeUSD,DailyVolume,DailyKrakenTax,DailyRewards,KrakenFactor) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (time_stamp,everrise_usd_reserves_balance,everrise_unified_price,everrise_volume_24hour_usd,everrise_volume_24hour,kraken_tax_24hour_unified,total_rewards_24hour,kraken_factor_unified))
    conn.commit()
  everswap_total_usd = 0
  for chain in CHAINS:
    if(not READONLY):
      cur.execute(f"CREATE TABLE IF NOT EXISTS {chain}_data (DateTime DateTime, TimeStamp bigint, KrakenReserves{chain.upper()} float, TokenPrice{chain.upper()} float, CoinPrice{chain.upper()} float, DailyVolume{chain.upper()} float, DailyVolume{chain.upper()}inUSD float, DailyKrakenTax{chain.upper()} float, DailyRewards{chain.upper()} float, KrakenFactorBSC{chain.upper()} float);")
    globals()[f"{chain}"] = everrise_data[chain]
    globals()[f"{chain}_current"] = globals()[f"{chain}"]["current"]
    globals()[f"{chain}_volume_trade_current"] = globals()[f"{chain}_current"]["volumeTrade"]
    globals()[f"{chain}_reserves_coin"] = globals()[f"{chain}_current"]["reservesCoinBalance"]
    globals()[f"{chain}_token_price"] = globals()[f"{chain}_current"]["tokenPriceStable"]
    globals()[f"{chain}_coin_price"] = globals()[f"{chain}_current"]["coinPriceStable"]
    globals()[f"{chain}_liquidity_token"] = globals()[f"{chain}_current"]["liquidityToken"]
    globals()[f"{chain}_liquidity_coin"] = globals()[f"{chain}_current"]["liquidityCoin"]
    globals()[f"{chain}_liquidity_token_usd"] = float(globals()[f"{chain}_token_price"]) * float(globals()[f"{chain}_liquidity_token"])
    globals()[f"{chain}_liquidity_coin_usd"] = float(globals()[f"{chain}_coin_price"]) * float(globals()[f"{chain}_liquidity_coin"])
    globals()[f"{chain}_liquidity_total_usd"] = float(globals()[f"{chain}_liquidity_token_usd"]) + float(globals()[f"{chain}_liquidity_coin_usd"])
    globals()[f"{chain}_holders_current"] = globals()[f"{chain}_current"]["holders"]
    globals()[f"{chain}_rewards_current"] = globals()[f"{chain}_current"]["rewards"]
    globals()[f"{chain}_everswap_current"] = globals()[f"{chain}_current"]["everSwap"]
    globals()[f"{chain}_24hours"] = globals()[f"{chain}"]["history24hrs"]
    globals()[f"{chain}_volume_trade_24hours"] = globals()[f"{chain}_24hours"]["volumeTrade"]
    globals()[f"{chain}_rewards_24hour_history"] = globals()[f"{chain}_24hours"]["rewards"]
    globals()[f"{chain}_everswap_24hour_history"] = globals()[f"{chain}_24hours"]["everSwap"]
    globals()[f"{chain}_rewards_24hour"] = float(globals()[f"{chain}_rewards_current"]) - float(globals()[f"{chain}_rewards_24hour_history"])
    globals()[f"{chain}_everswap_daily"] = float(globals()[f"{chain}_everswap_current"]) - float(globals()[f"{chain}_everswap_24hour_history"])
    globals()[f"{chain}_volume_24hour"] = float(globals()[f"{chain}_volume_trade_current"]) - float(globals()[f"{chain}_volume_trade_24hours"])
    globals()[f"{chain}_volume_24hour_usd"] = float(globals()[f"{chain}_volume_24hour"]) * float(globals()[f"{chain}_token_price"])
    globals()[f"{chain}_kraken_tax_24hour"] = globals()[f"{chain}_volume_24hour"] * TAX_RATE
    globals()[f"{chain}_kraken_delta"] = float(globals()[f"{chain}_kraken_tax_24hour"]) - float(globals()[f"{chain}_rewards_24hour"])
    globals()[f"{chain}_kraken_delta_native"] = float(globals()[f"{chain}_kraken_delta"]) * float(globals()[f"{chain}_token_price"]) / float(globals()[f"{chain}_coin_price"])
    globals()[f"{chain}_kraken_factor"] = float(globals()[f"{chain}_kraken_delta_native"]) / float(globals()[f"{chain}_reserves_coin"]) * 100
    globals()[f"{chain}_everswap_usd"] = float(globals()[f"{chain}_everswap_daily"]) * float(globals()[f"{chain}_coin_price"])
    everswap_total_usd += globals()[f"{chain}_everswap_usd"]
    if(not READONLY):
      cur.execute(f"INSERT INTO {chain}_data (DateTime,TimeStamp,KrakenReserves{chain.upper()},TokenPrice{chain.upper()},CoinPrice{chain.upper()},DailyVolume{chain.upper()},DailyVolume{chain.upper()}inUSD,DailyKrakenTax{chain.upper()},DailyRewards{chain.upper()},KrakenFactor{chain.upper()}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (current_time,time_stamp,globals()[f"{chain}_reserves_coin"],globals()[f"{chain}_token_price"],globals()[f"{chain}_coin_price"],globals()[f"{chain}_volume_24hour"],globals()[f"{chain}_volume_24hour_usd"],globals()[f"{chain}_kraken_tax_24hour"],globals()[f"{chain}_rewards_24hour"],globals()[f"{chain}_kraken_factor"]))
      conn.commit()

  
  print("EverRise Holders: ", total_holders_current)
  for chain in CHAINS:
    print(f"{chain.upper()} Holders: ", globals()[f"{chain}_holders_current"])
  if(not READONLY):
    cur.execute("INSERT INTO holders (TimeStamp,TotalHolders,HoldersBSC,HoldersETH,HoldersPOLY,HoldersFTM,HoldersAVAX) VALUES (?, ?, ?, ?, ?, ?, ?)", (time_stamp,total_holders_current,bsc_holders_current,eth_holders_current,poly_holders_current,ftm_holders_current,avax_holders_current))
    conn.commit()


  print("Liquidity: ")
  print("USD Unified Total:   $", round(usd_liquidity_total,2))
  for chain in CHAINS:
        print(f"{chain.upper()} Token Liquidity: $","{:,}".format(round(globals()[f"{chain}_liquidity_token_usd"],2)))
        print(f"{chain.upper()} Coin  Liquidity: $","{:,}".format(round(globals()[f"{chain}_liquidity_coin_usd"],2)))
        
  print("\nEverRise 24 Hour Volume:")
  print("Total:    $","{:,}".format(round(everrise_volume_24hour_usd),2))
  print("Total:     ","{:,}".format(round(everrise_volume_24hour),2))
  print("To Kraken: ","{:,}".format(round(kraken_tax_24hour_unified),2))
  for chain in CHAINS:
    print(f"{chain}:   ","{:,}".format(round(globals()[f"{chain}_volume_24hour"],2)))
    print(f"To Kraken: ","{:,}".format(round(globals()[f"{chain}_kraken_tax_24hour"],2)))

  print("\nEverRise 24 Hour Rewards:")
  print("Total:     ", round(float(total_rewards_24hour_formatted.replace(',',''))),2)
  for chain in CHAINS:
    print(f"{chain}: ", "{:,}".format(round(globals()[f"{chain}_rewards_24hour"],2)))

  print("\nKraken Deltas:")
  print("Total:     ", round(kraken_delta_unified,2))
  for chain in CHAINS:
    print(f"{chain}: ", "{:,}".format(round(globals()[f"{chain}_kraken_delta"],2)))

  print("\nKraken Factor:")
  print("Total:     ", round(kraken_factor_unified,2))
  for chain in CHAINS:
    print(f"{chain}: ", "{:,}".format(round(globals()[f"{chain}_kraken_factor"],2)))

  print("\nEverSwap Daily Income:")
  print("Total:    $", round(everswap_total_usd,2))
  for chain in CHAINS:
    print(f"{chain}: ", "{:,}".format(round(globals()[f"{chain}_everswap_usd"],2)))

  if(not READONLY):
    cur.execute("INSERT INTO everswap_data (TimeStamp,EverSwapTotalUSD,EverSwapBSC,EverSwapBSCinUSD,EverSwapETH,EverSwapETHinUSD,EverSwapPOLY,EverSwapPOLYinUSD,EverSwapFTM,EverSwapFTMinUSD,EverSwapAVAX,EverSwapAVAXinUSD) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (time_stamp,everswap_total_usd,bsc_everswap_daily,bsc_everswap_usd,eth_everswap_daily,eth_everswap_usd,poly_everswap_daily,poly_everswap_usd,ftm_everswap_daily,ftm_everswap_usd,avax_everswap_daily,avax_everswap_usd))
    conn.commit()

def connectToMariaDB():
    print("Connecting to remote MariaDB Instance...")
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
