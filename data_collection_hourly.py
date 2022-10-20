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
    query = "CREATE TABLE IF NOT EXISTS liquidity_usd (TimeStamp bigint, TotalLiquidityTokenUSD float, TotalLiquidityCoinUSD float, LiquidityTokenBSC float, LiquidityCoinBSC float, LiquidityTokenETH float, LiquidityCoinETH float, LiquidityTokenPOLY float, LiquidityCoinPOLY float, LiquidityTokenFTM float, LiquidityCoinFTM float, LiquidityTokenAVAX float, LiquidityCoinAVAX float);"
    cur.execute(query)
    query = "CREATE TABLE IF NOT EXISTS liquidity (TimeStamp bigint, TotalLiquidityToken float, LiquidityTokenBSC float, LiquidityCoinBSC float, LiquidityTokenETH float, LiquidityCoinETH float, LiquidityTokenPOLY float, LiquidityCoinPOLY float, LiquidityTokenFTM float, LiquidityCoinFTM float, LiquidityTokenAVAX float, LiquidityCoinAVAX float);"
    cur.execute(query)
    query = "CREATE TABLE IF NOT EXISTS price_data (TimeStamp bigint, TokenPriceBSC float, CoinPriceBSC float, TokenPriceETH float, CoinPriceETH float, TokenPricePOLY float, CoinPricePOLY float, TokenPriceFTM float, CoinPriceFTM float, TokenPriceAVAX float, CoinPriceAVAX float);"
    cur.execute(query)

# GET EVERRISE DATA #

  everrise_data = requests.get(EVERRISE_STATS).json()

# UNIFIED DATA #

  everrise_data_unified = everrise_data["unified"]
  everrise_data_unified_current = everrise_data_unified["current"]
  everrise_unified_price = everrise_data_unified_current["tokenPriceStable"]
  everrise_volume_trade_current = everrise_data_unified_current["volumeTrade"]
  everrise_volume_trade_current_usd = everrise_data_unified_current["usdVolumeTrade"]
  unified_liquidity_token = everrise_data_unified_current["liquidityToken"]
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
  kraken_tax_24hour_unified = everrise_volume_24hour * 0.05

  kraken_delta_unified = float(kraken_tax_24hour_unified) - float(total_rewards_24hour)
  kraken_factor_unified = float(kraken_delta_unified) * float(everrise_unified_price) / float(everrise_usd_reserves_balance) * 100

  for chain in CHAINS:
    globals()[f"{chain}_name"] = "{:<4}".format(f"{chain.upper()}")
    globals()[f"{chain}"] = everrise_data[chain]
    globals()[f"{chain}_current"] = globals()[f"{chain}"]["current"]
    globals()[f"{chain}_volume_trade_current"] = globals()[f"{chain}_current"]["volumeTrade"]
    globals()[f"{chain}_reserves"] = globals()[f"{chain}_current"]["reservesTokenBalance"]
    globals()[f"{chain}_token_price"] = globals()[f"{chain}_current"]["tokenPriceStable"]
    globals()[f"{chain}_coin_price"] = globals()[f"{chain}_current"]["coinPriceStable"]
    globals()[f"{chain}_liquidity_token"] = globals()[f"{chain}_current"]["liquidityToken"]
    globals()[f"{chain}_liquidity_coin"] = globals()[f"{chain}_current"]["liquidityCoin"]
    globals()[f"{chain}_liquidity_token_usd"] = float(globals()[f"{chain}_token_price"]) * float(globals()[f"{chain}_liquidity_token"])
    globals()[f"{chain}_liquidity_coin_usd"] = float(globals()[f"{chain}_coin_price"]) * float(globals()[f"{chain}_liquidity_coin"])
    globals()[f"{chain}_liquidity_total_usd"] = float(globals()[f"{chain}_liquidity_token_usd"]) + float(globals()[f"{chain}_liquidity_coin_usd"])
    globals()[f"{chain}_holders_current"] = globals()[f"{chain}_current"]["holders"]
    globals()[f"{chain}_rewards_current"] = globals()[f"{chain}_current"]["rewards"]
    globals()[f"{chain}_history24hours"] = globals()[f"{chain}"]["history24hrs"]
    globals()[f"{chain}_volume_trade_24hour"] = globals()[f"{chain}_history24hours"]["volumeTrade"]
    globals()[f"{chain}_rewards_24hour_history"] = globals()[f"{chain}_history24hours"]["rewards"]
    globals()[f"{chain}_rewards_24hour"] = float(globals()[f"{chain}_rewards_current"]) - float(globals()[f"{chain}_rewards_24hour_history"])
    globals()[f"{chain}_volume_24hour"] = float(globals()[f"{chain}_volume_trade_current"]) - float(globals()[f"{chain}_volume_trade_24hour"])
    globals()[f"{chain}_kraken_tax_24hour"] = globals()[f"{chain}_volume_24hour"] * TAX_RATE
    globals()[f"{chain}_kraken_delta"] = float(globals()[f"{chain}_kraken_tax_24hour"]) - float(globals()[f"{chain}_rewards_24hour"])
    globals()[f"{chain}_kraken_delta_native"] = float(globals()[f"{chain}_kraken_delta"]) * float(globals()[f"{chain}_token_price"]) / float(globals()[f"{chain}_coin_price"])
    globals()[f"{chain}_kraken_factor"] = float(globals()[f"{chain}_kraken_delta_native"]) / float(globals()[f"{chain}_reserves"]) * 100


# DISPLAY AND SAVE DATA

  print("\nLiquidity: ")
  print("USD Unified Total:   $","{:,}".format(round(usd_liquidity_total)))
  for chain in CHAINS:
        print(globals()[f"{chain}_name"], "Token Liquidity: $","{:>15}".format("{:,}".format(round(globals()[f"{chain}_liquidity_token_usd"],2))))
        print(globals()[f"{chain}_name"], "Tokens in LP:     ","{:>15}".format("{:,}".format(round(float(globals()[f"{chain}_liquidity_token"]),2))))
        

  

  if(not READONLY):
    cur.execute("INSERT INTO liquidity_usd (DateTime,TimeStamp,TotalLiquidityTokenUSD,TotalLiquidityCoinUSD,LiquidityTokenBSC,LiquidityCoinBSC,LiquidityTokenETH,LiquidityCoinETH,LiquidityTokenPOLY,LiquidityCoinPOLY,LiquidityTokenFTM,LiquidityCoinFTM,LiquidityTokenAVAX,LiquidityCoinAVAX) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (current_time,time_stamp,usd_liquidity_token,usd_liquidity_coin,bsc_liquidity_token_usd,bsc_liquidity_coin_usd,eth_liquidity_token_usd,eth_liquidity_coin_usd,poly_liquidity_token_usd,poly_liquidity_coin_usd,ftm_liquidity_token_usd,ftm_liquidity_coin_usd,avax_liquidity_token_usd,avax_liquidity_coin_usd))
    conn.commit()
    cur.execute("INSERT INTO liquidity (DateTime,TimeStamp,TotalLiquidityToken,LiquidityTokenBSC,LiquidityCoinBSC,LiquidityTokenETH,LiquidityCoinETH,LiquidityTokenPOLY,LiquidityCoinPOLY,LiquidityTokenFTM,LiquidityCoinFTM,LiquidityTokenAVAX,LiquidityCoinAVAX) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (current_time,time_stamp,unified_liquidity_token,bsc_liquidity_token,bsc_liquidity_coin,eth_liquidity_token,eth_liquidity_coin,poly_liquidity_token,poly_liquidity_coin,ftm_liquidity_token,ftm_liquidity_coin,avax_liquidity_token,avax_liquidity_coin))
    conn.commit()
    cur.execute("INSERT INTO price_data (DateTime,TimeStamp,TokenPriceBSC,CoinPriceBSC,TokenPriceETH,CoinPriceETH,TokenPricePOLY,CoinPricePOLY,TokenPriceFTM,CoinPriceFTM,TokenPriceAVAX,CoinPriceAVAX) VALUES (? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (current_time,time_stamp,bsc_token_price,bsc_coin_price,eth_token_price,eth_coin_price,poly_token_price,poly_coin_price,ftm_token_price,ftm_coin_price,avax_token_price,avax_coin_price))
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
