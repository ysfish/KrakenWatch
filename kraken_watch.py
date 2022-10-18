#!/usr/bin/env python3

import telegram
import tweepy
import mariadb
import requests
from credentials import *
from datetime import datetime
from config import *

def main():
  conn = connectToMariaDB()
  cur = conn.cursor()
  for CHAIN in CHAINS1:
    print("Current Blockchain: ", CHAIN.upper())
    cur.execute(f"SELECT BlockNumber FROM {CHAIN}_kraken_watch;")
    start_block = cur.fetchall()[-1][0] + 1
    if(CHAIN == "bsc"):
      CHAIN2 = "bsc"
      CHAIN3 = "BNB Chain"
      NATIVE = "BNB"
      router = PANCAKESWAP
      request_url = API_URL_BSC + API_URL_TXINTERNAL + CONTRACT_ADDRESS + "&startblock=" + str(start_block) + API_URL_ENDBLOCK + BSC_API_KEY
      telegram_hash_source = "\n🐋🐋 TX Hash: [BSCScan](https://bscscan.com/tx/"
      twitter_hash_source = "\n🐋 TX Hash: https://bscscan.com/tx/"
    if(CHAIN == "eth"):
      CHAIN2 = "eth"
      CHAIN3 = "Ethereum"
      NATIVE = "ETH"
      router = UNISWAP
      request_url = API_URL_ETH + API_URL_TXINTERNAL + CONTRACT_ADDRESS + "&startblock=" + str(start_block) + API_URL_ENDBLOCK + ETH_API_KEY
      telegram_hash_source = "\n🐋🐋 TX Hash: [EtherScan](https://etherscan.io/tx/"
      twitter_hash_source = "\n🐋 TX Hash: https://etherscan.io/tx/"
    if(CHAIN == "poly"):
      CHAIN2 = "polygon"
      CHAIN3 = "Polygon"
      NATIVE = "MATIC"
      router = QUICKSWAP
      request_url = API_URL_POLY + API_URL_TXINTERNAL + CONTRACT_ADDRESS + "&startblock=" + str(start_block) + API_URL_ENDBLOCK + POLY_API_KEY
      telegram_hash_source = "\n🐋🐋 TX Hash: [PolygonScan](https://polygonscan.com/tx/"
      twitter_hash_source = "\n🐋 TX Hash: [PolygonScan](https://polygonscan.com/tx/"
    bot = connectToTelegram()
    api = connectToTwitter()
    current_time = datetime.utcnow()
    print("Current Time:        ",current_time)
    print("Getting EverRise data...")
    everrise_current_data = requests.get(EVERRISE_STATS).json()[CHAIN]["current"]
    everrise_staked = everrise_current_data["staked"]
    everrise_multiplier = everrise_current_data["aveMultiplier"]
    current_token_price = everrise_current_data["tokenPriceStable"]
    current_coin_price = everrise_current_data["coinPriceStable"]
    internal_txns = requests.get(request_url).json()["result"]
    for item in internal_txns:
      if item['from'] == CONTRACT_ADDRESS.lower():
        if item['to'] == str(router).lower():
          kraken_found = 1
        else:
          kraken_found = 0
      else:
        kraken_found = 0
      if kraken_found == 1:
        native_value = int(item['value'])/(10 ** DECIMALS)
        current_rewards = float(native_value) * float(current_coin_price) / float(current_token_price)
        staker_percentage = (float(current_rewards) * 36) / (float(everrise_staked) * float(everrise_multiplier)) * 100
        kaboom_datetime = datetime.utcfromtimestamp(int(item['timeStamp']))
        print("Date/Time of Kaboom:   ",kaboom_datetime)
        print("TimeStamp:             ",item['timeStamp'])
        print("Block Number:          ",item['blockNumber'])
        print(f"{NATIVE} Value:       ",native_value)
        print("EverRise Value:        ",current_rewards)
        print("Transaction Hash:      ",item['hash'])
        cur.execute(f"INSERT INTO {CHAIN}_kraken_watch (TimeStamp,BlockNumber,{NATIVE}Value,TransactionHash) VALUES (?, ?, ?, ?)", (item['timeStamp'],item['blockNumber'],native_value,item['hash']))
        conn.commit()
        cur.execute(f"INSERT INTO {CHAIN}_kraken_rewards (TimeStamp,{NATIVE}Rewards,{NATIVE}Performance) VALUES (?, ?, ?)", (item['timeStamp'],current_rewards,staker_percentage))
        conn.commit()
        telegram_message = f"🐋🐋 *{CHAIN.upper()} Kraken Sighting:*\n🐋🐋 *Date/Time of Kaboom:* " + str(kaboom_datetime) + "\n🐋🐋 *Block Number:*               " + str(item['blockNumber']) + f"\n🐋🐋 *{NATIVE} Value:*                      " + str(native_value) + "\n🐋🐋 *RISE Rewards Distributed:*      " + str("{:,}".format(round(current_rewards,2))) + " RISE\n🐋🐋 *36x Stake Performance:*          " + str(round(staker_percentage,6)) + "%"
        bot.send_message(chat_id=CHAT_ID, text=telegram_message, parse_mode=telegram.ParseMode.MARKDOWN)
        twitter_message = f"🐋 #EverRise $RISE\n\n🐋 {CHAIN.upper()} Kraken:\n\nTime: " + str(kaboom_datetime) + f"\n🐋 {NATIVE}:  " + str(native_value) + "\n🐋 RISE: " + str("{:,}".format(round(current_rewards,2))) + "\n🐋 36x Staker Earned: " + str(round(staker_percentage, 6)) + "%"
        api.update_status(status=twitter_message)
  for CHAIN in CHAINS2:
    print("Current Blockchain: ", CHAIN.upper())
    cur.execute(f"SELECT BlockNumber FROM {CHAIN}_kraken_watch;")
    start_block = cur.fetchall()[-1][0] + 1
    if(CHAIN == "ftm"):
      CHAIN2 = "fantom"
      CHAIN3 = "Fantom"
      NATIVE = "FTM"
      router = SPOOKYSWAP
      request_url1 = API_URL_FTM + API_URL_TXLIST + CROSS_CHAIN_BUYBACK_ADDRESS + "&startblock=" + str(start_block) + API_URL_ENDBLOCK + FTM_API_KEY
      telegram_hash_source = "\n🐋🐋 TX Hash: [FTMScan](https://ftmscan.com/tx/"
      twitter_hash_source = "\n🐋 TX Hash: [FTMScan](https://ftmscan.com/tx/"
    if(CHAIN == "avax"):
      CHAIN2 = "avalanche"
      CHAIN3 = "Avalanche"
      NATIVE = "AVAX"
      router = TRADERJOE
      request_url1 = API_URL_AVAX + API_URL_TXLIST + CROSS_CHAIN_BUYBACK_ADDRESS + "&startblock=" + str(start_block) + API_URL_ENDBLOCK + AVAX_API_KEY
      telegram_hash_source = "\n🐋🐋 TX Hash: [SnowTrace](https://snowtrace.io/tx/"
      twitter_hash_source = "\n🐋 TX Hash: [SnowTrace](https://snowtrace.io/tx/"
    bot = connectToTelegram()
    api = connectToTwitter()
    current_time = datetime.utcnow()
    print("Current Time:        ",current_time)
    print("Getting EverRise data...")
    everrise_current_data = requests.get(EVERRISE_STATS).json()[CHAIN]["current"]
    everrise_staked = everrise_current_data["staked"]
    everrise_multiplier = everrise_current_data["aveMultiplier"]
    current_token_price = everrise_current_data["tokenPriceStable"]
    current_coin_price = everrise_current_data["coinPriceStable"]
    txns_result = requests.get(request_url1).json()["result"]
    for item in txns_result:
      if item['from'] == CROSS_CHAIN_BUYBACK_ADDRESS.lower():
        kraken_found = 1
      else:
        kraken_found = 0
      if kraken_found == 1:
        txn_hash = item['hash']
        print("Kraken Found!")
        if(CHAIN == "ftm"):
          request_url2 = "https://api.ftmscan.com/api?module=account&action=txlistinternal&txhash=" + txn_hash + "&apikey=" + FTM_API_KEY
        if(CHAIN == "avax"):
          request_url2 = "https://api.snowtrace.io/api?module=account&action=txlistinternal&txhash=" + txn_hash + "&apikey=" + AVAX_API_KEY
        internal_txns_result = requests.get(request_url2).json()["result"]
        if internal_txns_result == []:
          query = f"SELECT {NATIVE}Value FROM {CHAIN}_kraken_watch;"
          cur.execute(query)
          native_value = cur.fetchall()[-1][0]
        else:  
          native_value = int(internal_txns_result[0]['value'])/(10 ** DECIMALS)
        current_rewards = float(native_value) * float(current_coin_price) / float(current_token_price)
        staker_percentage = (float(current_rewards) * 36) / (float(everrise_staked) * float(everrise_multiplier)) * 100
        kaboom_datetime = datetime.utcfromtimestamp(int(item['timeStamp']))
        print('Date/Time of Kaboom: ',kaboom_datetime)
        print('Block Number:        ',item['blockNumber'])
        print(f"{NATIVE} Value:          ",native_value)
        print("Transaction Hash:      ",item['hash'])
        cur.execute(f"INSERT INTO {CHAIN}_kraken_watch (DateTime,TimeStamp,BlockNumber,{NATIVE}Value,TransactionHash) VALUES (?, ?, ?, ?, ?)", (kaboom_datetime,item['timeStamp'],item['blockNumber'],native_value,item['hash']))
        conn.commit()
        cur.execute(f"INSERT INTO {CHAIN}_kraken_rewards (DateTime,TimeStamp,{NATIVE}Rewards,{NATIVE}Performance) VALUES (?, ?, ?, ?)", (kaboom_datetime,item['timeStamp'],current_rewards,staker_percentage))
        conn.commit()
        telegram_message = f"🐋🐋 *{CHAIN.upper()} Kraken Sighting:*\n*🐋🐋 Date/Time of Kaboom:* " + str(kaboom_datetime) + "\n🐋🐋 *Block Number:*               " + str(item['blockNumber']) + f"\n🐋🐋 *{NATIVE} Value:*                      " + str(native_value) + "\n🐋🐋 *RISE Rewards Distributed:*      " + str("{:,}".format(round(current_rewards,2))) + " RISE\n🐋🐋 *36x Stake Performance:*          " + str(round(staker_percentage,6)) + "%"
        bot.send_message(chat_id=CHAT_ID, text=telegram_message, parse_mode=telegram.ParseMode.MARKDOWN)
        twitter_message = f"🐋 #EverRise $RISE\n\n🐋 {CHAIN.upper()} Kraken:\n\n🐋 Time: " + str(kaboom_datetime) + f"\n🐋 {NATIVE}: " + str(native_value) + "\n🐋 RISE: " + str("{:,}".format(round(current_rewards,2))) + "\n🐋 36x Staker Earned: " + str(round(staker_percentage, 6)) + "%"
        api.update_status(status=twitter_message)

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
    print("Connecting to  MariaDB Instance...")
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
