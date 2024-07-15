import asyncio
import csv
import requests
import math
import sys
import nest_asyncio
import pandas as pd
import time
# from ta.trend import MACD
# from confighandler import ConfigHandler
# from collections import deque
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler
from ib_insync import *

nest_asyncio.apply()

class Nse:

    def __init__(self) -> None:
        
        self.url_oc: str = "https://www.nseindia.com/option-chain"
        self.url_stock: str = "https://www.nseindia.com/api/option-chain-equities?symbol="
        self.url_index: str = "https://www.nseindia.com/api/option-chain-indices?symbol="
        self.url_symbols: str = "https://www.nseindia.com/api/underlying-information"
        self.headers: Dict[str, str] = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                          'like Gecko) Chrome/118.0.5993.71 Safari/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8',
            'accept-encoding': 'gzip, deflate, br'}
        self.session: requests.Session = requests.Session()
        self.cookies: Dict[str, str] = {}
        self.stocks_to_monitor: list = []
        self.config: list = []
        self.oi_stocks: list = []
        self.stock_set: list = []
        self.prices: dict = {}
        self.positions: dict = {}
        self.today_positions: dict = {}
        self.sizes: dict = {}
        self.levels: dict = {}
        self.symbols_with_signals: dict = {}
        self.underlying: dict = {}
        self.highs: dict = {}
        self.lows: dict = {}
        self.fair_price: dict = {}
        self.long_positions : int = 0
        self.short_positions : int = 0
        self.total_positions : int = 0 
        self.max_long_positions : int = 6
        self.max_short_positions : int = 6
        self.max_total_positions : int = 10
        self.ticker : dict = {}
        self.threshold: dict = {}
        self.stop_loss_price: dict = {}
        self.target_price: dict = {}
        self.entry_prices: dict = {}
        self.short_target_factor: dict = {}
        self.trailing_factor: dict = {}
        self.expiry_date: str
        self.contract_expiry_date: str
        self.sigma: dict = {}
        self.ib = IB()
        self._ib_connected = False
        self.ib.connect('127.0.0.1', 7496, clientId=39)
        self.ib.disconnectedEvent += self.reconnect


    def get_data(self, symbol):
        request: Optional[requests.Response] = None
        response: Optional[requests.Response] = None
        url = self.url_stock + symbol
        try:
            request: requests.Response = self.session.get(self.url_oc, headers=self.headers, timeout=5)
            self.cookies = dict(request.cookies)
            response = self.session.get(url, headers=self.headers, timeout=5, cookies=self.cookies)
            if response.status_code == 401:
                print('4')
                self.session.close()
                self.session = requests.Session()
                request = self.session.get(self.url_oc, headers=self.headers, timeout=5)
                self.cookies = dict(request.cookies)
                response = self.session.get(url, headers=self.headers, timeout=5, cookies=self.cookies)
                print("reset cookies")
        except Exception as err:
            #print(request)
            #print(response)
            #print(err, sys.exc_info()[0], "4")
            try:
                self.session.close()
                self.session = requests.Session()
                request = self.session.get(self.url_oc, headers=self.headers, timeout=5)
                self.cookies = dict(request.cookies)
                response = self.session.get(url, headers=self.headers, timeout=5, cookies=self.cookies)
                #print("reset cookies after exception")
            except Exception as err:
                print(err, sys.exc_info()[0], "5")
                return
        if response is not None:
            try:
                json_data: Any = response.json()
            except Exception as err:
                print(err, sys.exc_info()[0], "6")
                json_data = {}
        else:
            json_data = {}
        if json_data == {}:
            return

        return json_data

    def fetch_data_for_symbol(self, symbol: str):

        while True:
            try:
                json_data = self.get_data(symbol)  # Assuming get_data is now async
                if json_data is not None:
                    return symbol, self.get_local_time_string(), json_data
                else:
                    return
            except TypeError:
                return

    def get_local_time_string(self):

        now = datetime.now()
        
        return now.strftime("%H:%M:%S")
    
    def calculate_pcr(self, data, threshold):
        call_oi = None
        put_oi = None
        for item in data['records']['data']:
            if item['expiryDate'] == self.expiry_date and float(item['strikePrice']) == float(threshold):
                if 'CE' in item:
                    call_oi = item['CE']['openInterest']
                if 'PE' in item:
                    put_oi = item['PE']['openInterest']
        
        if call_oi is not None and call_oi != 0:
            return round(put_oi/call_oi, 2)
        else:
            return 
        
    def reconnect(self):

        self.ib.disconnect()

        while not self.ib.isConnected():
            print("Disconnected from TWS! Trying to reconnect...")
            try:
                self.ib.connect('127.0.0.1', 7496, clientId=39)  # Replace with your TWS details
                self.ib.sleep(2)  # Wait for connection
                print("Reconnected successfully!")
                break  # Exit the loop on successful reconnection
            except Exception as e:
                print(f"Reconnection failed: {e}")
                self.ib.sleep(5)  # Wait before retrying

    def calculate_fair_price(self, data):
        call_oi = {}
        put_oi = {}

        for item in data['records']['data']:
            if item['expiryDate'] == self.expiry_date:
                strike = item['strikePrice']
                if 'CE' in item:
                    call_oi[strike] = item['CE']['openInterest']
                if 'PE' in item:
                    put_oi[strike] = item['PE']['openInterest']

        total_product = total_oi = 0
        for strike in call_oi:
            oi_diff = abs(call_oi.get(strike, 0) - put_oi.get(strike, 0))
            total_product += strike * oi_diff
            total_oi += oi_diff
        
        return round(total_product / total_oi, 2) if total_oi else None
    
    def calculate_points(self, data):

        pe_values: List[dict] = [data['PE'] for data in data['records']['data'] if
                                 "PE" in data and data['expiryDate'].lower() == self.expiry_date.lower()]
        points: float = pe_values[0]['underlyingValue']
        if points == 0:
            for item in pe_values:
                if item['underlyingValue'] != 0:
                    points = item['underlyingValue']
                    break

        return points

    def calculate_pcr_around_threshold(self, data, market_price, sigma):
 
        change_sum = {"call_change": 0, "put_change": 0}
        
        for item in data['records']['data']:
            if item["expiryDate"] == self.expiry_date:
                current_strike = item["strikePrice"]

                # Calculate distance from reference strike (positive for above, negative for below)
                distance = (current_strike - market_price) * 100 / (sigma * market_price)  # Normalize to strike units

                if ('CE' in item) and abs(distance) <= 1.5:
                    change_sum["call_change"] += item['CE']['changeinOpenInterest']
                if ('PE' in item) and abs(distance) <= 1.5:
                    change_sum["put_change"] += item['PE']['changeinOpenInterest']

        if change_sum["put_change"] == 0 and change_sum["call_change"] == 0:
            return 1
        elif change_sum["put_change"] < 0 and change_sum["call_change"] < 0:
            return round(change_sum["call_change"] / change_sum["put_change"], 2)
        elif change_sum["put_change"] <= 0 and change_sum["call_change"] >= 0:
            return 0.1
        elif change_sum["put_change"] >= 0 and change_sum["call_change"] <= 0:
            return 10
        else:
            return round(change_sum["put_change"] / change_sum["call_change"], 2)
    
    def get_market_data(self, contract:Contract):
        
        try:
            self.ticker[contract.symbol] = self.ib.reqMktData(contract, '233')
                
            while self.ib.sleep(0.5):
                
                market_price = self.ticker.get(contract.symbol).marketPrice()
                # vwap_price = self.ticker.get(contract.symbol).vwap
                if not math.isnan(market_price): #and not math.isnan(vwap_price):
                    # self.ib.cancelMktData(contract)
                    return market_price #, vwap_price
        except:
            self.reconnect()
            return
        
    def calculate_thresholds(self, spot_price):
        # Define ranges and corresponding step magnitudes
        ranges_and_steps = [(50, 5), (100, 10), (800, 50), (3000, 100), (5000, 200), (20000, 500), (50000, 1000)]

        # Determine the appropriate step magnitude for the given stock price
        for price_limit, step_magnitude in ranges_and_steps:
            if spot_price < price_limit:
                break
        else:
            step_magnitude = 2000  # Default step for prices above the highest range

        # Calculate thresholds
        lower_threshold = spot_price - (spot_price % step_magnitude)
        upper_threshold = lower_threshold + step_magnitude

        distance1 = abs(spot_price - lower_threshold)
        distance2 = abs(spot_price - upper_threshold)

        if distance1 < distance2:
            return lower_threshold
        else:
            return upper_threshold
        
    def get_nearest_expiry_date(self, data):

        expiry_dates = data['records']['expiryDates']
        date_objects = [datetime.strptime(date, "%d-%b-%Y") for date in expiry_dates]

        # Get today's date
        today = datetime.today()

        # Find the date object closest to today (considering future dates only)
        nearest_date = min(date_objects, key=lambda x: abs(x - today))

        # Convert the nearest date object back to string format
        return nearest_date.strftime("%d-%b-%Y")
    
    def handle_order_event(self, trade:Trade, fill: Fill):
        
        underlying = self.underlying.get(trade.contract.symbol)
        stock_sigma = float(self.sigma.get(underlying))

        if self.levels.get(trade.contract.symbol) == 1:
            self.entry_prices[trade.contract.symbol] = fill.execution.avgPrice
        elif self.levels.get(trade.contract.symbol) == 2:
            prev_price = self.entry_prices.get(trade.contract.symbol)
            self.entry_prices[trade.contract.symbol] = (prev_price + (2 * fill.execution.avgPrice)) / 3
            self.levels[trade.contract.symbol] = 3
        else:
            del self.levels[trade.contract.symbol]

        if trade.order.action == 'BUY' and (trade.contract.symbol in self.positions):

            if self.levels.get(trade.contract.symbol) == 1:
                self.target_price[trade.contract.symbol] = self.entry_prices.get(trade.contract.symbol) * (1 + (0.55 * stock_sigma / 100))
                self.stop_loss_price[trade.contract.symbol] = self.entry_prices.get(trade.contract.symbol) * (1 - (1.2 * stock_sigma / 100))
            elif self.levels.get(trade.contract.symbol) == 3:
                self.target_price[trade.contract.symbol] = self.entry_prices.get(trade.contract.symbol) * (1 + (0.3 * stock_sigma / 100))

        elif trade.order.action == 'SELL' and (trade.contract.symbol in self.positions):
            if self.levels.get(trade.contract.symbol) == 1:
                self.target_price[trade.contract.symbol] = self.entry_prices.get(trade.contract.symbol) * (1 - (0.6 * stock_sigma / 100))
                self.stop_loss_price[trade.contract.symbol] = self.entry_prices.get(trade.contract.symbol) * (1 + (1.2 * stock_sigma / 100))
            elif self.levels.get(trade.contract.symbol) == 3:
                self.target_price[trade.contract.symbol] = self.entry_prices.get(trade.contract.symbol) * (1 - (0.3 * stock_sigma / 100))

        self.ib.sleep(0.5)

        print(f"Order filled at price for {trade.contract.symbol}:", fill.execution.avgPrice)
        print(f"Target Price is set for {trade.contract.symbol} at {self.target_price.get(trade.contract.symbol)}")
        print(f"Stop Loss Price is set for {trade.contract.symbol} at {self.stop_loss_price.get(trade.contract.symbol)}")
    
    def trade_future(self, symbol, action, level, limit_price=None):

        # Define the option contract
        date_obj = datetime.strptime(self.contract_expiry_date, "%d-%b-%Y")
        expiry = date_obj.strftime("%Y%m%d")
        underlying = self.underlying.get(symbol)
        contract = Future(
            symbol=symbol,
            lastTradeDateOrContractMonth=expiry,
            exchange='NSE',  # Adjust for your preferred exchange
            currency='INR',  # Adjust for your currency
        )


        if level != 0:
            multiplier = level
        else:
            multiplier = self.levels.get(symbol)

        quantity = self.sizes.get(underlying) * multiplier
        # Create the order
        order = Order(
            action=action,
            orderType='LMT' if limit_price else 'MKT',  # Use limit order if price specified
            totalQuantity = quantity 
        )
        

        if limit_price:
            order.lmtPrice = limit_price

        # Connect to TWS and place the order
        trade = self.ib.placeOrder(contract, order)
        trade.fillEvent += self.handle_order_event
        
        return

    def fetch_highs_and_lows(self, symbol, barsize):

        contract = Stock(symbol, exchange="NSE", currency="INR")  # Adjust for your specific exchange
        enddate = datetime.date.today()
        bars = self.ib.reqHistoricalData(
            contract, 
            endDateTime=enddate, 
            durationStr='10 D', 
            barSizeSetting=barsize, 
            whatToShow='TRADES', 
            useRTH=True,
            formatDate=1
        )
        high = None 
        low = None

        for bar in bars:
            if bar.high > high or high is None:
                high = bar.high

            if bar.low < low or low is None:
                low = bar.low

        return high, low

    def create_symbol_dict(self, symbols):

        symbol_dict = {}
        for symbol in symbols:
            modified_symbol = symbol[:9].replace('&', '')
            symbol_dict[modified_symbol] = symbol
        return symbol_dict


    async def continuously_fetch_data(self):

        stop_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=25)
        start_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=20) 
        end_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=14, minute=50)
        time_1 = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=45)

        i : int = -1
        batch_size = 10

        while True:
            try: 
                now = datetime.now(timezone(timedelta(hours=5.5)))

                if now >= stop_time:
                    print("Time limit reached, stopping execution.")
                    break

                if (i < math.floor(len(self.stocks_to_monitor) / batch_size)):    
                    i += 1
                else:
                    i = 0
                    

                stock_batch = self.stocks_to_monitor[i * batch_size : (i + 1) * batch_size]
                
                results = [(self.fetch_data_for_symbol(symbol)) for symbol in stock_batch]
                filtered_results = list(filter(None, results)) 
                
                for result in filtered_results:

                    symbol, current_time, data = result
                    spot_price = self.calculate_points(data)
                    fair_price = self.calculate_fair_price(data)
                    underlying = symbol[:9].replace('&', '')
                    
                    if (start_time <= now <= end_time):# and (today_fair is not None)
                        
                        if data is not None:
                        
                            stock_sigma = float(self.sigma.get(symbol))
                            self.threshold[symbol] = self.calculate_thresholds(spot_price)
                            
                            pcr_at_threshold = self.calculate_pcr(data, self.threshold.get(symbol))
                            near_threshold: bool = (abs((spot_price - self.threshold.get(symbol)) * 100 / spot_price) <= 1 * stock_sigma)


                            if underlying not in self.positions.copy():
                                
                                if pcr_at_threshold < 1 and near_threshold:
                                                                                         
                                    self.symbols_with_signals[symbol] = "Short"
                            
                                elif pcr_at_threshold > 1 and near_threshold:
                                        
                                    self.symbols_with_signals[symbol] = "Long"  

                                else:
                                    if symbol in self.symbols_with_signals.copy():
                                        del self.symbols_with_signals[symbol]

                    elif (now >= end_time): 

                        if symbol in self.symbols_with_signals.copy():
                            del self.symbols_with_signals[symbol]           
                            
                    
                print("Waiting for next update...")
                if now > time_1:
                    await asyncio.sleep(120)
                else: 
                    await asyncio.sleep(45)
                
            except Exception as e:
                print(symbol)
                print("Error:", e)

    async def monitor_and_execute_orders(self):

        stop_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=20) 
        end_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=14, minute=50)
        start_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=35)   
        time_1 = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=11, minute=00)
        time_2 = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=13, minute=00)

        while True:
            now = datetime.now(timezone(timedelta(hours=5.5)))

            if now >= stop_time:
                print("Time limit reached, stopping execution.")
                await asyncio.sleep(1)
                break

            if len(self.symbols_with_signals.copy()) > 0 and (now <= end_time):
                try:
                    # print(self.symbols_with_signals.copy())
                    if (now >= start_time):
                        

                        for symbol in self.symbols_with_signals.copy():

                            underlying = symbol[:9].replace('&', '')

                            if self.total_positions < self.max_total_positions and not self.today_positions.get(underlying):
                                
                                if self.symbols_with_signals.get(symbol) == "Long" and self.long_positions < self.max_long_positions:
                                    
                                    contract = Stock(symbol[:9].replace('&', ''), exchange="NSE", currency="INR")
                                    market_price = self.get_market_data(contract)
                                    stock_sigma = float(self.sigma.get(symbol))
                                    entry_level = ((market_price - self.threshold.get(symbol)) * 100 / (stock_sigma * market_price))
                                    
                                    if -0.25 <= entry_level <= 0.1: 
                                        
                                        print("Trying placing order")
                                        self.positions[underlying] = 'Long'
                                        self.today_positions[underlying] = True
                                        self.levels[underlying] = 1
                                        self.trade_future(underlying, 'BUY', self.levels.get(underlying))
                                        self.ib.cancelMktData(contract)
                                        self.long_positions += 1
                                        self.total_positions += 1
                                
                                elif self.symbols_with_signals.get(symbol) == "Short" and self.short_positions < self.max_short_positions:
                                    
                                    contract = Stock(symbol[:9].replace('&', ''), exchange="NSE", currency="INR")
                                    market_price = self.get_market_data(contract)
                                    stock_sigma = float(self.sigma.get(symbol))
                                    entry_level = ((self.threshold.get(symbol) - market_price) * 100 / (stock_sigma * market_price))

                                    if -0.25 <= entry_level <= 0.1: 
                                        
                                        print("Trying placing order")
                                        self.positions[underlying] = 'Short'
                                        self.today_positions[underlying] = True
                                        self.levels[underlying] = 1
                                        self.trade_future(underlying, 'SELL', self.levels.get(underlying))
                                        self.ib.cancelMktData(contract)
                                        self.short_positions += 1
                                        self.total_positions += 1
                                    
                        self.ib.sleep(1)
                        self.check_exit_conditions()

                    else:
                        await asyncio.sleep(155)

                except Exception as e:
                    print(symbol)
                    print("Error in monitoring and execution:", e) 

            else:
                self.check_exit_conditions()
    
            
    def check_exit_conditions(self):
        
        try:
            
            now = datetime.now(timezone(timedelta(hours=5.5)))
            end_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=10) 
            start_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=18)
            date_obj = datetime.strptime(self.contract_expiry_date, "%d-%b-%Y")
            expiry = date_obj.strftime("%Y%m%d")

            if now >= start_time:
                self.config = self.extract_stocks_from_file("config.txt")
                for symbol in self.positions.copy():
                    # Get latest market price
                    underlying = self.underlying.get(symbol)
                    contract = Future(symbol, lastTradeDateOrContractMonth=expiry, exchange="NSE", currency="INR")               
                    market_price = self.get_market_data(contract)
                    stock_sigma = float(self.sigma.get(underlying))
                    pos = self.positions.get(symbol)

                    # Check your exit conditions (replace with your actual logic)
                    if pos == "Long":  # Example exit condition
                        # Place exit order
                        threshold = self.entry_prices.get(symbol)
                        n = ((market_price - threshold) * 100 / (stock_sigma * threshold))

                        if market_price >= self.target_price.get(symbol) or now >= end_time or ('EXIT' in self.config) or (symbol in self.config):    
                            
                            del self.positions[symbol]
                            level = 0 
                            self.trade_future(symbol, 'SELL', level)
                            self.long_positions -= 1
                            self.total_positions -= 1 
                            self.ib.cancelMktData(contract)
                        
                        elif market_price <= self.stop_loss_price.get(symbol): 

                            del self.positions[symbol]
                            level = 0
                            self.trade_future(symbol, 'SELL', level)
                            self.long_positions -= 1
                            self.total_positions -= 1 
                            self.ib.cancelMktData(contract) 

                        elif n < -0.9 and self.levels.get(symbol) == 1:
                            self.levels[symbol] = 2
                            self.trade_future(symbol, 'BUY', self.levels.get(symbol))

                        elif (-0.9 < n < -0.65) and self.levels.get(symbol) == 1:
                            self.target_price[symbol] = self.entry_prices.get(symbol) * (1 + (0.3 * stock_sigma / 100))
                            print(f'Target Price is updated for {symbol} to {self.target_price.get(symbol)}')

                        elif (-0.65 < n < -0.4) and self.levels.get(symbol) == 3:
                            self.target_price[symbol] = self.entry_prices.get(symbol) * (1 + (0 * stock_sigma / 100))
                            print(f'Target Price is updated for {symbol} to {self.target_price.get(symbol)}')
                        
                        elif (n > 0.5):
                            self.stop_loss_price[symbol] = self.entry_prices.get(symbol) * (1 + (0.3 * stock_sigma / 100))
                            print(f'Stop Loss Price is updated for {symbol} to {self.stop_loss_price.get(symbol)}')

                    elif pos == "Short":
                        # Place exit order
                        threshold = self.entry_prices.get(symbol)
                        n = ((threshold - market_price) * 100 / (stock_sigma * threshold))

                        if market_price <= self.target_price.get(symbol) or now >= end_time or ('EXIT' in self.config) or (symbol in self.config):
                                
                            del self.positions[symbol]
                            level = 0
                            self.trade_future(symbol, 'BUY', level)
                            self.short_positions -= 1
                            self.total_positions -= 1
                            self.ib.cancelMktData(contract)
                            
                        elif market_price >= self.stop_loss_price.get(symbol): 

                            del self.positions[symbol]
                            level = 0
                            self.trade_future(symbol, 'BUY', level)
                            self.short_positions -= 1
                            self.total_positions -= 1
                            self.ib.cancelMktData(contract)                    

                        elif n < -0.85 and self.levels.get(symbol) == 1:
                            self.levels[symbol] = 2
                            self.trade_future(symbol, 'SELL', self.levels.get(symbol))

                        elif (-0.85 < n < -0.6) and self.levels.get(symbol) == 1:
                            self.target_price[symbol] = self.entry_prices.get(symbol) * (1 - (0.35 * stock_sigma / 100))
                            print(f'Target Price is updated for {symbol} to {self.target_price.get(symbol)}')

                        elif (-0.65 < n < -0.4) and self.levels.get(symbol) == 3:
                            self.target_price[symbol] = self.entry_prices.get(symbol) * (1 - (0 * stock_sigma / 100))
                            print(f'Target Price is updated for {symbol} to {self.target_price.get(symbol)}')

                        elif (n > 0.5):
                            self.stop_loss_price[symbol] = self.entry_prices.get(symbol) * (1 - (0.35 * stock_sigma / 100))
                            print(f'Stop Loss Price is updated for {symbol} to {self.stop_loss_price.get(symbol)}')

            self.ib.sleep(1)
        
        except Exception as e:
            print("Error in exit check: ", e)

    def populate_highs_and_lows(self, symbols):

        for symbol in symbols:
            self.highs[symbol], self.lows[symbol] = self.fetch_highs_and_lows(symbol, '1D')

        return

    def extract_stocks_from_file(self, filename):
        with open(filename, 'r') as file:
            stocks = [line.strip() for line in file.readlines()]
        return stocks

    def read_csv_to_dict(self, file_path):
        symbol_iv_dict = {}

        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                symbol_iv_dict[row['Symbol']] = row['IV']

        return symbol_iv_dict
    
    def read_sizes_to_dict(self, file_path):
        symbol_iv_dict = {}

        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                symbol_iv_dict[row['Symbol']] = row['Size']

        return symbol_iv_dict
    
    def create_symbol_dict(self, symbols):

        symbol_dict = {}
        for symbol in symbols:
            modified_symbol = symbol[:9].replace('&', '')
            symbol_dict[modified_symbol] = symbol
        return symbol_dict

    async def main(self):
            
        filename = "stocks.txt"
        banlist_filename = "banlist.txt"
        csv_filename = "iv.csv"
        size_filename = "sizes.csv"
        self.contract_expiry_date = "25-Jul-2024"  # Specify the expiry date
        self.expiry_date = "25-Jul-2024" 
        self.sigma = self.read_csv_to_dict(csv_filename)
        self.sizes = self.read_sizes_to_dict(size_filename)
        stocks = self.extract_stocks_from_file(filename)
        ban_list = self.extract_stocks_from_file(banlist_filename) 

        self.stocks_to_monitor = list(filter(lambda x: x not in ban_list, stocks))
        self.underlying = self.create_symbol_dict(self.stocks_to_monitor)
        # self.populate_highs_and_lows(self.stocks_to_monitor)

        loop = asyncio.get_running_loop()
        
        task1 = loop.create_task(self.continuously_fetch_data())
        task2 = loop.create_task(self.monitor_and_execute_orders())

        asyncio.run(asyncio.gather(task1, task2))

        ist_now = datetime.now(timezone(timedelta(hours=5.5)))
        end_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=25)

        if ist_now > end_time:
            self.ib.disconnectedEvent -= self.reconnect
            self.ib.disconnect()  


if __name__ == "__main__":
    nse = Nse()
    asyncio.run(nse.main())
