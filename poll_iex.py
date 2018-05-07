import requests
import json

from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

class IEX(object):
	def __init__(self, mongo_client):
		self.STOCK_URL = 'https://api.iextrading.com/1.0/stock/{}'
		self.FINANCIALS = '{}/financials'
		self.EARNINGS = '{}/earnings'
		self.thread_pool = None

		from multiprocessing import cpu_count
		self.cpu_count = cpu_count()
		#from constants import fin_colns, earn_colns
		#self.financials_column_names = fin_colns
		#self.earnings_column_names = earn_colns
		self.db = mongo_client['tickers']

	### PRIVATE METHODS ###
	def __poll_IEX(self, ticker, endpoint):
		endpoint = endpoint.format(ticker)
		return requests.get(self.STOCK_URL.format(endpoint)).json()

	def __parallel_poll(self, tickers, endpoint):
		self.thread_pool = ThreadPool(self.cpu_count)
		if type(tickers) != list:
			tickers = [tickers]

		part_endpoint = partial(self.__poll_IEX, endpoint=endpoint)
		results = self.thread_pool.map(part_endpoint, tickers)
		self.thread_pool.close()
		self.thread_pool.join()	
		return results	

	def __financials_to_db(self, quarterly_data):
		ticker_loc = self.db[quarterly_data['symbol']]
		mongo_dict = {'financials': quarterly_data['financials']}
		ticker_loc.insert_one(mongo_dict)

	def __earnings_to_db(self, quarterly_data):
		ticker_loc = self.db[quarterly_data['symbol']]
		mongo_dict = {'earnings': quarterly_data['earnings']}
		ticker_loc.insert_one(mongo_dict)

	### PUBLIC METHODS ###
	def update_all(self, tickers):
		financials_json = self.__parallel_poll(tickers, self.FINANCIALS)
		earnings_json = self.__parallel_poll(tickers, self.EARNINGS)

		for quarter in financials_json:
			self.__financials_to_db(quarter)
		for quarter in earnings_json:
			self.__earnings_to_db(quarter)

if __name__ == '__main__':
	from pymongo import MongoClient
	iex = IEX(MongoClient('localhost', 27017))
	tickers = ['aapl', 'nvda', 'amat', 'amd', 'x', 'tsla', 'snap']
	print(iex.update_all(tickers))

