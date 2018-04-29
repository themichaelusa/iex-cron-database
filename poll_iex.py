#import psycopg2
import requests
import json
import pandas

from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count

class IEX(object):
	def __init__(self):
		self.STOCK_URL = 'https://api.iextrading.com/1.0/stock/{}'
		self.FINANCIALS = '{}/financials'
		self.EARNINGS = '{}/earnings'
		self.thread_pool = None

	### PRIVATE METHODS ###
	def __poll_IEX(self, ticker, endpoint):
		endpoint = endpoint.format(ticker)
		return requests.get(self.STOCK_URL.format(endpoint)).json()

	def __parallel_poll(self, tickers, endpoint):
		self.thread_pool = ThreadPool(cpu_count())
		if type(tickers) != list:
			tickers = [tickers]

		part_endpoint = partial(self.__poll_IEX, endpoint=endpoint)
		results = self.thread_pool.map(part_endpoint, tickers)
		self.thread_pool.close()
		self.thread_pool.join()	
		return results	

	### PUBLIC METHODS ###
	def get_financials(self, tickers):
		financials_json = self.__parallel_poll(tickers, self.FINANCIALS)
		return financials_json

	def get_earnings(self, tickers):		
		earnings_json = self.__parallel_poll(tickers, self.EARNINGS)
		return earnings_json

if __name__ == '__main__':
	iex = IEX()
	tickers = ['aapl', 'nvda', 'amat', 'amd', 'x', 'tsla', 'snap']
	print(iex.get_financials(tickers), iex.get_earnings(tickers))

