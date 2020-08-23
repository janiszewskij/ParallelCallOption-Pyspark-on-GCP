# -*- coding: utf-8 -*-

#import packages
import math
import datetime
import random
from random import gauss
from math import exp, sqrt
from operator import add

#Generate asset price paths using SDE
def generate_asset_price(S,v,r,T):
    return S * exp((r - 0.5 * v**2) * T + v * sqrt(T) * gauss(0,1.0))

#Calculate Call price
def call_payoff(S_T,K):
    return max(0.0,S_T-K)


#model inputs
S = 857.29 # underlying price
v = 0.2076 # vol of 20.76%
r = 0.0014 # rate of 0.14%
T = (datetime.date(2017,9,21) - datetime.date(2013,9,3)).days / 365.0
K = 860.
v = 0.2
r = 0.02

simulations = 1000
payoffs = []
S_T_list=[]
discount_factor = math.exp(-r * T)

# Approach 1: Monte Carlo loop 
for i in range(simulations):
    S_T = generate_asset_price(S,v,r,T)
    payoffs.append(
        call_payoff(S_T, K)
    )

#Calculate Monte Carlo results
price = discount_factor * (sum(payoffs) / float(simulations))
print('Price: %.4f' % price)

#Approach 2: Monte Carlo with PySpark parallel computing
#Def
def price_sim(seed):
    random.seed(seed)
    S_T = generate_asset_price(S,v,r,T)
    return call_payoff(S_T, K)

#Map/Reduce execustion
seeds = sc.parallelize([time.time() + i for i in xrange(simulations)])
results = seeds.map(price_sim)

sum = results.reduce(add)
print( (sum / simulations)*discount_factor)


v = 0.4
r = 0.02
print (sc.parallelize([time.time() + i for i in xrange(simulations)]).map(price_sim).reduce(add)/simulations)
