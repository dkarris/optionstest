'''
In this module functions for MOEX theoritical volatility calculation and
Black - Scholes pricing are defined.
'''
import math

def theoreticalVolatility(strike, asset_price, s, a, b, c, d, e, t):
    '''
    returns MOEX theoretical volatility calculated using moex formula and parameters
    '''
    # s = float(s)
    # a = float(a)
    # b = float(b)
    # c = float(c)
    # d = float(d)
    # e = float(e)
    # t = float(e)
    
    if a == 0:
        return 0
    x = math.log(strike/asset_price)*math.sqrt(t)
    y = x - s
    sigma = (a + b * (1- math.exp(-c*(y**2))) + d * math.atan(e*y)/e)/100
    return sigma

def phi(x):
    '''
    https://docs.python.org/dev/library/math.html#math.erf
    Cumulative distribution function for the standard normal distribution'
    used in option_priceBS
    '''
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def option_priceBS(asset_price, strike, volatility, time, opttype):
    '''
    returns Black Scholes price and some greeks for option with given parameters
    type = "call" or "put". All other returned with 0

    returns price, delta, gamma, theta, thvega
    '''
    if opttype != 'call' and opttype != 'put':
        return 0,0,0,0,0
    if volatility == 0:
        return 0,0,0,0,0
    # if time == 0:
    #      return 0,0,0,0,0
    strike = float(strike)
    asset_price = float(asset_price)
    d1 = (math.log(asset_price/strike) + (volatility**2)*time/2) / (volatility*math.sqrt(time))
    d2 = d1 - volatility*math.sqrt(time)
    option_price = asset_price*phi(d1) - strike*phi(d2)
    # delta calc
    # assume r = 0 : risk free interest rate
    #        q = 0 : obviously no dividends
    #              : using 365 days for theta
    r = 0
    q = 0
    days = 365
    delta_d1 = (math.log(asset_price/strike) + (r + (volatility**2)/2)*time)/volatility*math.sqrt(time)
    delta = phi(delta_d1)
    gamma = (math.exp(-1*(d1**2)/2)/math.sqrt(2*math.pi))/(asset_price*volatility*math.sqrt(time))*math.exp(-q*time)
    theta1 = -((asset_price*volatility*math.exp(-1*q*time))/(2*math.sqrt(time))*(math.exp(-1*(d1**2)/2)/math.sqrt(2*math.pi)))
    theta2 = r * strike * math.exp(-1*r*time)*phi(d2)
    theta3 = q * asset_price * math.exp(-1*q*time)*phi(d1)
    theta = (theta1 - theta2 + theta3)/days
    vega = asset_price*math.exp(-1*q*time)*math.sqrt(time)/math.sqrt(2*math.pi)*math.exp(-1*(d1**2)/2)/100
    if opttype == 'put': # if option is put then apply call/put parity for put calc
        option_price = option_price + strike - asset_price
        delta = delta - 1
    return option_price, delta, gamma, theta, vega
