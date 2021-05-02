import helpers
import deco
"""
t is a special parameter denoting index within a timeseries starting at t of 0

parameters with the same name as a function will automatically be connected.
parameters that do not have a matching function MUST be included as inputs during setup
"""

@deco.ignoreme
def basic(sa, sb):
    return helpers.mult(sa, sb)

def add_ts(t, tsa, tsb):
    return tsa[t] + tsb[t]

def count_forward(t, count_forward):
    if t == 0:
        return 0
    else:
        return count_forward[t-1] + 1


def count_backward(t, count_backward):
    if t > 5:
        return 5
    else:
        return count_backward[t+1] + 1


def add_ts_cb(t, count_forward, count_backward):
    return count_forward[t] + count_backward[t]


def sum_atc(t, sum_atc, add_ts_cb):
    if t == 0:
        return add_ts_cb[t]
    else:
        return sum_atc[t-1] + add_ts_cb[t]

def percent_atc(t, sum_atc, add_ts_cb):
    return add_ts_cb[t] / sum_atc[t]


