import runpy
import sys

def test_to_profile():
    runpy.run_module('to_profile', run_name='__main__')

def test_to_profile_profile():
    old = sys.argv
    sys.argv = [old[0], 'to_profile.py']
    runpy.run_module('cProfile', run_name='__main__')
    sys.argv = old

    