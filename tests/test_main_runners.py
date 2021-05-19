import runpy
import sys

def test_to_profile():
    runpy.run_module('to_profile', run_name='__main__')

def test_home_economics():
    runpy.run_module('home_economics', run_name='__main__')

def test_simple_example():
    runpy.run_module('simple_example', run_name='__main__')

def test_simple_example():
    runpy.run_module('nesting_engines', run_name='__main__')