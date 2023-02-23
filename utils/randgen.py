"""
Create a generator class to produce random numbers
"""


import random


class ProduceChars:
    """Generator class to produce random numbers in a given range"""

    def __init__(self, start, end, limit):
        self.start = start
        self.end = end
        self.limit = limit

    def __iter__(self):
        counter = 1
        while counter <= self.limit:
            yield random.randint(self.start, self.end)
            counter += 1