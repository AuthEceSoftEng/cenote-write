import os
import sys

import storm

os.chdir("./CockroachHandler")
from DataWrite import DataWrite


class WriteToCassandra(storm.BasicBolt):
    # Initialize this instance

    def __init__(self):
        self._conf = None
        self._context = None
        self.writer = None

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self.writer = DataWrite()

    def process(self, tup):
        message = str(tup.values[0])
        try:
            res = self.writer.write_data(message)
            if res['response'] != 201:
                raise Exception(res)
        except Exception as e:
            with open("cenote-error.log", "a+") as f:
                f.write("Error: " + str(e) + " Message: " + message + "\n")


# Start the bolt when it's invoked
WriteToCassandra().run()
