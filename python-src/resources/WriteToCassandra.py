import storm
from CassandraHandler.DataWrite import WriteData as wd
from collections import Counter


class WriteToCassandra(storm.BasicBolt):
    # Initialize this instance
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self.writer = wd()

    def process(self, tup):
        message = str(tup.values[0])
        self.writer.write_data('cenote', message)


# Start the bolt when it's invoked
WriteToCassandra().run()
