import sys
import os
import shutil

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

if sys.version < '3':
    import cPickle as pickle
else:
    import pickle

    unicode = str


class StreamingProPythonTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.filename = os.path.join(os.getcwd(), "python_temp.pickle")
        kafka_params = {
            "kafkaParam": {"topic": "test_1525765646689", "bootstrap.servers": "127.0.0.1:9092", "group_id": "group1"}}
        pickle.dump(kafka_params, open(cls.filename, "wb"), 2)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.filename)


class KafkaReadTest(StreamingProPythonTestCase):
    def test_consume(self):
        import mlsql
        print(self.filename)
        rd = mlsql.read_data()
        for items in rd(max_records=100):
            print(str(len(items)))
