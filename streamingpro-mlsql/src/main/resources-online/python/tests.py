from __future__ import absolute_import, division, print_function
import sys
import os
import time
from kafka import KafkaProducer

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

try:
    xrange
except:
    xrange = range


class StreamingProPythonTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.filename = os.path.join(os.getcwd(), "python_temp.pickle")
        cls.kafka_host = "localhost:9092"
        millis = int(round(time.time() * 1000))
        cls.topic = "test_{}".format(millis)
        kafka_params = {
            "kafkaParam": {"topic": cls.topic, "bootstrap.servers": cls.kafka_host,
                           "group_id": "group-jj",
                           "debug": False},
            "systemParam": {},
            "internalSystemParam": {"stopFlagNum": 3},
            "fitParam": {"alg": "RandomForestClassifier", "debug": "True"}
        }
        pickle.dump(kafka_params, open(cls.filename, "wb"), 2)

        producer = KafkaProducer(bootstrap_servers=cls.kafka_host)
        for i in xrange(10):
            producer.send(cls.topic, pickle.dumps("{}", 2))
        producer.close()

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.filename)


class GenerateSKlearnModel(StreamingProPythonTestCase):
    def test_generateSkLearnModel(self):
        from sklearn import datasets
        iris = datasets.load_iris()
        from sklearn.naive_bayes import GaussianNB
        gnb = GaussianNB()
        gnb.fit(iris.data, iris.target)
        with open(os.path.join("./", "sklearn_model_iris.pickle"), "wb") as f:
            pickle.dump(gnb, f, protocol=2)


class KafkaReadTest(StreamingProPythonTestCase):
    def test_consume(self):
        import mlsql
        rd = mlsql.read_data()
        count = 0
        for items in rd(max_records=100):

            print(str(len(items)))
            if count == 0:
                assert len(items) == 10
            else:
                assert len(items) == 0
            count += 1


class KafkaReadTest2(StreamingProPythonTestCase):
    def test_sklearn_all_data(self):
        import mlsql
        mlsql.sklearn_all_data()
