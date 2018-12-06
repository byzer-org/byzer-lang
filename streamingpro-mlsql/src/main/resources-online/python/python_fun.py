import sys
import io
import pickle

from pyspark.ml.linalg import VectorUDT
from pyspark.serializers import CloudPickleSerializer, AutoBatchedSerializer, PickleSerializer, NoOpSerializer

if sys.version > '3':
    basestring = unicode = str
else:
    from itertools import imap as map, ifilter as filter


def wrap_function(func, profiler=None):
    def pickle_command(command):
        # the serialized command will be compressed by broadcast
        ser = CloudPickleSerializer()
        pickled_command = ser.dumps(command)
        return pickled_command

    ser = AutoBatchedSerializer(PickleSerializer())
    command = (func, profiler, NoOpSerializer(), ser)
    pickled_command = pickle_command(command)
    return bytearray(pickled_command)


def write_binary_file(path, func):
    with open(path, "wb") as f:
        f.write(wrap_function(func))


ser = AutoBatchedSerializer(PickleSerializer())


def udf(func):
    import mlsql
    p = mlsql.params()
    func_path = p["systemParam"]["funcPath"]
    write_binary_file(func_path, func)
