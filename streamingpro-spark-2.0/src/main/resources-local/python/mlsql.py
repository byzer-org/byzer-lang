# pylint: disable=protected-access
from __future__ import absolute_import, division, print_function
import os
import sys
import gc
import uuid
import time
import io
import multiprocessing
from kafka import KafkaConsumer
import msg_queue

if sys.version < '3':
    import cPickle as pickle
else:
    import pickle

    unicode = str


def dump(value, f):
    try:
        pickle.dump(value, f, 2)
    except pickle.PickleError:
        raise
    except Exception as e:
        msg = "Could not serialize broadcast: %s" \
              % (e.message)
        sys.stderr
        raise pickle.PicklingError(msg)
    f.close()
    return f.name


def load(path):
    with open(path, 'rb', 1 << 20) as f:
        # pickle.load() may create lots of objects, disable GC
        # temporary for better performance
        gc.disable()
        try:
            return pickle.load(f)
        finally:
            gc.enable()


filename = os.path.join(os.getcwd(), "python_temp.pickle")
_params = load("{}".format(filename))
print("params from parent: {}".format(_params))

if "kafkaParam" in _params:
    kafka_param = _params["kafkaParam"]

if "fitParam" in _params:
    fit_param = _params["fitParam"]

if "internalSystemParam" in _params:
    internal_system_param = _params["internalSystemParam"]

if "systemParam" in _params:
    systemParam = _params["systemParam"]


def read_data():
    # Update params
    # os.environ.get('pickleFile')
    authkey = uuid.uuid4().bytes
    mgr = msg_queue.start(authkey=authkey, queue_max_size=10, queues=['input'])

    def from_kafka(args, mgr):
        consumer = KafkaConsumer(kafka_param["topic"],
                                 group_id=kafka_param["group_id"],
                                 bootstrap_servers=kafka_param["bootstrap.servers"],
                                 auto_offset_reset="earliest",
                                 enable_auto_commit=False
                                 )

        max_records = args["max_records"]
        no_message_count = 0
        no_message_time = 5
        try:
            stop_count = 0
            fail_msg_count = 0
            while True:
                messages = consumer.poll(timeout_ms=1000, max_records=max_records)
                queue = mgr.get_queue("input")
                group_msgs_count = 0
                group_msgs = []
                for tp, records in messages.items():
                    for record in records:
                        try:
                            with io.BytesIO(record.value) as f:
                                msg_value = pickle.load(f)
                            if msg_value == "_stop_":
                                stop_count += 1
                            else:
                                group_msgs.append(msg_value)
                                group_msgs_count += 1
                        except:
                            fail_msg_count += 0
                            print("unpickle from kafka fail")
                            sys.stdout.flush()
                            pass
                if len(group_msgs) > 0:
                    no_message_count = 0
                    queue.put(group_msgs, block=True)

                if len(group_msgs) == 0 and no_message_count < 10:
                    time.sleep(no_message_time)
                    no_message_count += 1

                if (stop_count >= internal_system_param["stopFlagNum"] and group_msgs_count == 0) or (
                                no_message_count >= 10 and group_msgs_count == 0):
                    queue.put(["_stop_"], block=True)
                    print(
                        "no message from kafka, send _stop_ message. no_message_count={},stop_count={},stopFlagNum={}".format(
                            no_message_count, stop_count, internal_system_param["stopFlagNum"]))
                    sys.stdout.flush()
                    break
        finally:
            consumer.close()

    def _read_data(max_records=64, consume_threads=1, print_consume_time=False):

        def asyn_produce(consume_threads=1):
            print("asyn_produce start consuming")
            x = 0
            while x < consume_threads:
                x += 1
                process = multiprocessing.Process(target=from_kafka, args=({"max_records": max_records}, mgr))
                process.start()

        def sync_produce(consume_threads=1):
            import threading
            x = 0
            while x < consume_threads:
                x += 1
                print("sync_produce start consuming")
                threading.Thread(target=from_kafka, args=({"max_records": max_records}, mgr)).start()

        if "useThread" in systemParam:
            sync_produce(consume_threads=consume_threads)
        else:
            asyn_produce(consume_threads=consume_threads)

        print("start consuming from queue")
        queue = mgr.get_queue("input")

        def now_time():
            return int(round(time.time() * 1000))

        leave_msg_group = []
        while True:
            msg_group = []
            count = 0
            should_break = False

            if print_consume_time:
                start_time = now_time()
            wait_count = 0
            while count < max_records:
                if queue.empty():
                    wait_count += 1
                items = queue.get(block=True)
                if items[-1] == "_stop_":
                    should_break = True
                    break
                items = items + leave_msg_group
                leave_msg_group = []
                items_size = len(items)

                if items_size == max_records:
                    msg_group = items
                    break
                if items_size > max_records:
                    msg_group = items[0:max_records]
                    leave_msg_group = items[max_records:items_size]
                    break
                if items_size < max_records:
                    leave_msg_group = leave_msg_group + items
                count += 1

            if len(leave_msg_group) > 0:
                msg_group = leave_msg_group

            if wait_count > 1:
                print("queue get blocked count:{} when batch size is:{}".format(wait_count, max_records))
            if print_consume_time:
                ms = now_time() - start_time
                print("queue fetch {} consume:{}".format(max_records, ms))
            sys.stdout.flush()
            yield msg_group
            if should_break:
                print("_stop_ msg received, All data consumed.")
                break
        queue.task_done()

    return _read_data


def params():
    return _params
