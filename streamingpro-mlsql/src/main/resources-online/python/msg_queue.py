# Copyright 2017 Yahoo Inc.
# Licensed under the terms of the Apache 2.0 license.
# Please see LICENSE file in the project root for terms.

from __future__ import absolute_import
from __future__ import division
from __future__ import nested_scopes
from __future__ import print_function

from multiprocessing.managers import BaseManager
from multiprocessing import JoinableQueue, Queue


class MsgQueue(BaseManager): pass


mgr = None
qdict = {}
kdict = {}


def _get(key):
    return kdict[key]


def _set(key, value):
    kdict[key] = value


def start(authkey, queues, queue_max_size=2048, mode='local'):
    """
    Create a new multiprocess.Manager (or return existing one).
    """
    global mgr, qdict, kdict
    qdict.clear()
    kdict.clear()
    for q in queues:
        qdict[q] = JoinableQueue(queue_max_size)
    MsgQueue.register('get_queue', callable=lambda qname: qdict[qname])
    MsgQueue.register('get', callable=lambda key: _get(key))
    MsgQueue.register('set', callable=lambda key, value: _set(key, value))
    if mode == 'remote':
        mgr = MsgQueue(address=('', 0), authkey=authkey)
    else:
        mgr = MsgQueue(authkey=authkey)
    mgr.start()
    return mgr


def connect(address, authkey):
    """
    Connect to a multiprocess.Manager
    """
    MsgQueue.register('get_queue')
    MsgQueue.register('get')
    MsgQueue.register('set')
    m = MsgQueue(address, authkey=authkey)
    m.connect()
    return m
