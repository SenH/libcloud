# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Dummy driver for queues
"""

# Backward compatibility for Python 2.5
from __future__ import with_statement

import time

from collections import OrderedDict, namedtuple
from libcloud.queues.base import Message, Queue, QueueDriver
from libcloud.queues.types import QueueAlreadyExistsError
from libcloud.queues.types import QueueDoesNotExistError
from libcloud.queues.types import QueueIsNotEmptyError
from libcloud.queues.types import QueueIsEmptyError
from libcloud.queues.types import QueueDeletedRecentlyError
from libcloud.queues.types import InvalidQueueNameError
from libcloud.queues.types import InvalidMessageContentError
from libcloud.queues.types import InvalidQueueParametersError
from libcloud.queues.types import MessageTooLongError
from libcloud.queues.types import MessageDoesNotExistError
from libcloud.queues.types import MessageError
from libcloud.queues.types import QueueError

DummyMessage = namedtuple('DummyMessage', ('id', 'time', 'seen', 'body'))


class DummyQueueDriver(QueueDriver):
    """
    A fake QueueDriver which always returns a success
    """

    name = 'Dummy Queue Driver'
    website = 'http://example.com'

    def __init__(self, *args, **kwargs):
        super(DummyQueueDriver, self).__init__(*args, **kwargs)
        self._queues = OrderedDict()

    def iterate_queues(self):
        """
        Return a generator of queues for the given account

        @return: A generator of Queue instances.
        @rtype: C{generator} of L{Queue}
        """
        for q in self._queues.values():
            yield q

    def create_queue(self, queue_name, **kwargs):
        """
        Create a new queue

        @param queue_name: The name of the queue which has to be created
        @type queue_name: C{str}

        @return: A newly created queue instance
        @rtype: L{Queue}
        """

        if queue_name in self._queues:
            raise QueueAlreadyExistsError('The queue already exists',
                                          queue_name=queue_name, driver=self)

        extra = {'q': OrderedDict()}
        queue = Queue(queue_name, extra, driver=self)
        self._queues[queue_name] = queue

        return queue

    def get_queue(self, queue_name):
        """
        Get an existing queue

        @param queue_name: The name of the queue to get
        @type queue_name: C{str}

        @return: A queue instance
        @rtype: L{Queue}
        """
        if queue_name not in self._queues:
            raise QueueDoesNotExistError('The queue does not exist',
                                          queue_name=queue_name, driver=self)

        return self._queues[queue_name]

    def delete_queue(self, queue):
        """
        Delete an existing queue

        @param queue: The queue which has to be deleted
        @type queue: L{Queue}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """
        if queue.name not in self._queues:
            raise QueueDoesNotExistError('The queue does not exist',
                                          queue_name=queue.name, driver=self)

        q = self._queues[queue.name]
        if q.extra['q']:
            raise QueueIsNotEmptyError('The queue is not empty',
                                       queue_name=queue.name, driver=self)

        self._queues.pop(queue.name)
        return True

    def put_message(self, queue, body, delay=None, **kwargs):
        """
        Put a message into the queue

        @param queue: The queue for putting the message into
        @type queue: L{Queue}

        @param body: The message body
        @type body: C{str}

        @param delay: The message will be available after this timeout
        @type delay: C{int}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """
        see_at = time.time()
        msg_id = str(see_at)
        if delay:
            see_at += delay

        q = queue.extra['q']
        msg = DummyMessage(id=msg_id, time=see_at, seen=0, body=body)
        q[msg_id] = msg

        return True

    def get_messages(self, queue, count=1, hide_for=None, **kwargs):
        """
        Get multiple messages from the queue

        @param queue: The queue for getting the messages from
        @type queue: L{Queue}

        @param count: The maximum number of messages to fetch
        @type count: C{int}

        @param hide_for: The time for which the message is hidden
                         after it is read
        @type hide_for: C{int}

        @return: A list of messages that were fetched
        @rtype: C{list}
        """
        q = queue.extra['q']
        msgs = []
        now = time.time()

        if not hide_for:
            hide_for = 10

        for msg in q.values():
            if count <= 0:
                break

            if msg.time > now:
                continue

            msg = DummyMessage(msg.id, msg.time+hide_for, msg.seen+1, msg.body)
            msgs.append(Message(msg.id, msg.body, queue, msg.seen))
            q[msg.id] = msg
            count -= 1

        return msgs

    def delete_message(self, message):
        """
        Delete a message from a queue

        @param message: The message which is to be deleted
        @type message: L{Message}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """
        q = message.queue.extra['q']

        if message.msg_id not in q:
            raise MessageDoesNotExistError('This message does not exist',
                                           message_id=message.msg_id,
                                           driver=self)

        q.pop(message.msg_id)

    def update_message_delay(self, message, delay=None, **kwargs):
        """
        Update a message content or meta-data

        @param message: The message which has to be updated
        @type queue: L{Message}

        @param delay: The message will be available after this timeout
        @type delay: C{int}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """
        q = message.queue.extra['q']
        msg = q[message.msg_id]
        msg = DummyMessage(message.msg_id, msg.time+delay, msg.seen, msg.body)
        q[message.msg_id] = msg
