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
Provides base classes for working with queues
"""

# Backward compatibility for Python 2.5
from __future__ import with_statement

import os.path
import hashlib

from libcloud.utils.py3 import httplib
from libcloud.utils.py3 import b

from libcloud.common.types import LibcloudError
from libcloud.common.base import ConnectionUserAndKey, BaseDriver


class Message(object):
    """
    Represents a message (BLOB).
    """

    def __init__(self, msg_id, body, queue, views=None, extra=None):
        """
        @param msg_id: Message id
        @type  msg_id: C{str}

        @param body: The message body
        @type  body: C{str}

        @param size: Message size in bytes.
        @type  size: C{int}

        @param queue: Message queue.
        @type  queue: L{Queue}

        @param views: The number of times this object has been viewed
        @type  views: C{int}

        @param extra: Extra attributes.
        @type  extra: C{dict}
        """

        self.msg_id = msg_id
        self.body = body
        self.views = views
        self.queue = queue
        self.extra = extra or {}

    def get_cdn_url(self):
        return self.queue.driver.get_object_cdn_url(obj=self)

    def delete(self):
        return self.queue.driver.delete_message(self)

    def update_delay(self, delay=None, **kwargs):
        return self.queue.driver.update_message_delay(self, delay, **kwargs)

    def __repr__(self):
        return ('<Message: id=%s, views=%d, provider=%s ...>' %
                (self.msg_id, self.views, self.queue.driver.name))


class Queue(object):
    """
    Represents a queue which can hold messages
    """

    def __init__(self, name, extra, driver):
        """
        @param name: Queue name (must be unique).
        @type name: C{str}

        @param extra: Extra attributes.
        @type extra: C{dict}

        @param driver: QueueDriver instance.
        @type driver: L{QueueDriver}
        """

        self.name = name
        self.extra = extra or {}
        self.driver = driver

    def get_metadata(self):
        return self.driver.get_metadata(queue=self)

    def set_metadata(self, metadata):
        return self.driver.set_metadata(queue=self, metadata=metadata)

    def put_message(self, body, delay=None, **kwargs):
        return self.driver.put_message(self, body, delay, **kwargs)

    def put_messages(self, messages, delay=None, **kwargs):
        return self.driver.put_messages(self, messages, delay, **kwargs)

    def get_messages(self, count=1, timeout=None, **kwargs):
        return self.driver.get_messages(queue=self, count=count,
                                        timeout=timeout, **kwargs)

    def delete_message(self, message):
        return self.driver.delete_message(message)

    def update_message_delay(self, message, delay=None, **kwargs):
        return self.driver.update_message_delay(message, delay, **kwargs)

    def get_cdn_url(self):
        return self.driver.get_container_cdn_url(queue=self)

    def enable(self, **kwargs):
        return self.driver.enable_queue(queue=self, **kwargs)

    def disable(self, **kwargs):
        return self.driver.disable_queue(queue=self, **kwargs)

    def delete_message(self, message):
        return self.driver.delete_message(message)

    def delete(self):
        return self.driver.delete_queue(self)

    def __repr__(self):
        return ('<Queue: name=%s, provider=%s>'
                % (self.name, self.driver.name))


class QueueDriver(BaseDriver):
    """
    A base QueueDriver to derive from.
    """

    connectionCls = ConnectionUserAndKey
    name = None

    def __init__(self, key, secret=None, secure=True, host=None, port=None,
                 **kwargs):
        super(QueueDriver, self).__init__(key=key, secret=secret,
                                          secure=secure, host=host,
                                          port=port, **kwargs)

    def iterate_queues(self):
        """
        Return a generator of queues for the given account

        @return: A generator of Queue instances.
        @rtype: C{generator} of L{Queue}
        """
        raise NotImplementedError(
            'iterate_queues not implemented for this driver')

    def list_queues(self):
        """
        Return a list of queues.

        @return: A list of Queue instances.
        @rtype: C{list} of L{Queue}
        """
        return list(self.iterate_queues())

    def create_queue(self, queue_name, **kwargs):
        """
        Create a new queue

        @param queue_name: The name of the queue which has to be created
        @type queue_name: C{str}

        @return: A newly created queue instance
        @rtype: L{Queue}
        """
        raise NotImplementedError(
            'create_queue not implemented for this driver')

    def get_queue(self, queue_name):
        """
        Get an existing queue

        @param queue_name: The name of the queue to get
        @type queue_name: C{str}

        @return: A queue instance
        @rtype: L{Queue}
        """
        raise NotImplementedError(
            'get_queue not implemented for this driver')

    def delete_queue(self, queue):
        """
        Delete an existing queue

        @param queue: The queue which has to be deleted
        @type queue: L{Queue}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """
        raise NotImplementedError(
            'delete_queue not implemented for this driver')

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
        raise NotImplementedError(
            'put_message not implemented for this driver')

    def put_messages(self, queue, messages, delay=None, **kwargs):
        """
        Put multiple messages into the queue

        @param queue: The queue for putting the message into
        @type queue: L{Queue}

        @param messages: A list/iterator of strings
        @type body: C{list}

        @param delay: The message will be available after this timeout
        @type delay: C{int}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """

        # This is just a hack for implementations which do not have a
        # batch send mode
        for body in messages:
            if not self.put_message(queue, body, delay, **kwargs):
                return False
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
        raise NotImplementedError(
            'get_messages not implemented for this driver')

    def delete_message(self, message):
        """
        Delete a message from a queue

        @param message: The message which is to be deleted
        @type message: L{Message}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """
        raise NotImplementedError(
            'delete_message not implemented for this driver')

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
        raise NotImplementedError(
            'update_message_delay not implemented for this driver')
