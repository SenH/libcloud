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

import base64

from xml.etree.ElementTree import Element, SubElement

from libcloud.utils.py3 import PY3
from libcloud.utils.py3 import httplib
from libcloud.utils.py3 import b
from libcloud.utils.py3 import tostring

from libcloud.utils.xml import fixxpath, findtext
from libcloud.common.types import LibcloudError
from libcloud.common.azure import AzureConnection

from libcloud.queues.base import Message, Queue, QueueDriver
from libcloud.queues.types import QueueAlreadyExistsError
from libcloud.queues.types import QueueDoesNotExistError
from libcloud.queues.types import QueueIsNotEmptyError
from libcloud.queues.types import QueueIsEmptyError
from libcloud.queues.types import InvalidQueueNameError
from libcloud.queues.types import InvalidQueueParametersError
from libcloud.queues.types import MessageDoesNotExistError
from libcloud.queues.types import MessageTooLongError
from libcloud.queues.types import MessageError
from libcloud.queues.types import QueueError

RESPONSES_PER_REQUEST = 100


class AzureQueueConnection(AzureConnection):
    """
    Represents a single connection to the azure queue service
    """
    host = 'queue.core.windows.net'


class AzureQueuesDriver(QueueDriver):
    name = 'Azure Queues'
    website = 'http://queue.core.windows.net'
    connectionCls = AzureQueueConnection

    def __init__(self, key, secret=None, secure=True, host=None, port=None,
                 **kwargs):

        # The hostname must be 'account.queue.core.windows.net'
        self.connectionCls.host = '%s.%s' % (key, self.connectionCls.host)

        # B64decode() this key and keep it, so that we don't have to do
        # so for every request. Minor performance improvement
        secret = base64.b64decode(b(secret))

        super(AzureQueuesDriver, self).__init__(key=key, secret=secret,
                                                secure=secure, host=host,
                                                port=port, **kwargs)

    def _to_xml(self, body):
        root = Element('QueueMessage')
        message = SubElement(root, 'MessageText')
        message.text = str(body)
        return tostring(root)

    def _xml_to_queue(self, node):
        name = node.findtext(fixxpath(xpath='Name'))
        metadata = node.find(fixxpath(xpath='Metadata'))

        extra = {}
        extra['url'] = node.findtext(fixxpath(xpath='Url'))
        extra['meta_data'] = {}

        for meta in metadata.getchildren():
            extra['meta_data'][meta.tag] = meta.text

        return Queue(name, extra, self)

    def _response_to_queue(self, name, response):
        headers = response.headers
        msg_count = int(headers.get('x-ms-approximate-messages-count', 0))

        extra = {}
        extra['meta_data'] = {}
        extra['msg_count'] = msg_count

        for key, value in headers.items():
            if key.startswith('x-ms-meta-'):
                key = key.split('x-ms-meta-')[1]
                extra['meta_data'][key] = value

        return Queue(name, extra, self)

    def _xml_to_message(self, queue, node):
        msg_id = node.findtext(fixxpath(xpath='MessageId'))
        body = node.findtext(fixxpath(xpath='MessageText'))
        views = int(node.findtext(fixxpath(xpath='DequeueCount')))

        extra = {
            'receipt': node.findtext(fixxpath(xpath='PopReceipt')),
            'sent_at': node.findtext(fixxpath(xpath='InsertionTime')),
            'expires_at': node.findtext(fixxpath(xpath='ExpirationTime')),
            'next_visible_at': node.findtext(fixxpath(xpath='TimeNextVisible'))
        }

        return Message(msg_id, body, queue, views=views, extra=extra)

    def _response_to_messages(self, queue, response):
        body = response.parse_body()
        messages = body.findall(fixxpath(xpath='QueueMessage'))

        return [self._xml_to_message(queue, message) for message in messages]

    def _get_queue_path(self, queue_name):
        return '/%s' % (queue_name)

    def _get_messages_path(self, queue):
        return '/%s/messages' % (queue.name)

    def _get_message_path(self, message):
        return '/%s/messages/%s' % (message.queue.name, message.msg_id)

    def _update_metadata(self, headers, meta_data):
        """
        Update the given metadata in the headers

        @param headers: The headers dictionary to be updated
        @type headers: C{dict}

        @param meta_data: Metadata key value pairs
        @type meta_data: C{dict}
        """
        for key, value in list(meta_data.items()):
            key = 'x-ms-meta-%s' % (key)
            headers[key] = value

    def iterate_queues(self):
        """
        Return a generator of queues for the given account

        @return: A generator of Queue instances.
        @rtype: C{generator} of L{Queue}
        """

        params = {
            'comp': 'list',
            'maxresults': RESPONSES_PER_REQUEST,
            'include': 'metadata'
        }

        while True:
            response = self.connection.request('/', params)

            if response.status != httplib.OK:
                raise LibcloudError('Unexpected status code: %s' %
                                    (response.status), driver=self)

            body = response.parse_body()
            queues = body.find(fixxpath(xpath='Queues'))
            queues = queues.findall(fixxpath(xpath='Queue'))

            for queue in queues:
                yield self._xml_to_queue(queue)

            params['marker'] = body.findtext('NextMarker')

            if not params['marker']:
                break

    def create_queue(self, queue_name, ex_meta_data=None, **kwargs):
        """
        Create a new queue

        @param queue_name: The name of the queue which has to be created
        @type queue_name: C{str}

        @return: A newly created queue instance
        @rtype: L{Queue}
        """

        headers = {}
        meta_data = ex_meta_data if ex_meta_data else {}
        self._update_metadata(headers, meta_data)

        path = self._get_queue_path(queue_name)
        self.connection.set_context(context)

        response = self.connection.request(path, headers, method='PUT')

        if response.status == httplib.CREATED:
            return self._response_to_queue(queue_name, response)

        elif response.status == httplib.NO_CONTENT:
            raise QueueDeletedRecentlyError(
                              'This queue was recently deleted. '
                              'Wait for some time before creating it',
                              queue_name=queue_name, driver=self)

        elif response.status == httplib.CONFLICT:
            raise QueueAlreadyExistsError(
                value='Queue with this name already exists. The name must '
                      'be unique among all the containers in the system',
                queue_name=queue_name, driver=self)

        elif response.status == httplib.BAD_REQUEST:
            raise InvalidQueueNameError(value='Queue name contains invalid '
                                        'characters', queue_name=queue_name,
                                        driver=self)

        raise LibcloudError('Unexpected status code: %s' % (response.status),
                            driver=self)

    def get_queue(self, queue_name):
        """
        Get an existing queue

        @param queue_name: The name of the queue to get
        @type queue_name: C{str}

        @return: A queue instance
        @rtype: L{Queue}
        """

        params = {'comp': 'metadata'}
        path = self._get_queue_path(queue_name)

        response = self.connection.request(path, params, method='HEAD')

        if response.status == httplib.NOT_FOUND:
            raise QueueDoesNotExistError('Queue %s does not exist' %
                                         (queue_name), driver=self,
                                         queue_name=queue_name)

        elif response.status != httplib.OK:
            raise LibcloudError('Unexpected status code: %s' %
                                (response.status), driver=self)

        return self._response_to_queue(queue_name, response)

    def delete_queue(self, queue):
        """
        Delete an existing queue

        @param queue: The queue which has to be deleted
        @type queue: L{Queue}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """

        path = self._get_queue_path(queue.name)
        response = self.connection.request(path, method='DELETE')

        if response.status == httplib.NO_CONTENT:
            return True

        elif response.status == httplib.NOT_FOUND:
            raise QueueDoesNotExistError('Queue %s does not exist' %
                                         (queue_name), driver=self,
                                         queue_name=queue.name)

        return False

    def put_message(self, queue, body, delay=None, ex_ttl=None, **kwargs):
        """
        Put a message into the queue

        @param queue: The queue for putting the message into
        @type queue: L{Queue}

        @param body: The message body
        @type body: C{str}

        @param delay: The time for which a message must be invisible
        @type delay: C{int}

        @param ex_ttl: The time period after which a message will expire
        @type ex_ttl: C{int}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """

        path = self._get_messages_path(queue)
        params = {}

        if delay:
            params['visibilitytimeout'] = delay

        if ex_ttl:
            params['ttl'] = ex_ttl

        data = self._to_xml(body)
        response = self.connection.request(path, params, data, method='POST')

        return response.status == httplib.CREATED

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

        @param timeout: The timeout for blocking while waiting for messages
        @type timeout: C{int}

        @return: A list of messages that were fetched
        @rtype: C{list}
        """

        path = self._get_messages_path(queue)
        params = {}

        if not hide_for:
            hide_for = 10

        params['numofmessages'] = count
        params['visibilitytimeout'] = hide_for

        response = self.connection.request(path, params)

        if response.status != httplib.OK:
            raise LibcloudError('Unexpected status code: %s' %
                                (response.status), driver=self)

        return self._response_to_messages(queue, response)

    def delete_message(self, message, **kwargs):
        """
        Delete a message from a queue

        @param message: The message which is to be deleted
        @type message: L{Message}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """

        receipt = message.extra.get('receipt', None)

        if receipt is None:
            raise MessageError('Cannot delete read-only message',
                               message_id=message.msg_id)

        params = {'popreceipt': receipt}

        path = self._get_message_path(message)
        response = self.connection.request(path, params, method='DELETE')

        if response.status == httplib.NO_CONTENT:
            return True
        elif response.status == httplib.NOT_FOUND:
            raise MessageDoesNotExistError('Message s does not exist',
                                           driver=self,
                                           queue_name=message.queue.name)

        return False

    def update_message_delay(self, message, delay=None, **kwargs):
        """
        Update a message delay

        @param message: The message which has to be updated
        @type queue: L{Message}

        @param delay: The time for which a message must be invisible
        @type delay: C{int}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """
        return self.ex_update_message(message, None, delay, **kwargs)

    def ex_update_message(self, message, body, delay=None, **kwargs):
        """
        Update a message content or meta-data

        @param message: The message which has to be updated
        @type queue: L{Message}

        @param body: The new body for the message
        @type body: C{str}

        @param delay: The time for which a message must be invisible
        @type delay: C{int}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """

        receipt = message.extra.get('receipt', None)

        if receipt is None:
            raise MessageError('Cannot update read-only message',
                               message_id=message.msg_id)

        if not delay:
            delay = 10

        params = {}
        params['popreceipt'] = receipt
        params['visibilitytimeout'] = delay

        path = self._get_message_path(message)
        data = self._to_xml(body)
        response = self.connection.request(path, params, data, method='PUT')

        if response.status == httplib.NO_CONTENT:
            return True
        elif response.status == httplib.NOT_FOUND:
            raise MessageDoesNotExistError('Message s does not exist',
                                           driver=self,
                                           queue_name=message.queue.name)

        return False

    def ex_clear_queue(self, queue):
        """
        Clear all messages from the queue

        @param queue: The queue which has to be cleared
        @type queue: L{Queue}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """

        path = self._get_messages_path(queue)
        response = self.connection.request(path, method='DELETE')

        if response.status == httplib.NO_CONTENT:
            return True
        elif response.status == httplib.NOT_FOUND:
            raise QueueDoesNotExistError('Queue %s does not exist' %
                                         (queue_name), driver=self,
                                         queue_name=queue_name)

        return False

    def ex_peek(self, queue, count=1, **kwargs):
        """
        Get multiple messages from the queue without altering their visibility

        @param queue: The queue for getting the messages from
        @type queue: L{Queue}

        @param count: The maximum number of messages to peek
        @type count: C{int}

        @return: A list of messages that were peeked into
        @rtype: C{list}
        """

        params = {
            'numofmessages': count,
            'peekonly': 'true'
        }

        path = self._get_messages_path(queue)
        response = self.connection.request(path, params)

        if response.status != httplib.OK:
            raise LibcloudError('Unexpected status code: %s' %
                                (response.status), driver=self)

        return self._response_to_messages(queue, response)
