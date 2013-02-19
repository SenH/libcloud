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

from libcloud.utils.py3 import urlparse
from libcloud.utils.xml import fixxpath, findtext
from libcloud.common.aws import AWSGenericResponse
from libcloud.common.aws import SignedAWSConnectionV4

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


API_VERSION = '2012-11-05'
NAMESPACE = 'http://queue.amazonaws.com/doc/%s/' % (API_VERSION)


class SQSResponse(AWSGenericResponse):
    namespace = NAMESPACE
    xpath = 'Error'

    exceptions = {
        'AWS.SimpleQueueService.QueueDeletedRecently':
                                        QueueDeletedRecentlyError,
        'QueueAlreadyExists': QueueAlreadyExistsError,
        'AWS.SimpleQueueService.NonExistentQueue': QueueDoesNotExistError,
        'InvalidMessageContents': InvalidMessageContentError,
        'MessageTooLong': MessageTooLongError,
        'ReadCountOutOfRange': InvalidQueueParametersError,
    }


class SQSConnection(SignedAWSConnectionV4):
    responseCls = SQSResponse
    service = 'sqs'


class SQSQueueDriver(QueueDriver):
    name = 'Amazon SQS (standard)'
    website = 'http://aws.amazon.com/sqs/'
    connectionCls = SQSConnection
    namespace = NAMESPACE

    def _make_queue(self, url, name=None):
        url = urlparse.urlparse(url)
        extra = {'url': url}

        if name is None:
            name = url.path.split('/')[-1]

        return Queue(name, extra, self)

    def _xml_to_queue(self, name, node):
        url = node.findtext(fixxpath(xpath='QueueUrl',
                                     namespace=self.namespace))
        return self._make_queue(url, name)

    def _findtext(self, node, text):
        return node.findtext(fixxpath(xpath=text, namespace=self.namespace))

    def _find(self, node, text):
        return node.find(fixxpath(xpath=text, namespace=self.namespace))

    def _findall(self, node, text):
        return node.findall(fixxpath(xpath=text, namespace=self.namespace))

    def _xml_to_message(self, queue, node):
        msg_id = self._findtext(node, 'MessageId')
        body = self._findtext(node, 'Body')

        extra = {
            'receipt': self._findtext(node, 'ReceiptHandle')
        }

        for attr in self._findall(node, 'Attribute'):
            name = self._findtext(attr, 'Name')
            value = self._findtext(attr, 'Value')

            if name == 'SenderId':
                extra['sender'] = value
            elif name == 'SentTimestamp':
                extra['sent_at'] = value
            elif name == 'ApproximateReceiveCount':
                views = int(value)
            elif name == 'ApproximateFirstReceiveTimestamp':
                extra['first_seen_at'] = value

        return Message(msg_id, body, queue, views=views, extra=extra)

    def _update_attributes(self, params, attributes):
        """
        Update the given attributes in the headers

        @param params: The params dictionary to be updated
        @type params: C{dict}

        @param attributes: Attribute key value pairs
        @type attributes: C{dict}
        """
        count = 1
        for key, value in attributes.items():
            params['Attribute.%d.Name' % (count)] = key
            params['Attribute.%d.Value' % (count)] = value
            count += 1

    def iterate_queues(self):
        """
        Return a generator of queues for the given account

        @return: A generator of Queue instances.
        @rtype: C{generator} of L{Queue}
        """

        params = {'Action': 'ListQueues'}
        response = self.connection.request('/', params)
        body = response.parse_body()
        queues = self._find(body, 'ListQueuesResult')

        for queue in self._findall(queues, 'QueueUrl'):
            url = queue.text
            yield self._make_queue(queue.text)

    def create_queue(self, queue_name, ex_attribs=None):
        """
        Create a new queue

        @param queue_name: The name of the queue which has to be created
        @type queue_name: C{str}

        @param ex_attribs: The attributes for creating the queue
        @type ex_attribs: C{dict}

        @return: A newly created queue instance
        @rtype: L{Queue}
        """

        params = {
            'Action': 'CreateQueue',
            'QueueName': queue_name
        }

        attributes = ex_attribs if ex_attribs else {}
        self._update_attributes(params, attributes)

        self.connection.set_context({'queue_name': queue_name})
        response = self.connection.request('/', params)

        body = response.parse_body()
        node = self._find(body, 'CreateQueueResult')

        return self._xml_to_queue(queue_name, node)

    def get_queue(self, queue_name):
        """
        Get an existing queue

        @param queue_name: The name of the queue to get
        @type queue_name: C{str}

        @return: A queue instance
        @rtype: L{Queue}
        """

        params = {
            'Action': 'GetQueueUrl',
            'QueueName': queue_name
        }

        self.connection.set_context({'queue_name': queue_name})
        response = self.connection.request('/', params)

        body = response.parse_body()
        node = self._find(body, 'GetQueueUrlResult')

        return self._xml_to_queue(queue_name, node)

    def delete_queue(self, queue):
        """
        Delete an existing queue

        @param queue: The queue which has to be deleted
        @type queue: L{Queue}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """

        params = {
            'Action': 'DeleteQueue',
            'QueueName': queue.name
        }

        url = queue.extra['url']
        self.connection.set_context({'queue_name': queue.name})
        response = self.connection.request(url.path, params)

        return True

    def put_message(self, queue, body, delay=None, **kwargs):
        """
        Put a message into the queue

        @param queue: The queue for putting the message into
        @type queue: L{Queue}

        @param body: The message body
        @type body: C{str}

        @param delay: The time for which a message must be invisible
        @type delay: C{int}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """

        params = {
            'Action': 'SendMessage',
            'MessageBody': str(body),
        }

        if delay:
            params['DelaySeconds'] = delay

        url = queue.extra['url']
        self.connection.set_context({'queue_name': queue.name})
        self.connection.request(url.path, params)

        return True

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

        params = {
            'Action': 'SendMessageBatch',
        }

        count = 1

        for body in messages:
            prefix = 'SendMessageBatchRequestEntry.%d' % (count)
            params['%s.Id' % (prefix)] = 'msg_%d' % (count)
            params['%s.MessageBody' % (prefix)] = str(body)

            if delay:
                params['%s.DelaySeconds' % (prefix)] = delay

            count += 1

        url = queue.extra['url']
        self.connection.set_context({'queue_name': queue.name})
        self.connection.request(url.path, params)

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

        @param timeout: The timeout for blocking while waiting for messages
        @type timeout: C{int}

        @return: A list of messages that were fetched
        @rtype: C{list}
        """

        params = {
            'Action': 'ReceiveMessage',
            'MaxNumberOfMessages': count,
            'AttributeName': 'All'
        }

        if hide_for:
            params['VisibilityTimeout'] = hide_for

        url = queue.extra['url']
        self.connection.set_context({'queue_name': queue.name})
        response = self.connection.request(url.path, params)

        body = response.parse_body()
        node = self._find(body, 'ReceiveMessageResult')
        messages = self._findall(node, 'Message')

        return [self._xml_to_message(queue, msg) for msg in messages]

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
            raise MessageError('Invalid message', message_id=message.msg_id)

        params = {
            'Action': 'DeleteMessage',
            'ReceiptHandle': receipt
        }

        url = message.queue.extra['url']
        self.connection.set_context({'message_id': message.msg_id})
        response = self.connection.request(url.path, params)

        return True

    def update_message_delay(self, message, delay=None, **kwargs):
        """
        Update a message's delay

        @param message: The message which has to be updated
        @type queue: L{Message}

        @param delay: The time for which a message must be invisible
        @type delay: C{int}

        @return: True or False to indicate the status of the operation
        @rtype: C{bool}
        """

        receipt = message.extra.get('receipt', None)

        if pop_receipt is None:
            raise MessageError('Invalid message', message_id=message.msg_id)

        if not delay:
            delay = 10

        params = {
            'Action': 'ChangeMessageVisibility',
            'ReceiptHandle': receipt,
            'VisibilityTimeout': delay
        }

        url = message.queue.extra['url']
        self.connection.set_context({'message_id': message.msg_id})
        response = self.connection.request(url.path, params)

        return True


class SQSUSWestConnection(SQSConnection):
    region = 'us-west-1'


class SQSUSWestQueueDriver(SQSQueueDriver):
    name = 'Amazon SQS (us-west-1)'
    connectionCls = SQSUSWestConnection


class SQSUSWestOregonConnection(SQSConnection):
    region = 'us-west-2'


class SQSUSWestOregonQueueDriver(SQSQueueDriver):
    name = 'Amazon SQS (us-west-2)'
    connectionCls = SQSUSWestOregonConnection


class SQSEUWestConnection(SQSConnection):
    region = 'eu-west-1'


class SQSEUWestQueueDriver(SQSQueueDriver):
    name = 'Amazon SQS (eu-west-1)'
    connectionCls = SQSEUWestConnection


class SQSAPSEConnection(SQSConnection):
    region = 'ap-southeast-1'


class SQSAPSEQueueDriver(SQSQueueDriver):
    name = 'Amazon SQS (ap-southeast-1)'
    connectionCls = SQSAPSEConnection


class SQSAPNEConnection(SQSConnection):
    region = 'ap-northeast-1'


class SQSAPNEQueueDriver(SQSQueueDriver):
    name = 'Amazon SQS (ap-northeast-1)'
    connectionCls = SQSAPNEConnection
