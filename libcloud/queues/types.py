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

from libcloud.common.types import LibcloudError

__all__ = ['Provider',
           'QueueError',
           'MessageError',
           'QueueAlreadyExistsError',
           'QueueDoesNotExistError',
           'QueueIsNotEmptyError',
           'QueueIsEmptyError',
           'QueueDeletedRecentlyError',
           'InvalidQueueNameError',
           'MessageDoesNotExistError']


class Provider(object):
    """
    Defines for each of the supported providers

    @cvar DUMMY: Example provider
    @cvar SQS: Amazon SQS US
    """
    DUMMY = 'dummy'
    SQS = 'sqs'
    SQS_US_WEST = 'sqs_us_west'
    SQS_US_WEST_OREGON = 'sqs_use_west_oregon'
    SQS_EU_WEST = 'sqs_eu_west'
    SQS_AP_SOUTH_EAST = 'sqs_ap_south_east'
    SQS_AP_NORTH_EAST = 'sqs_ap_north_east'
    AZURE_QUEUES = 'azure_queues'


class QueueError(LibcloudError):
    error_type = 'QueueError'
    kwargs = ['queue_name']

    def __init__(self, value, driver, queue_name=None):
        self.queue_name = queue_name
        super(QueueError, self).__init__(value=value, driver=driver)

    def __str__(self):
        return ('<%s in %s, queue=%s, value=%s>' %
                (self.error_type, repr(self.driver),
                 self.queue_name, self.value))


class MessageError(LibcloudError):
    error_type = 'MessageError'
    kwargs = ['message_id']

    def __init__(self, value, driver, message_id=None):
        self.message_id = message_id
        super(MessageError, self).__init__(value=value, driver=driver)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '<%s in %s, value=%s, msg = %s>' % (self.error_type,
                                                   repr(self.driver),
                                                   self.value,
                                                   self.message_id)


class QueueAlreadyExistsError(QueueError):
    error_type = 'QueueAlreadyExistsError'


class QueueDoesNotExistError(QueueError):
    error_type = 'QueueDoesNotExistError'


class QueueIsNotEmptyError(QueueError):
    error_type = 'QueueIsNotEmptyError'


class QueueIsEmptyError(QueueError):
    error_type = 'QueueIsEmptyError'


class QueueDeletedRecentlyError(QueueError):
    error_type = 'QueueDeletedRecentlyError'


class InvalidQueueParametersError(QueueError):
    error_type = 'InvalidQueueParametersError'


class InvalidQueueNameError(QueueError):
    error_type = 'InvalidQueueNameError'


class InvalidMessageContentError(MessageError):
    error_type = 'InvalidMessageContentError'


class MessageTooLongError(MessageError):
    error_type = 'MessageTooLongError'


class MessageDoesNotExistError(MessageError):
    error_type = 'MessageDoesNotExistError'
