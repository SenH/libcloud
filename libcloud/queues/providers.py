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

from libcloud.utils.misc import get_driver as get_provider_driver
from libcloud.utils.misc import set_driver as set_provider_driver
from libcloud.queues.types import Provider

DRIVERS = {
    Provider.DUMMY:
        ('libcloud.queues.drivers.dummy', 'DummyQueueDriver'),
    Provider.SQS:
        ('libcloud.queues.drivers.sqs', 'SQSUSWestQueueDriver'),
    Provider.SQS_US_WEST:
        ('libcloud.queues.drivers.sqs', 'SQSUSWestQueueDriver'),
    Provider.SQS_US_WEST_OREGON:
        ('libcloud.queues.drivers.sqs', 'SQSUSWestOregonQueueDriver'),
    Provider.SQS_EU_WEST:
        ('libcloud.queues.drivers.sqs', 'SQSEUWestQueueDriver'),
    Provider.SQS_AP_SOUTH_EAST:
        ('libcloud.queues.drivers.sqs', 'SQSAPSEQueueDriver'),
    Provider.SQS_AP_NORTH_EAST:
        ('libcloud.queues.drivers.sqs', 'SQSAPNEQueueDriver'),
    Provider.AZURE_QUEUES:
        ('libcloud.queues.drivers.azure_queues', 'AzureQueuesDriver'),
}


def get_driver(provider):
    return get_provider_driver(DRIVERS, provider)


def set_driver(provider, module, klass):
    return set_provider_driver(DRIVERS, provider, module, klass)
