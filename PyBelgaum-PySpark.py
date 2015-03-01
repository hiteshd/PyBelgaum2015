# Copyright 2015 Hitesh Dharmdasani
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import random
from pyspark import SparkContext

# Create a SparkContext
# This is used to connect to the cluster and run operations
# "local" is the URL for standalone systems
# Use "spark://..." for more than 1 machine

sc = SparkContext("local")


"""
				Basic Examples
"""

# Define a random collection of numbers
data = random.sample(xrange(100),10)

data

# This is Spark Context
sc

# Passing the Python list to Spark 
distData = sc.parallelize(data)

distData

# Increment a given number
def mapFunc(x):
	return x + 1

# Check if number is even.
def filterFunc(x):
	if x % 2 == 0:
		return True
	else:
		return False

# Applying a Map operation
# Every element in distData is passed through `mapFunc`
increment_rdd = distData.map(mapFunc)
# OR
increment_rdd = distData.map(lambda x: x + 1)

# This is just a transformation. no computation is performed.
# Let us apply a action to see the results

increment_rdd.collect()

# Chaining of operations
# Apply a map and a filter in order
evenNumbers = distData.map(mapFunc).filter(filterFunc)
# OR
evenNumbers = distData.map(lambda x: x + 1 ).filter(lambda x: x % 2 == 0 )

# Observe the results
evenNumbers.collect()

# Using External libraries
# `math` is a CPython Module
import math

def isPrime(number):
	if number == 2:
		return True
	if number % 2 == 0 or number <= 1:
		return False
	squareRootOfNumber = int(math.sqrt(number)) + 1
	for divisor in range(3,squareRootOfNumber, 2):
		if number % divisor == 0:
			return False
	return True

# Finding Prime numbers
# Passing an arbitrary python function to RDD operations
primeNumbers = distData.filter(isPrime)

"""
				Using more complex data
"""

# Let us create a RDD from the dns.log file
dns_rdd_init = sc.textFile("data/dns.log")

# Removing the headers and commented lines
dns_rdd = dns_rdd_init.filter(lambda x: not x.startswith('#'))

# Extracting the field that contains the DNS query
queries = dns_rdd.map(lambda x: x.split('\t')[8])

# Caching the dataset in Memory. All but the 1st operation is going to run directly in memory
queries.cache()

queryCount = queries.count()

print "DNS Query Count", queryCount

# Loading the functional add operator
# as good as 
#		lambda x,y : x + y

from operator import add

# Extracting the least frequently queried domains in DNS requests
# 
lowFreqQueries = queries.map(lambda x: (x,1)).\
						reduceByKey(add).\
						sortBy((lambda x: x[1]),True)

# Display least 20 domains
# Some can be malicious
print "Least Queried DNS ", lowFreqQueries.collect()[:20]

# Lets find domsin names larger than 20 characters. These can be malicious domains created via scripts
lowFreqQueries.map(lambda x: (x[0],len(x[0]))).filter(lambda x: x[1] > 20).sortBy((lambda x: x[1]),False)

print "Domains larger than 20 characters ", lowFreqQueries.collect()[-20:]

"""
				Using more complex data - More queries
"""

# Lets create a tuple of (Domain, IPs)
# '-' indicates a NXDOMAIN
responses = dns_rdd.map(lambda x: ( x.split('\t')[8], x.split('\t')[20]) ).filter(lambda x: x[1] != '-')

# Finding a list of parked domains
# They are often parked if the bot-master has no use for them
parkedDomains = responses.filter(lambda x: '127.0.0.1' in x[1])

print "Probable Parked domains ", parkedDomains.collect()

# Creating Passive DNS

# We can run this as a streaming job
# All IPs are grouped with the domain as the Key
# Produces a (Key, [Value1, Value2, Value3,...]) structure

requestResponsePair = responses.groupByKey()

print requestResponsePair.collect()[:20]

# Save the results to a Cassandra Datastore that can power a frontend/API
# Comment below line
# requestResponsePair.saveToCassandra("passivedns", "records", SomeColumns("domain", "entires"))

dns_rdd.map(lambda x: (x.split('\t')[4],1)).reduceByKey(add).sortBy((lambda x: x[1]),False).join(dnsServer).sortBy(lambda x: x[1][0]).collect()


dnsServers = [("10.0.2.2", "Local"), 
("10.0.2.255", "Local"), 
("10.0.2.3", "Local"), 
("172.16.0.2", "Local"), 
("172.16.1.1", "Local"), 
("172.16.255.255", "Multicast"), 
("172.29.0.255", "Multicast"), 
("192.168.106.255", "Multicast"), 
("192.168.248.255", "Multicast"), 
("192.168.254.255", "Multicast"), 
("213.133.100.100", "Hetzner Standard"), 
("213.133.98.98", "Hetzner Standard"), 
("4.2.2.2", "Level 3 Communications"), 
("68.87.71.230", "Comcast"), 
("68.87.73.246", "Comcast"), 
("75.75.75.75", "Comcast"), 
("75.75.76.76", "Comcast"), 
("8.8.4.4", "Google"), 
("8.8.8.8", "Google")]

dnsServersRdd = sc.parallelize(dnsServers)

mostFreqDnsServersRdd = dns_rdd.\
			map(lambda x: (x.split('\t')[4],1)).\
			reduceByKey(add).\
			sortBy((lambda x: x[1]),False).\
			join(dnsServersRdd).\
			sortBy(lambda x: x[1][0])

mostFreqDnsServers = mostFreqDnsServersRdd.collect()

"""
				Reading from a textFile
"""

dnsServers = sc.textFile("data/providers.txt")

dnsServersRdd = dnsServers.map(lambda x: (x.split('-')[0],x.split('-')[1]))
