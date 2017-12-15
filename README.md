# hazelcast-golang-client

Originally a fork of: https://github.com/hasancelik/hazelcast-go-client
and extended allowing interaction with Hazelcast Queues

No dependencies on other projects - core golang only

Developed and tested against: Hazelcast 3.9 http://docs.hazelcast.org/docs/rn/3.9.html

With the help of:
* https://github.com/hazelcast/hazelcast-client-protocol/raw/v1.2.0/docs/published/protocol/1.2.0/HazelcastOpenBinaryClientProtocol-1.2.0.pdf
* http://docs.hazelcast.org/docs/ClientProtocolImplementationGuide-Version1.0-Final.pdf
* A debugger and the java client source ;-)

#### What's missing, what needs improvement?

* There is a lot missing! This is a very narrow implementation allowing us to interact with Queues only.
    * Map, MultiMap, Topic, List, Set, Lock, Condition, ExecutorService, AtomicLong, AtomicReference, CountdownLatch, Semaphore, ReplicatedMap. MapReduce, TransactionalMap, TransactionalMultimap, TransactionalSet, TransactionalList, TransactionalQueue, Cache, XATransactional, Transactional, EnterpriseMap, RingBuffer and DurableExecutor are NOT currently supported! 
* Timeouts and Retry - currently not supported.  Need to add a common protocol retry mechanism
* Split response messages - messages split into multiples using the BEGIN/END flags are not supported.
    * Currently all requests are sent in a single message with BEGIN/END flag set.
    * All responses are assumed to be consumed in a single message read. 
* This is a 'Dummy' client not a 'Smart' client.  We'd need a higher level node connection manager to monitor cluster membership and build connections as required before we can call this a smart client.
* Error Handling - can always do with improvement!
