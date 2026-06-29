/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.NewPartitionReassignment
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer, RangeAssignor}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.ServerLogConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{MethodSource, ValueSource}

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.jdk.CollectionConverters._

class FetchFromFollowerIntegrationTest extends BaseFetchRequestTest {
  val numNodes = 2
  val numParts = 1

  val topic = "test-fetch-from-follower"
  val leaderBrokerId = 0
  val followerBrokerId = 1

  def overridingProps: Properties = {
    val props = new Properties
    props.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, numParts.toString)
    props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, numNodes.toString)
    props
  }

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(numNodes, enableControlledShutdown = false, enableFetchFromFollower = true)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  @Timeout(15)
  def testFollowerCompleteDelayedFetchesOnReplication(groupProtocol: String): Unit = {
    // Create a topic with 2 replicas where broker 0 is the leader and 1 is the follower.
    val admin = createAdminClient()
    val partitionLeaders = TestUtils.createTopicWithAdmin(
      admin,
      topic,
      brokers,
      controllerServers,
      replicaAssignment = Map(0 -> Seq(leaderBrokerId, followerBrokerId))
    )
    TestUtils.waitUntilLeaderIsKnown(brokers, new TopicPartition(topic, 0))
    assertTrue(partitionLeaders.values.forall(_ == leaderBrokerId))

    val version = ApiKeys.FETCH.latestVersion()
    val topicPartition = new TopicPartition(topic, 0)
    val offsetMap = Map(topicPartition -> 0L)

    // Set fetch.max.wait.ms to a value (20 seconds) greater than the timeout (15 seconds).
    // Send a fetch request before the record is replicated to ensure that the replication
    // triggers purgatory completion.
    val fetchRequest = createConsumerFetchRequest(
      maxResponseBytes = 1000,
      maxPartitionBytes = 1000,
      Seq(topicPartition),
      offsetMap,
      version,
      maxWaitMs = 20000,
      minBytes = 1
    )

    val socket = connect(brokerSocketServer(followerBrokerId))
    try {
      send(fetchRequest, socket)
      TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 1)
      val response = receive[FetchResponse](socket, ApiKeys.FETCH, version)
      assertEquals(Errors.NONE, response.error)
      assertEquals(util.Map.of(Errors.NONE, 2), response.errorCounts)
    } finally {
      socket.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testFetchFromLeaderWhilePreferredReadReplicaIsUnavailable(groupProtocol: String): Unit = {
    // Create a topic with 2 replicas where broker 0 is the leader and 1 is the follower.
    val admin = createAdminClient()
    TestUtils.createTopicWithAdmin(
      admin,
      topic,
      brokers,
      controllerServers,
      replicaAssignment = Map(0 -> Seq(leaderBrokerId, followerBrokerId))
    )

    TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 10)

    assertEquals(1, getPreferredReplica)

    // Shutdown follower broker.
    brokers(followerBrokerId).shutdown()
    val topicPartition = new TopicPartition(topic, 0)
    TestUtils.waitUntilTrue(() => {
      val endpoints = brokers(leaderBrokerId).metadataCache.getPartitionReplicaEndpoints(topicPartition, listenerName)
      !endpoints.containsKey(followerBrokerId)
    }, "follower is still reachable.")

    assertEquals(-1, getPreferredReplica)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testFetchFromFollowerWithRoll(groupProtocol: String): Unit = {
    // Create a topic with 2 replicas where broker 0 is the leader and 1 is the follower.
    val admin = createAdminClient()
    TestUtils.createTopicWithAdmin(
      admin,
      topic,
      brokers,
      controllerServers,
      replicaAssignment = Map(0 -> Seq(leaderBrokerId, followerBrokerId))
    )

    // Create consumer with client.rack = follower id.
    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.CLIENT_RACK_CONFIG, followerBrokerId.toString)
    consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol)
    val consumer = new KafkaConsumer(consumerProps, new ByteArrayDeserializer, new ByteArrayDeserializer)
    try {
      consumer.subscribe(util.List.of(topic))

      // Wait until preferred replica is set to follower.
      TestUtils.waitUntilTrue(() => {
        getPreferredReplica == 1
      }, "Preferred replica is not set")

      // Produce and consume.
      TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 1)
      TestUtils.pollUntilAtLeastNumRecords(consumer, 1)

      // Shutdown follower, produce and consume should work.
      brokers(followerBrokerId).shutdown()
      TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 1)
      TestUtils.pollUntilAtLeastNumRecords(consumer, 1)

      // Start the follower and wait until preferred replica is set to follower.
      brokers(followerBrokerId).startup()
      TestUtils.waitUntilTrue(() => {
        getPreferredReplica == 1
      }, "Preferred replica is not set")

      // Produce and consume should still work.
      TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 1)
      TestUtils.pollUntilAtLeastNumRecords(consumer, 1)
    } finally {
      consumer.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @ValueSource(strings = Array("classic"))
  def testRackAwareRangeAssignor(groupProtocol: String): Unit = {
    val partitionList = brokers.indices.toList

    val topicWithAllPartitionsOnAllRacks = "topicWithAllPartitionsOnAllRacks"
    createTopic(topicWithAllPartitionsOnAllRacks, brokers.size, brokers.size)

    // Racks are in order of broker ids, assign leaders in reverse order
    val topicWithSingleRackPartitions = "topicWithSingleRackPartitions"
    createTopicWithAssignment(topicWithSingleRackPartitions, partitionList.map(i => (i, Seq(brokers.size - i - 1))).toMap)

    // Create consumers with instance ids in ascending order, with racks in the same order.
    consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RangeAssignor].getName)
    val consumers = brokers.map { server =>
      consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      consumerConfig.setProperty(ConsumerConfig.CLIENT_RACK_CONFIG, server.config.rack.orElse(null))
      consumerConfig.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, s"instance-${server.config.brokerId}")
      consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000")
      consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      createConsumer()
    }

    val producer = createProducer()
    val executor = Executors.newFixedThreadPool(consumers.size)

    def verifyAssignments(partitionOrder: List[Int], topics: String*): Unit = {
      val assignments = partitionOrder.map { p =>
        topics.map(topic => new TopicPartition(topic, p)).toSet
      }

      val assignmentFutures = consumers.zipWithIndex.map { case (consumer, i) =>
        executor.submit(() => {
          val expectedAssignment = assignments(i)
          TestUtils.pollUntilTrue(consumer, () => consumer.assignment() == expectedAssignment.asJava,
            s"Timed out while awaiting expected assignment $expectedAssignment. The current assignment is ${consumer.assignment()}",
            waitTimeMs = 30000)
        }, 0)
      }
      assignmentFutures.foreach(future => assertEquals(0, future.get(30, TimeUnit.SECONDS)))

      assignments.flatten.foreach { tp =>
        producer.send(new ProducerRecord(tp.topic, tp.partition, s"key-$tp".getBytes, s"value-$tp".getBytes))
      }

      val recordFutures = consumers.zipWithIndex.map { case (consumer, i) =>
        executor.submit(() => {
          TestUtils.pollUntilAtLeastNumRecords(consumer, assignments(i).size, waitTimeMs = 30000)
        })
      }
      recordFutures.zipWithIndex.foreach { case (future, i) =>
        val records = future.get(30, TimeUnit.SECONDS)
        assertEquals(assignments(i), records.map(r => new TopicPartition(r.topic, r.partition)).toSet)
      }
      consumers.foreach{ _.commitSync() }
    }


    try {
      // Rack-based assignment results in partitions assigned in reverse order since partition racks are in the reverse order.
      consumers.foreach(_.subscribe(util.Set.of(topicWithSingleRackPartitions)))
      verifyAssignments(partitionList.reverse, topicWithSingleRackPartitions)

      // Non-rack-aware assignment results in ordered partitions.
      consumers.foreach(_.subscribe(util.Set.of(topicWithAllPartitionsOnAllRacks)))
      verifyAssignments(partitionList, topicWithAllPartitionsOnAllRacks)

      // Rack-aware assignment with co-partitioning results in reverse assignment for both topics.
      consumers.foreach(_.subscribe(util.Set.of(topicWithSingleRackPartitions, topicWithAllPartitionsOnAllRacks)))
      verifyAssignments(partitionList.reverse, topicWithAllPartitionsOnAllRacks, topicWithSingleRackPartitions)

      // Perform reassignment for topicWithSingleRackPartitions to reverse the replica racks and
      // verify that change in replica racks results in re-assignment based on new racks.
      val admin = createAdminClient()
      val reassignments = new util.HashMap[TopicPartition, util.Optional[NewPartitionReassignment]]()
      partitionList.foreach { p =>
        val newAssignment = new NewPartitionReassignment(util.List.of(p))
        reassignments.put(new TopicPartition(topicWithSingleRackPartitions, p), util.Optional.of(newAssignment))
      }
      admin.alterPartitionReassignments(reassignments).all().get(30, TimeUnit.SECONDS)
      verifyAssignments(partitionList, topicWithAllPartitionsOnAllRacks, topicWithSingleRackPartitions)

    } finally {
      executor.shutdownNow()
    }
  }

  /**
   * Demonstrates the spurious-rebalance behavior introduced by KAFKA-14867 / KIP-881 on the classic
   * consumer protocol. In a multi-rack, rack-aware cluster (client.rack configured), a SINGLE broker
   * bounce causes TWO rebalances:
   *   - one when the broker goes down: its replicas drop out of the live-broker list, so they resolve
   *     to Node(host="", rack=null) and the rack Set tracked by ConsumerCoordinator.MetadataSnapshot
   *     changes, which is treated as a metadata change and triggers a rejoin;
   *   - one when it comes back: the rack reappears, the Set changes again, and we rejoin again.
   *
   * Run with INFO logging and grep for "[RACK-REPRO]" to see the replicas received from metadata and
   * the rack Set being built, alongside the "Request joining group due to: cached metadata has
   * changed ..." lines emitted when the rejoin is requested.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @ValueSource(strings = Array("classic"))
  @Timeout(120)
  def testBrokerBounceCausesTwoRebalancesWithRackAwareConsumer(groupProtocol: String): Unit = {
    val rackTopic = "rack-bounce-topic"
    val rackTopicPartition = new TopicPartition(rackTopic, 0)
    val groupId = "rack-bounce-group"

    val admin = createAdminClient()
    // One partition replicated on BOTH brokers. enableFetchFromFollower sets broker.rack = broker id,
    // so the healthy replica rack Set for this partition is {"0", "1"}.
    TestUtils.createTopicWithAdmin(
      admin,
      rackTopic,
      brokers,
      controllerServers,
      replicaAssignment = Map(0 -> Seq(leaderBrokerId, followerBrokerId))
    )
    TestUtils.waitUntilLeaderIsKnown(brokers, rackTopicPartition)

    val assignedCount = new AtomicInteger(0)
    val events = new ConcurrentLinkedQueue[String]()
    def record(msg: String): Unit = {
      events.add(msg)
      println(msg)
    }

    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[RangeAssignor].getName)
    // Rack-aware: this is what makes MetadataSnapshot track replica racks. The value only needs to be
    // present; it does not affect whether the rack Set changes when some broker bounces.
    consumerProps.put(ConsumerConfig.CLIENT_RACK_CONFIG, leaderBrokerId.toString)
    // Refresh metadata quickly so the consumer notices the rack change soon after the bounce.
    consumerProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000")
    val consumer = new KafkaConsumer(consumerProps, new ByteArrayDeserializer, new ByteArrayDeserializer)

    val listener = new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit =
        record(s"[RACK-REPRO] onPartitionsAssigned #${assignedCount.incrementAndGet()} -> $partitions")
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit =
        record(s"[RACK-REPRO] onPartitionsRevoked -> $partitions")
    }

    val keepPolling = new AtomicBoolean(true)
    val pollThread = new Thread(() => {
      while (keepPolling.get()) {
        try consumer.poll(Duration.ofMillis(100))
        catch { case e: Exception => events.add(s"[RACK-REPRO] poll exception: $e") }
      }
    }, "rack-repro-poll-thread")

    try {
      consumer.subscribe(util.List.of(rackTopic), listener)
      pollThread.start()

      // Initial join/assignment - this is the baseline, not part of the bounce.
      TestUtils.waitUntilTrue(() => assignedCount.get >= 1,
        "Consumer never received its initial assignment", waitTimeMs = 30000)
      val baseline = assignedCount.get
      record(s"[RACK-REPRO] baseline assignment count after initial join = $baseline")

      // Find the group coordinator and bounce the OTHER broker, so the consumer keeps its coordinator
      // throughout and the only relevant metadata change is the replica's rack going null/non-null.
      var coordinatorId = -1
      TestUtils.waitUntilTrue(() => {
        try {
          coordinatorId = admin.describeConsumerGroups(util.List.of(groupId))
            .describedGroups().get(groupId).get(30, TimeUnit.SECONDS).coordinator().id()
          coordinatorId >= 0
        } catch { case _: Exception => false }
      }, "Group coordinator was not assigned", waitTimeMs = 30000)
      val bounceBrokerId = if (coordinatorId == leaderBrokerId) followerBrokerId else leaderBrokerId
      record(s"[RACK-REPRO] group coordinator is broker $coordinatorId; bouncing broker $bounceBrokerId " +
        s"(a replica of $rackTopic, leaving the coordinator untouched)")

      // ---- DOWN: the replica's rack becomes null -> rack Set changes -> rejoin ----
      record(s"[RACK-REPRO] >>> shutting down broker $bounceBrokerId")
      brokers(bounceBrokerId).shutdown()
      TestUtils.waitUntilTrue(() => assignedCount.get >= baseline + 1,
        s"Consumer did not rebalance after broker $bounceBrokerId went down", waitTimeMs = 90000)
      val afterDown = assignedCount.get
      record(s"[RACK-REPRO] assignment count after broker went DOWN = $afterDown")

      // ---- UP: the rack reappears -> rack Set changes again -> rejoin again ----
      record(s"[RACK-REPRO] >>> starting broker $bounceBrokerId")
      brokers(bounceBrokerId).startup()
      TestUtils.waitUntilTrue(() => assignedCount.get >= afterDown + 1,
        s"Consumer did not rebalance after broker $bounceBrokerId came back", waitTimeMs = 90000)
      val afterUp = assignedCount.get
      record(s"[RACK-REPRO] assignment count after broker came UP = $afterUp")

      keepPolling.set(false)
      pollThread.join(TimeUnit.SECONDS.toMillis(10))

      val bounceRebalances = afterUp - baseline
      record(s"[RACK-REPRO] ===== a single bounce of broker $bounceBrokerId produced " +
        s"$bounceRebalances rebalance(s) =====")

      assertTrue(bounceRebalances >= 2,
        s"Expected a single broker bounce to cause at least 2 rebalances (one on shutdown, one on " +
          s"startup) on trunk, but observed $bounceRebalances. Timeline:\n${events.asScala.mkString("\n")}")
    } finally {
      keepPolling.set(false)
      pollThread.join(TimeUnit.SECONDS.toMillis(10))
      consumer.close()
    }
  }

  private def getPreferredReplica: Int = {
    val topicPartition = new TopicPartition(topic, 0)
    val offsetMap = Map(topicPartition -> 0L)

    val request = createConsumerFetchRequest(
      maxResponseBytes = 1000,
      maxPartitionBytes = 1000,
      Seq(topicPartition),
      offsetMap,
      ApiKeys.FETCH.latestVersion,
      maxWaitMs = 500,
      minBytes = 1,
      rackId = followerBrokerId.toString
    )
    val response = connectAndReceive[FetchResponse](request, brokers(leaderBrokerId).socketServer)
    assertEquals(Errors.NONE, response.error)
    assertEquals(util.Map.of(Errors.NONE, 2), response.errorCounts)
    assertEquals(1, response.data.responses.size)
    val topicResponse = response.data.responses.get(0)
    assertEquals(1, topicResponse.partitions.size)

    topicResponse.partitions.get(0).preferredReadReplica
  }
}
