package franz

import franz.engine.kafka_one.JobId
import franz.engine.kafka_one.JobStatuses
import franz.engine.kafka_one.jobId
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Test
import kotlin.test.*

private typealias JS = JobStatuses<String, String>
private typealias CR = ConsumerRecord<String, String>

private const val DEFAULT_TOPIC = "test_topic"
private const val ALTERNATE_TOPIC = "other_topic"
private const val DEFAULT_VALUE = "test_value"
private const val DEFAULT_PARTITION = 1
private const val DEFAULT_KEY = "test_key"

private fun CROffsetPartition(partition: Int, offset: Int) = CR(
        DEFAULT_TOPIC,
        partition,
        offset.toLong(),
        DEFAULT_KEY,
        DEFAULT_VALUE
)

private fun CROffset(x: Int) = CROffsetPartition(DEFAULT_PARTITION, x)

private val ALTERNATE_TOPIC_P1 = TopicPartition(ALTERNATE_TOPIC, DEFAULT_PARTITION)
private val DEFAULT_TOPIC_PARTITION = TopicPartition(DEFAULT_TOPIC, DEFAULT_PARTITION)
private val DEFAULT_TOPIC_P2 = TopicPartition(DEFAULT_TOPIC, 2)

private val CR1 = CROffset(1)
private val CR2 = CROffset(2)
private val CR3 = CROffset(3)

private val P2CR1 = CROffsetPartition(2, 1)
private val P2CR2 = CROffsetPartition(2, 2)
private val P2CR3 = CROffsetPartition(2, 3)

private val OTHER_TOPIC_CR1 = CR(ALTERNATE_TOPIC, 1, 1, DEFAULT_KEY, DEFAULT_VALUE)

private val FAILED = JobStatus.TransientFailure

// js is immutable, and JS() its zero value
private val js = JS()

private fun jsWith(updates: Map<JobId, JobStatus>) = js.update(updates)

private fun recordsWStatuses(vararg pairs: Pair<CR, JobStatus>) =
        pairs.toMap().mapKeys { (k, _) -> k.jobId() }

private fun recordWStatus(cr: CR, status: JobStatus) =
        mapOf(cr.jobId() to status)

class JobStatusesTest {
    @Test
    fun testAddingJob() {
        js.addJobs(listOf(CR1)).let {
            assertNotEquals(js, it, "Adding a record should modify JS")
        }
    }
    @Test
    fun testAddingNoJobs() {
        js.addJobs(emptyList()).let {
            assertEquals(js, it, "Adding the empty list of jobs should have no effect")
        }

    }
    @Test
    fun testAddingJobIdempotent() {
        js.addJobs(listOf(CR1)).let {
            val anotherAdd = js.addJobs(listOf(CR1))
            assertEquals(it, anotherAdd, "Repeated adds of the same record should be idempotent")
        }
    }
    @Test
    fun testEmptyNotCommittable() {
        js.committableOffsets().let {
            assertTrue("No offsets should be committable") { it.isEmpty() }
        }
    }
    @Test
    fun testTrivialCommittable() {
        jsWith(recordWStatus(CR1, JobStatus.Success)).committableOffsets().let {
            val res = it[DEFAULT_TOPIC_PARTITION]
            assertNotNull(res,
                    "The default partition should have a committable offset"
            )
            val offset = res!!.offset()
            assertEquals(offset, CR1.offset(), "The offset of the Successful job should be comitted")
        }
    }
    @Test
    fun testTrivialNonCommittable() {
        jsWith(recordWStatus(CR1, JobStatus.Incomplete)).committableOffsets().let {
            assertTrue("No offsets should be committable") { it.isEmpty() }
        }
    }
    @Test
    fun testHeadOfLineNonCommittable() {
        jsWith(recordsWStatuses(
                CR1 to JobStatus.Incomplete,
                CR2 to JobStatus.Success
        )).committableOffsets().let {
            assertTrue("No offsets should be committable") { it.isEmpty() }
        }
    }
    @Test
    fun testSomeCommittable() {
        jsWith(recordsWStatuses(
                CR1 to JobStatus.Success,
                CR2 to JobStatus.Incomplete,
                CR3 to JobStatus.Success
        )).committableOffsets().let {
            val res = it[DEFAULT_TOPIC_PARTITION]
            assertNotNull(res, "One partition should be committable")
            val offset = res!!.offset()
            assertEquals(offset, CR1.offset())
        }
    }
    @Test
    fun testSinglePartitionCommitable() {
        jsWith(recordsWStatuses(
                CR1 to JobStatus.Success,
                P2CR1 to JobStatus.Incomplete
        )).committableOffsets().let {
            assertNull(it[DEFAULT_TOPIC_P2], "Partition 2 should not be committable")
            assertNotNull(it[DEFAULT_TOPIC_PARTITION], "Partition 1 should be committable")
            assertEquals(CR1.offset(), it[DEFAULT_TOPIC_PARTITION]!!.offset(), "CR1 should be committable")
        }
    }
    @Test
    fun testAlternateTopicCommittable() {
        jsWith(recordsWStatuses(
                OTHER_TOPIC_CR1 to JobStatus.Success,
                P2CR1 to JobStatus.Incomplete
        )).committableOffsets().let {
            assertNull(it[DEFAULT_TOPIC_PARTITION], "Default topic partition should not be committable")
            assertNotNull(it[ALTERNATE_TOPIC_P1], "alternate topic, partition 1 should be committable")
            assertEquals(OTHER_TOPIC_CR1.offset(),
                    it[ALTERNATE_TOPIC_P1]!!.offset(), "OTHER_TOPIC_CR1 should be committable"
            )
        }
    }
    @Test
    fun testRescheduleTransientUnchangedOnEmpty() {
        val (newJS, retry) = js.rescheduleTransientFailures()
        assertEquals(js, newJS, "Jobs should be unchanged")
        assertTrue("Nothing should be retried") { retry.isEmpty() }
    }
    @Test
    fun testRescheduleTransientUnchangedNotFailed() {
        val origJs = jsWith(recordsWStatuses(CR1 to JobStatus.Incomplete))
        origJs.rescheduleTransientFailures().let { (newJS, retry) ->
            assertEquals(origJs, newJS, "Jobs should be unchanged")
            assertTrue("Nothing should be retried") { retry.isEmpty() }
        }
    }
    @Test
    fun testRescheduleTransientFailedJob() {
        val origJs = js
                .addJobs(listOf(CR1))
                .update(recordsWStatuses(CR1 to FAILED))
        origJs.rescheduleTransientFailures().let { (newJS, retry) ->
            assertNotEquals(origJs, newJS, "Jobs should be changed")
            assertEquals(1, retry.count(), "Exactly one job should returned for retrying")
            assertEquals(CR1, retry[0], "CR1 should be retried")
        }
    }
    @Test
    fun testRescheduleTwoTransientFailedJobs() {
        val origJs = js
                .addJobs(listOf(CR1, CR2))
                .update(recordsWStatuses(CR1 to FAILED, CR2 to FAILED))
        origJs.rescheduleTransientFailures().let { (newJS, retry) ->
            assertNotEquals(origJs, newJS, "Jobs should be changed")
            assertEquals(2, retry.count(), "Exactly one job should returned for retrying")
            assertTrue("CR1 to be retried") { retry.contains(CR1) }
            assertTrue("CR2 to be retried") { retry.contains(CR2) }
        }
    }
    @Test
    fun testRemoveCommittedEmpty() {
        val orig = jsWith(recordsWStatuses(CR1 to JobStatus.Success, CR2 to JobStatus.Success))
        orig.removeCommitted(emptyMap()).let {
            assertEquals(orig, it, "Should be unchanged")
        }
    }
    @Test
    fun testRemoveCommittedAll() {
        jsWith(recordsWStatuses(CR1 to JobStatus.Success, CR2 to JobStatus.Success))
                .removeCommitted(mapOf(DEFAULT_TOPIC_PARTITION to OffsetAndMetadata(2))).let {
            assertEquals(js, it, "Should be unchanged")
        }
    }
    @Test
    fun testRemoveCommittedSome() {
        jsWith(recordsWStatuses(CR1 to JobStatus.Success, CR2 to JobStatus.Success))
                .removeCommitted(mapOf(DEFAULT_TOPIC_PARTITION to OffsetAndMetadata(1))).let {
            assertEquals(
                    jsWith(recordsWStatuses(CR2 to JobStatus.Success)),
                    it,
                    "Should contain only CR2"
            )
        }
    }
    @Test
    fun testRemoveCommittedBehind() {
        val orig = jsWith(recordsWStatuses(CR2 to JobStatus.Success, CR3 to JobStatus.Success))
        orig.removeCommitted(mapOf(DEFAULT_TOPIC_PARTITION to OffsetAndMetadata(1))).let {
            assertEquals(orig, it, "Should be unchanged")
        }
    }
    @Test
    fun testRemoveOnePartition() {
        jsWith(recordsWStatuses(CR1 to JobStatus.Success, P2CR1 to JobStatus.Success))
                .removeCommitted(mapOf(DEFAULT_TOPIC_PARTITION to OffsetAndMetadata(1))).let {
            assertEquals(
                    jsWith(recordsWStatuses(P2CR1 to JobStatus.Success)),
                    it,
                    "Partition 2 should be safe from partition one removal"
            )
        }
    }
    @Test
    fun testRemoveTwoPartitions() {
        jsWith(recordsWStatuses(CR1 to JobStatus.Success, P2CR1 to JobStatus.Success))
                .removeCommitted(mapOf(
                        DEFAULT_TOPIC_PARTITION to OffsetAndMetadata(1),
                        DEFAULT_TOPIC_P2 to OffsetAndMetadata(2))).let {
            assertEquals(js, it, "all records should be removed")
        }
    }
}
