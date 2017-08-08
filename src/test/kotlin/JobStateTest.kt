package franz

import franz.internal.JobDSL
import franz.internal.JobStatus
import kotlinx.coroutines.experimental.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class JobStateTest {

    private fun <T> consumerRecordOfValue(value: T): ConsumerRecord<String, T> {
        return ConsumerRecord("topic", 0, 0L, "key", value)
    }

    private fun <U> jobFrom(value: U): JobDSL<String, U> = JobDSL(consumerRecordOfValue(value))

    val jobOne = jobFrom("1")

    @Test
    fun testCreateJobState() {
        val job = jobOne
        assertEquals("1", job.asPipe().value)
    }

    @Test
    fun testValidateTrue() {
        val job = jobOne
        val status = job.asPipe()
                .require { true }
                .require { true }

        assertEquals(JobStatus.Incomplete, status.status)
    }

    @Test
    fun testValidateFalse() {
        val job = jobOne
        val status = job.asPipe()
                .require { true }
                .require { false }
                .require { true }

        assertEquals(JobStatus.PermanentFailure, status.status)
    }

    @Test
    fun testExecuteTrue() {
        val job = jobOne
        val status = job.asPipe()
                .execute { true }
                .execute { true }

        assertEquals(JobStatus.Incomplete, status.status)
    }

    @Test
    fun testExecuteFalse() {
        val job = jobOne
        val status = job.asPipe()
                .execute { true }
                .execute { false }
                .execute { true }

        assertEquals(JobStatus.TransientFailure, status.status)
    }

    @Test
    fun testConfirmTrue() {
        val job = jobOne
        val status = job.asPipe()
                .advanceIf { true }
                .advanceIf { true }

        assertEquals(JobStatus.Incomplete, status.status)
    }

    @Test
    fun testConfirmFalse() {
        val job = jobOne
        val status = job.asPipe()
                .advanceIf { true }
                .advanceIf { false }
                .advanceIf { true }

        assertEquals(JobStatus.Success, status.status)
    }

    @Test
    fun testMapSuccessful() {
        val job = jobOne
        val status = job.asPipe()
                .require { true }
                .map(Integer::parseInt)
                .require { it == 1 }

        assertEquals(1, status.value)
    }

    @Test
    fun testMapToNull() {
        val job = jobOne

        val state = job.asPipe()
                .require { true }
                .require { false }
                .map(Integer::parseInt)

        assertNull(state.value)
    }

    @Test
    fun testEndSuccessful() {
        val job = jobOne

        val status = job.asPipe()
                .require { true}
                .execute { true }
                .advanceIf { true }
                .end { true }

        assertEquals(JobStatus.Success, status)
    }

    @Test
    fun testEndFailure() {
        val job = jobOne

        val status = job.asPipe()
                .require { true}
                .execute { true }
                .advanceIf { true }
                .end { false }

        assertEquals(JobStatus.TransientFailure, status)
    }

    @Test
    fun testConversionWithTwoMapsInSequence() {
        val job = jobOne
        val result = job.asPipe()
                .advanceIf { it.isNotEmpty() }
                .map(Integer::parseInt)
                .map { it * 2 }
                .map { it + 4 }
                .require { it > 1 }
                .end { it > 0 }

        assertEquals(JobStatus.Success, result)
    }

    @Test
    fun testConversionWithFailingValidationAndMap() {
        val job = jobOne
        val result = job.asPipe()
                .advanceIf { it.isNotEmpty() }
                .map(Integer::parseInt)
                .map { it * 2 }
                .require { it < 0 } // This should fail and give JobStatus.PermanentFailure
                .map { it + 4 }
                .end { it > 0 }

        assertEquals(JobStatus.PermanentFailure, result)
    }

    private suspend fun lolz() = 10

    @Test
    fun testInlineMapWorks() {
        runBlocking {
            jobOne.asPipe().map { lolz() }
        }.let {
            assertEquals(10, it.value)
        }
    }

}