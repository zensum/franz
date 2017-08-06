package franz

import franz.internal.JobDSL
import franz.internal.JobStatus
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class JobStateTest {

    private fun <T> consumerRecordOfValue(value: T): ConsumerRecord<String, T> {
        return ConsumerRecord("topic", 0, 0L, "key", value)
    }

    private fun <U> jobFrom(value: U): JobDSL<String, U> = JobDSL(consumerRecordOfValue(value))

    @Test
    fun testCreateJobState() {
        val job = jobFrom("1")
        assertEquals("1", job.asPipe().value)
    }

    @Test
    fun testValidateTrue() {
        val job = jobFrom("1")
        val status = job.asPipe()
                .validate { true }
                .validate { true }

        assertEquals(JobStatus.Incomplete, status.status)
    }

    @Test
    fun testValidateFalse() {
        val job = jobFrom("1")
        val status = job.asPipe()
                .validate { true }
                .validate { false }
                .validate { true }

        assertEquals(JobStatus.PermanentFailure, status.status)
    }

    @Test
    fun testExecuteTrue() {
        val job = jobFrom("1")
        val status = job.asPipe()
                .execute { true }
                .execute { true }

        assertEquals(JobStatus.Incomplete, status.status)
    }

    @Test
    fun testExecuteFalse() {
        val job = jobFrom("1")
        val status = job.asPipe()
                .execute { true }
                .execute { false }
                .execute { true }

        assertEquals(JobStatus.TransientFailure, status.status)
    }

    @Test
    fun testConfirmTrue() {
        val job = jobFrom("1")
        val status = job.asPipe()
                .confirm { true }
                .confirm { true }

        assertEquals(JobStatus.Incomplete, status.status)
    }

    @Test
    fun testConfirmFalse() {
        val job = jobFrom("1")
        val status = job.asPipe()
                .confirm { true }
                .confirm { false }
                .confirm { true }

        assertEquals(JobStatus.Success, status.status)
    }

    @Test
    fun testMapSuccessful() {
        val job = jobFrom("1")
        val status = job.asPipe()
                .validate { true }
                .map(Integer::parseInt)
                .validate { it == 1 }

        assertEquals(1, status.value)
    }

    @Test
    fun testMapToNull() {
        val job = jobFrom("1")

        val state = job.asPipe()
                .validate {true}
                .validate {false}
                .map(Integer::parseInt)

        assertNull(state.value)
    }

    @Test
    fun testEndSuccessful() {
        val job = jobFrom("1")

        val status = job.asPipe()
                .validate { true}
                .execute { true }
                .confirm { true }
                .end { true }

        assertEquals(JobStatus.Success, status)
    }

    @Test
    fun testEndFailure() {
        val job = jobFrom("1")

        val status = job.asPipe()
                .validate { true}
                .execute { true }
                .confirm { true }
                .end { false }

        assertEquals(JobStatus.TransientFailure, status)
    }

    @Test
    fun testConversionWithTwoMapsInSequence() {
        val job = jobFrom("1")
        val result = job.asPipe()
                .confirm { it.isNotEmpty() }
                .map(Integer::parseInt)
                .map { it * 2 }
                .map { it + 4 }
                .validate { it > 1 }
                .end { it > 0 }

        assertEquals(JobStatus.Success, result)
    }

    @Test
    fun testConversionWithFailingValidationAndMap() {
        val job = jobFrom("1")
        val result = job.asPipe()
                .confirm { it.isNotEmpty() }
                .map(Integer::parseInt)
                .map { it * 2 }
                .validate { it < 0 } // This should fail and give JobStatus.PermanentFailure
                .map { it + 4 }
                .end { it > 0 }

        assertEquals(JobStatus.PermanentFailure, result)
    }
}