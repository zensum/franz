package franz

import franz.internal.JobDSL
import franz.internal.JobStatus
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
    fun testNonNullableConversionNeededWhenStillInProgress() {
        val job = jobFrom("1")
        job.asPipe()
                .confirm { it.isNotEmpty() }
                .map(Integer::parseInt) // Return type is "Int?"
                .validate {it == 1} // But function "process" enforces non-nullable type with !!
                                    // when the job is still in progress.
    }

    @Test
    fun testNonNullableConversionWithTwoMapsInSequence() {
        val job = jobFrom("1")
        job.asPipe()
                .confirm { it.isNotEmpty() }
                .map(Integer::parseInt)
                .map { it * 2 } // Maps in sequence does however now work so sell.
        // Here a !! is needed to explicitly say it's not null, or we have a compile error.
    }
}