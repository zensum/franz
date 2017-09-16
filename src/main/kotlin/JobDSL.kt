package franz.internal

import franz.JobStatus
import franz.Message
import mu.KLogging

class JobDSL<T, U>(rec: Message<T, U>) {
    val success = JobStatus.Success
    @Deprecated("Handle exception and return jobstatus")
    fun permanentFailure(ex: Throwable) = JobStatus.PermanentFailure.also {
        logger.error("PermanentFailure: ", ex)
    }
    @Deprecated("Handle exception and return jobstatus")
    fun transientFailure(ex: Throwable) = JobStatus.TransientFailure.also {
        logger.error("TransientFailure: ", ex)
    }
    private val keyInner = rec.key()
    val key = lazy { keyInner!! }
    fun keySet() = keyInner != null

    val value = rec.value()!!
    companion object : KLogging()
}