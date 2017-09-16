package franz.internal

import franz.JobStatus
import franz.Message
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
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

private inline fun tryJobStatus(fn: () -> JobStatus) = try {
    fn()
} catch (ex: Exception) {
    // logger.error(ex, "Unhandled error in job")
    JobStatus.TransientFailure
}

typealias WorkerFunction<T, U> = suspend (Message<T, U>) -> JobStatus
private fun <T, U> worker(cons: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) = cons.subscribe {
    launch(CommonPool) {
        val job = KafkaMessage(it)
        cons.setJobStatus(it.jobId(), tryJobStatus {
            fn(job)
        })
    }
}

fun <T, U> createWorker(cons: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) = Thread { worker(cons, fn) }
