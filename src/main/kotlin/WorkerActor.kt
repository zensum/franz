package franz.internal

import franz.JobStatus
import franz.Message
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch

private inline fun tryJobStatus(fn: () -> JobStatus) = try {
    fn()
} catch (ex: Exception) {
    // logger.error(ex, "Unhandled error in job")
    JobStatus.TransientFailure
}

typealias WorkerFunction<T, U> = suspend (Message<T, U>) -> JobStatus
private fun <T, U> worker(cons: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) = cons.subscribe {
    launch(CommonPool) {
        cons.setJobStatus(it, tryJobStatus {
            fn(it)
        })
    }
}

fun <T, U> createWorker(cons: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) = Thread { worker(cons, fn) }
