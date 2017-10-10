package franz

import franz.engine.ConsumerActor
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
import mu.KotlinLogging

private val logger = KotlinLogging.logger("WorkerActor")

private inline fun tryJobStatus(fn: () -> JobStatus) = try {
    fn()
} catch (ex: Exception) {
    logger.error("Job threw an exception", ex)
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
