package franz.internal

import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
import mu.KLogging
import org.apache.kafka.clients.consumer.ConsumerRecord


class JobDSL<T, U>(rec: ConsumerRecord<T, U>){
    val success = JobStatus.Success
    fun permanentFailure(ex: Throwable) = JobStatus.PermanentFailure.also {
        logger.error("PermanentFailure: ", ex)
    }
    fun transientFailure(ex: Throwable) = JobStatus.TransientFailure.also {
        logger.error("TransientFailure: ", ex)
    }
    val key = rec.key()!!
    val value = rec.value()!!
    companion object : KLogging()
}

typealias WorkerFunction<T, U> = suspend JobDSL<T, U>.() -> JobStatus

private fun <T, U> worker(cons: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) = cons.subscribe {
    launch(CommonPool) {
        val dsl = JobDSL(it)
        cons.setJobStatus(it.jobId(), try {
            fn(dsl)
        } catch (exc: Exception) {
            dsl.transientFailure(exc)
        })
    }
}

fun <T, U> createWorker(cons: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) = Thread { worker(cons, fn) }
