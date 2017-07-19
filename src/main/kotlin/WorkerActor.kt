package franz.internal

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

class JobDSL<T, U>(rec: ConsumerRecord<T, U>){
    val success = JobStatus.Success
    fun permanentFailure(ex: Throwable) = JobStatus.PermanentFailure(ex)
    fun transientFailure(ex: Throwable) = JobStatus.TransientFailure(ex)
    val key = rec.key()!!
    val value = rec.value()!!
}

typealias WorkerFunction<T, U> = JobDSL<T, U>.() -> JobStatus

private fun <T, U> tryJob(dsl: JobDSL<T, U>, fn: WorkerFunction<T, U>) = try {
    fn(dsl)
} catch (exc: Exception) {
    dsl.transientFailure(exc)
}

private fun <T, U> worker(cons: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) = cons.subscribe {
    val dsl = JobDSL(it)
    val res = tryJob(dsl, fn)
    cons.setJobStatus(it.jobId(), res)
}

private fun <T, U> workerThread(cons: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) = Thread { worker(cons, fn) }

fun <T, U> createWorkers(n: Int, cons: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) =
        (1..n).map { workerThread(cons, fn) }.forEach { it.start() }
