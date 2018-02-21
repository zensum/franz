package franz.engine

import franz.JobStatus
import franz.Message

typealias WorkerFunction<T, U> = suspend (Message<T, U>) -> JobStatus

interface ConsumerActor<T, U> {
    fun start()
    fun stop()
    fun subscribe(fn: (Message<T, U>) -> Unit)
    fun setJobStatus(msg: Message<T, U>, status: JobStatus)
    fun createWorker(fn: WorkerFunction<T, U>): Runnable
}