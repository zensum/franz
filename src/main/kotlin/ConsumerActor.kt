package franz.internal

import franz.JobStatus
import franz.Message

interface ConsumerActor<T, U> {
    fun start()
    fun stop()
    fun subscribe(fn: (Message<T, U>) -> Unit)
    fun setJobStatus(msg: Message<T, U>, status: JobStatus)
}