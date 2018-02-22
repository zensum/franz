package franz.engine.mock

import franz.JobStatus
import franz.Message
import franz.engine.ConsumerActor
import franz.engine.WorkerFunction
import kotlinx.coroutines.experimental.runBlocking

abstract class MockConsumerActorBase<T, U> : ConsumerActor<T, U> {

    data class Result(
        val throwable: Throwable?,
        val status: JobStatus
    )

    private val internalResults: MutableList<Result> = mutableListOf()
    fun results() = internalResults.toList()

    protected var handlers = mutableListOf<(Message<T, U>) -> Unit>()

    override fun start() = Unit
    override fun stop() = Unit

    override fun setJobStatus(msg: Message<T, U>, status: JobStatus) {
        internalResults.add(Result(throwable = null, status = status))
    }

    private fun setException(e: Throwable) {
        internalResults.add(Result(e, JobStatus.TransientFailure))
    }

    override fun createWorker(fn: WorkerFunction<T, U>) {
                worker(this, fn)
    }

    private fun worker(consumer: ConsumerActor<T, U>, fn: WorkerFunction<T, U>) {
        consumer.subscribe {
            try {
                val result = runBlocking { fn(it) }
                setJobStatus(it, result)
            } catch (e: Throwable) {
                setException(e)
            }
        }
    }

    fun createFactory() =
        MockConsumerActorFactory(this)
}