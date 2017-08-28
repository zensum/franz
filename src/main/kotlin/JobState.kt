package franz

import franz.internal.JobDSL
import franz.internal.JobStatus

fun <T, U: Any> JobDSL<T, U>.asPipe(): JobState<U> = JobState(this.value)

class JobState<U: Any> @PublishedApi internal constructor(val value: U?){
    var status: JobStatus = JobStatus.Incomplete

        get() = field

        set(value) {
            if(status == JobStatus.Incomplete)
                field = value
        }

    fun inProgress(): Boolean = status == JobStatus.Incomplete

    /**
     * Use when an operation must succeed or it is considered a permanent failure and should not trigger a retry,
     * like checking the validity of phone number or mail address.
     */
    inline fun require(predicate: (U) -> Boolean): JobState<U> = process(JobStatus.PermanentFailure, predicate)

    /**
     * Use when an operation may fail in such a why that a retry should be scheduled, like an error that is
     * a result of a network connectivity issue or similar. In other words, there is nothing in the job itself
     * that is erroneous, only the conditions for when it was executed.
     */
    inline fun execute(predicate: (U) -> Boolean): JobState<U> = process(JobStatus.TransientFailure, predicate)

    /**
     *  Use when either outcome is regarded as a successful result. Most common example of this is when a
     *  job has already been processed and is already found written in the idempotence store or similar.
     *  Everything is in its order but the current job should not trigger any further action and resolve
     *  to [JobStatus.Success].
     */
    inline fun advanceIf(predicate: (U) -> Boolean): JobState<U> = process(JobStatus.Success, predicate)

    /**
     * Use for modelling a side-effect which doesn't have a return status. This
     * function is equivalent to calling require with a function that always returns true.
     */
    inline fun sideEffect(fn: (U) -> Unit): JobState<U> = execute { fn(it!!); true }

    /**
     * Use when conducting the final operation on the job. If it successfully done (the predicate returns true)
     * the [JobState] will automatically be set as [JobStatus.Success] and return the [JobStatus] rather than
     * the [JobState] itself, prohibiting that any more work is done on this job through this pipe. The returned
     * [JobStatus] can never be [JobStatus.Incomplete] when returning from this function (unless that was the status
     * prior to this function call).
     * */

    inline fun end(predicate: (U) -> Boolean): JobStatus {
        if(inProgress()) {
            this.status = when(predicate(value!!)) {
                true -> JobStatus.Success
                false -> JobStatus.TransientFailure
            }
        }
        return status
    }

    @JvmName("endNullary")
    fun end(): JobStatus {
        this.status = JobStatus.Success
        return status
    }

    inline fun process(newStatus: JobStatus, predicate: (U) -> Boolean): JobState<U> {
        if(inProgress() && !predicate(value!!))
            this.status = newStatus
        return this
    }

    inline fun <R: Any> map(transform: (U) -> R): JobState<R> {
        val transFormedVal: R? = when(inProgress()) {
            true -> value?.let(transform)
            false -> null
        }

        val state: JobState<R> = JobState(transFormedVal)
        state.status = this.status
        return state
    }
}