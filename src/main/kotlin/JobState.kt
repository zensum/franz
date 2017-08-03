package franz

import franz.internal.JobDSL
import franz.internal.JobStatus

fun <T, U> JobDSL<T, U>.asPipe(): JobState<U> = JobState(this.value)

class JobState<U> internal constructor(val value: U){
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
    fun validate(predicate: (U) -> Boolean): JobState<U> = process(JobStatus.PermanentFailure, predicate)

    /**
     * Use when an operation may fail in such a why that a retry should be scheduled, like an error that is
     * a result of a network connectivity issue or similar. In other words, there is nothing in the job itself
     * that is erroneous, only the conditions for when it was executed.
     */
    fun execute(predicate: (U) -> Boolean): JobState<U> = process(JobStatus.TransientFailure, predicate)

    /**
     *  Use when either outcome is regarded as a successful result. Most common example of this is when a
     *  job has already been processed and is already found written in the idempotence store or similar.
     *  Everything is in its order but the current job should not trigger any further action and resolve
     *  to [JobStatus.Success].
     */
    fun confirm(predicate: (U) -> Boolean): JobState<U> = process(JobStatus.Success, predicate)

    /**
     * Use when conducting the final operation on the job. If it successfully done (the predicate returns true)
     * the [JobState] will automatically be set as [JobStatus.Success] and return the [JobStatus] rather than
     * the [JobState] itself, prohibiting that any more work is done on this job through this pipe. The returned
     * [JobStatus] can never be [JobStatus.Incomplete] when returning from this function.
     * */

    fun end(predicate: (U) -> Boolean): JobStatus {
        val state = process(JobStatus.TransientFailure, predicate)
        if(state.inProgress())
            state.status = JobStatus.Success
        return state.status
    }

    private inline fun process(newStatus: JobStatus, predicate: (U) -> Boolean): JobState<U> {
        if(inProgress() && !predicate(value))
            this.status = newStatus
        return this
    }

    fun <R> map(transform: (U) -> R): JobState<R> {
        if(!inProgress())
            throw IllegalStateException("Try to perform a map operation on job that is longer in progress")
        val newVal: R = transform(value)
        return JobState(newVal)
    }
}