package franz

import mu.KotlinLogging

val log = KotlinLogging.logger("job")
@Deprecated("Use WorkerBuilder.pipedHandler instead")
fun <T, U: Any> JobDSL<T, U>.asPipe(): JobState<U> = JobState(this.value)

class JobState<U: Any> @PublishedApi internal constructor(val value: U?, val interceptors: List<WorkerInterceptor> = emptyList()) {
    var status: JobStatus = JobStatus.Incomplete

        get() = field

        set(value) {
            if (status == JobStatus.Incomplete)
                field = value
        }

    fun inProgress(): Boolean = status == JobStatus.Incomplete

    /**
     * Use when an operation must succeed or it is considered a permanent failure and should not trigger a retry,
     * like checking the validity of phone number or mail address.
     */
    fun require(predicate: (U) -> Boolean): JobState<U> = process(JobStatus.PermanentFailure, predicate)
    fun require(msg: String, predicate: (U) -> Boolean): JobState<U> = process(JobStatus.PermanentFailure, predicate, msg)

    /**
     * Use when an operation may fail in such a why that a retry should be scheduled, like an error that is
     * a result of a network connectivity issue or similar. In other words, there is nothing in the job itself
     * that is erroneous, only the conditions for when it was executed.
     */
    fun execute(predicate: (U) -> Boolean): JobState<U> = process(JobStatus.TransientFailure, predicate)
    fun execute(msg: String, predicate: (U) -> Boolean): JobState<U> = process(JobStatus.TransientFailure, predicate, msg)

    /**
     *  Use when either outcome is regarded as a successful result. Most common example of this is when a
     *  job has already been processed and is already found written in the idempotence store or similar.
     *  Everything is in its order but the current job should not trigger any further action and resolve
     *  to [JobStatus.Success].
     */
    fun advanceIf(predicate: (U) -> Boolean): JobState<U> = process(JobStatus.Success, predicate)
    fun advanceIf(msg: String, predicate: (U) -> Boolean): JobState<U> = process(JobStatus.Success, predicate, msg)

    /**
     * Use for modelling a side-effect which doesn't have a return status. This
     * function is equivalent to calling require with a function that always returns true.
     */
    fun sideEffect(fn: (U) -> Unit): JobState<U> = execute { fn(it!!); true }

    /**
     * If the [JobState] is non-terminal mark it as a success
     */
    fun success(): JobState<U> = also { it.status = JobStatus.Success }

    /**
     * Use when conducting the final operation on the job. If it successfully done (the predicate returns true)
     * the [JobState] will automatically be set as [JobStatus.Success] and return the [JobStatus] rather than
     * the [JobState] itself, prohibiting that any more work is done on this job through this pipe. The returned
     * [JobStatus] can never be [JobStatus.Incomplete] when returning from this function (unless that was the status
     * prior to this function call).
     * */
    fun end(predicate: (U) -> Boolean): JobStatus {
        if (inProgress()) {
            this.status = when (predicate(value!!)) {
                true -> JobStatus.Success
                false -> JobStatus.TransientFailure
            }
        }
        log.debug { "Ended with status ${this.status.name}" }
        return status
    }

    @JvmName("endNullary")
    fun end(): JobStatus {
        this.status = JobStatus.Success
        log.debug { "Ended with status ${this.status.name}" }
        return status
    }

    fun process(newStatus: JobStatus, predicate: (U) -> Boolean, msg: String? = null): JobState<U> {
        val lastInterceptor = WorkerInterceptor {
            if (inProgress() && !predicate(value!!)) {
                //this.status = newStatus
                msg?.let { log.debug("Failed on: $it") }
                newStatus
            }else{
                this.status
            }

        }

        val firstInterceptor = when(interceptors.size){
            0 -> lastInterceptor
            else -> {
                interceptors.last().next = lastInterceptor
                interceptors.first()
            }
        }

        val endValue = firstInterceptor.onIntercept(firstInterceptor)
        this.status = endValue

        return this
    }

    fun <R: Any> map(transform: (U) -> R): JobState<R> {
        val transFormedVal: R? = when (inProgress()) {
            true -> value?.let(transform)
            false -> null
        }

        val state: JobState<R> = JobState(transFormedVal, interceptors)
        state.status = this.status
        return state
    }
}