package franz

import mu.KotlinLogging

val log = KotlinLogging.logger("job")
@Deprecated("Use WorkerBuilder.pipedHandler instead")
fun <T, U: Any> JobDSL<T, U>.asPipe(): JobState<U> = JobState(this.value)

private val FAILED_JOB_STATUSES = listOf(WorkerResult.Failure, WorkerResult.Retry)

class JobState<U: Any> constructor(val value: U?, val interceptors: List<WorkerInterceptor> = emptyList()) {
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
    suspend fun require(predicate: suspend (U) -> Boolean): JobState<U> = processPredicate(JobStatus.PermanentFailure,  predicate)
    suspend fun require(msg: String, predicate: suspend (U) -> Boolean): JobState<U> = processPredicate(JobStatus.PermanentFailure, predicate, msg)

    /**
     * Use when an operation may fail in such a why that a retry should be scheduled, like an error that is
     * a result of a network connectivity issue or similar. In other words, there is nothing in the job itself
     * that is erroneous, only the conditions for when it was executed.
     */
    suspend fun execute(predicate: suspend (U) -> Boolean): JobState<U> = processPredicate(JobStatus.TransientFailure, predicate)
    suspend fun execute(msg: String, predicate: suspend (U) -> Boolean): JobState<U> = processPredicate(JobStatus.TransientFailure, predicate, msg)

    /**
     * Use when an operation can result in either success, permanent failure or transient failure. This is quite common
     * if an external resource is queried and the response may be successfully or trigger either a permanent, transient failure.
     */
    suspend fun executeToResult(fn: suspend (U) -> WorkerResult) = processWorkerFunction(fn)
    suspend fun executeToResult(msg: String, fn: suspend (U) -> WorkerResult) = processWorkerFunction(fn, msg)

    /**
     *  Use when either outcome is regarded as a successful result. Most common example of this is when a
     *  job has already been processed and is already found written in the idempotence store or similar.
     *  Everything is in its order but the current job should not trigger any further action and resolve
     *  to [JobStatus.Success].
     */
    suspend fun advanceIf(predicate: suspend (U) -> Boolean): JobState<U> = processPredicate(JobStatus.Success, predicate)
    suspend fun advanceIf(msg: String, predicate: suspend (U) -> Boolean): JobState<U> = processPredicate(JobStatus.Success, predicate, msg)

    /**
     * Use for modelling a side-effect which doesn't have a return status. This
     * function is equivalent to calling require with a function that always returns true.
     */
    suspend fun sideEffect(fn: suspend (U) -> Unit): JobState<U> = execute { fn(it); true }

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
    suspend fun end(predicate: suspend (U) -> Boolean): JobStatus {
        if (inProgress()) {
            this.status = when (predicate(value!!)) {
                true -> JobStatus.Success
                false -> JobStatus.TransientFailure
            }
        }
        log.info { "Ended with status ${this.status.name}" }
        return status
    }

    @JvmName("endNullary")
    fun end(): JobStatus {
        this.status = JobStatus.Success
        log.info { "Ended with status ${this.status.name}" }
        return status
    }

    /**
     * Transforms the type of the job by using the supplied transform function.
     * If an unhandled exception is encountered, sets the JobState to TransientFailure.
     */
    inline fun <R: Any> map(transform: (U) -> R): JobState<R>  = processMap(JobStatus.TransientFailure, transform)
    inline fun <R: Any> map(msg: String, transform: (U) -> R): JobState<R>  = processMap(JobStatus.TransientFailure, transform, msg)

    /**
     * Transforms the type of the job by using the supplied transform function.
     * If an unhandled exception is encountered, sets the JobState to PermanentFailure.
     */
    inline fun <R: Any> mapRequire(transform: (U) -> R): JobState<R> = processMap(JobStatus.PermanentFailure, transform)
    inline fun <R: Any> mapRequire(msg: String, transform: (U) -> R): JobState<R> = processMap(JobStatus.PermanentFailure, transform, msg)

    @PublishedApi
    internal inline fun <R: Any> processMap(newStatus: JobStatus, transform: (U) -> R, msg: String? = null): JobState<R>{
        try{
            val transFormedVal: R? = when (inProgress()) {
                true -> transform(value!!)
                false -> null
            }
            return JobState(transFormedVal, this.interceptors).also { it.status = this.status }
        }catch(e: Exception) {
            msg?.let { log.info { "Failed on: $it" } }
            return JobState<R>(null, interceptors).also { it.status = newStatus }
        }
    }

    private suspend fun processWorkerFunction(fn: suspend(U) -> WorkerResult, msg: String? = null): JobState<U> {
        val lastInterceptor = WorkerInterceptor {
            if(inProgress()) {
                val result = fn(value!!)
                if(FAILED_JOB_STATUSES.contains(result)){
                    msg?.let {log.info { "Failed on: $it" }}
                }
                result.toJobStatus()
            }else{
                this.status
            }
        }

        return process(lastInterceptor)
    }

    private suspend fun processPredicate(newStatus: JobStatus, predicate: suspend (U) -> Boolean, msg: String? = null): JobState<U> {
        val lastInterceptor = WorkerInterceptor {
            if (inProgress() && !predicate(value!!)) {
                msg?.let { log.info("Failed on: $it") }
                newStatus
            }else{
                this.status
            }
        }

        return process(lastInterceptor)

    }

    private suspend fun process(lastInterceptor: WorkerInterceptor): JobState<U>{
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

    private fun WorkerResult.toJobStatus() =
        when(this){
            WorkerResult.Success -> JobStatus.Incomplete
            WorkerResult.Retry -> JobStatus.TransientFailure
            WorkerResult.Failure -> JobStatus.PermanentFailure
        }
}