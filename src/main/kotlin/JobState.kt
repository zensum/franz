package franz

import mu.KotlinLogging

typealias Predicate<U> = suspend (U) -> Boolean
typealias WorkerFunction<U> = suspend (JobState<U>) -> JobStatus
typealias SideEffect<U> = suspend (U) -> Unit
typealias Transform<U, R> = suspend (U) -> R

val log = KotlinLogging.logger("job")
@Deprecated("Use WorkerBuilder.pipedHandler instead")
fun <T, U: Any> JobDSL<T, U>.asPipe(): JobState<U> = JobState(this.value)

class JobStateException(
    val result: JobStatus,
    message: String,
    innerException: Throwable
):Exception(message, innerException)

private val FAILED_JOB_STATUS = listOf(WorkerStatus.Failure, WorkerStatus.Retry)

private fun WorkerStatus.toJobStatus() = when(this){
    WorkerStatus.Success -> JobStatus.Incomplete
    WorkerStatus.Failure -> JobStatus.PermanentFailure
    WorkerStatus.Retry -> JobStatus.TransientFailure
}

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
    suspend fun require(predicate: Predicate<U>): JobState<U> = processPredicate(JobStatus.PermanentFailure,  predicate)
    suspend fun require(msg: String, predicate: Predicate<U>): JobState<U> = processPredicate(JobStatus.PermanentFailure, predicate, msg)

    /**
     * Use when an operation may fail in such a why that a retry should be scheduled, like an error that is
     * a result of a network connectivity issue or similar. In other words, there is nothing in the job itself
     * that is erroneous, only the conditions for when it was executed.
     */
    suspend fun execute(predicate: Predicate<U>): JobState<U> = processPredicate(JobStatus.TransientFailure, predicate)
    suspend fun execute(msg: String, predicate: Predicate<U>): JobState<U> = processPredicate(JobStatus.TransientFailure, predicate, msg)

    /**
     * Use when an operation can result in successfully mapped value or a failure in the form of a  permanent failure or transient failure. This is quite common
     * if an external resource is queried and the response may be successfully and we want to use that value further down the pipe or trigger either a permanent, transient failure.
     */
    suspend fun <R: Any> executeToResult(fn: suspend (U) -> WorkerResult<R>) = processToWorkerResult(fn)
    suspend fun <R: Any> executeToResult(msg: String, fn: suspend (U) -> WorkerResult<R>) = processToWorkerResult(fn, msg)

    /**
     * Use when an operation can result in either success, permanent failure or transient failure. This works like executeToResult but won't want the data but only flag as result as successfull
     */
    suspend fun executeToStatus(fn: suspend (U) -> WorkerStatus) = processToWorkerStatus(fn)
    suspend fun executeToStatus(msg: String, fn: suspend (U) -> WorkerStatus) = processToWorkerStatus(fn, msg)

    /**
     *  Use when either outcome is regarded as a successful result. Most common example of this is when a
     *  job has already been processed and is already found written in the idempotence store or similar.
     *  Everything is in its order but the current job should not trigger any further action and resolve
     *  to [JobStatus.Success].
     */
    suspend fun advanceIf(predicate: Predicate<U>): JobState<U> = processPredicate(JobStatus.Success, predicate)
    suspend fun advanceIf(msg: String, predicate: Predicate<U>): JobState<U> = processPredicate(JobStatus.Success, predicate, msg)

    /**
     * Use this when you want to branch of the execution of the worker. When the predicate evaluates to true, a new jobState
     * worker is created and executed to a JobStatus (by using either end() or jobStatus()).
     */
    suspend fun branchIf(predicate: Boolean, fn: WorkerFunction<U>) = processBranch( { predicate }, fn)
    suspend fun branchIf(predicate: Predicate<U>, fn: WorkerFunction<U>) = processBranch(predicate, fn)
    suspend fun branchIf(msg: String, predicate: Boolean, fn: WorkerFunction<U>) = processBranch( { predicate }, fn, msg)
    suspend fun branchIf(msg: String, predicate: Predicate<U>, fn: WorkerFunction<U>) = processBranch(predicate, fn, msg)

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
    suspend fun end(predicate: Predicate<U>) = processEnd(predicate)
    suspend fun end(msg: String, predicate: Predicate<U>) = processEnd(predicate, msg)
    suspend fun end() = processEnd({true})

    /**
     * Exposes the current job status of the jobstate. Useful in branches when we don't want to call end to end execution.
     */
    fun jobStatus() = this.status

    /**
     * Transforms the type of the job by using the supplied transform function.
     * If an unhandled exception is encountered, sets the JobState to TransientFailure.
     */
    suspend fun <R: Any> map(transform: Transform<U, R>): JobState<R>  = processMap(JobStatus.TransientFailure, transform)
    suspend fun <R: Any> map(msg: String, transform: Transform<U, R>): JobState<R>  = processMap(JobStatus.TransientFailure, transform, msg)

    /**
     * Transforms the type of the job by using the supplied transform function.
     * If an unhandled exception is encountered, sets the JobState to PermanentFailure.
     */
    suspend fun <R: Any> mapRequire(transform: Transform<U, R>): JobState<R> = processMap(JobStatus.PermanentFailure, transform)
    suspend fun <R: Any> mapRequire(msg: String, transform: Transform<U, R>): JobState<R> = processMap(JobStatus.PermanentFailure, transform, msg)


    /**
     * Runs a side effect function only if the current state of the worker is a transient failure state.
     * Useful for error handling in a piped worker.
     */
    suspend fun onTransientFailure(fn: SideEffect<U>) = processOnFailure( fn, allowedStatuses = listOf(JobStatus.TransientFailure))
    suspend fun onTransientFailure(msg: String, fn: SideEffect<U>) = processOnFailure(fn, allowedStatuses = listOf(JobStatus.TransientFailure), msg = msg)

    /**
     * Runs a side effect function only if the current state of the worker is a permanent failure state.
     * Useful for error handling in a piped worker.
     */
    suspend fun onPermanentFailure(fn: SideEffect<U>) = processOnFailure(fn, allowedStatuses = listOf(JobStatus.PermanentFailure))
    suspend fun onPermanentFailure(msg: String, fn: SideEffect<U>) = processOnFailure(fn, allowedStatuses = listOf(JobStatus.PermanentFailure), msg = msg)

    private suspend fun processEnd(predicate: Predicate<U>, msg:String? = null): JobStatus {
        if (inProgress()) {
            this.status = when (predicate(value!!)) {
                true -> JobStatus.Success
                false -> {
                    msg?.let { log.info { "Failed on: ${it}" }}
                    JobStatus.TransientFailure
                }
            }
        }
        log.info { "Ended with status ${this.status.name}" }
        return status
    }

    private suspend fun <R: Any> processMap(newStatus: JobStatus, transform: Transform<U, R>, msg: String? = null): JobState<R>{
        var tranformedValue: R? = null
        val lastInterceptor = WorkerInterceptor {_, _ ->
            try{
                tranformedValue = when (inProgress()) {
                    true -> value?.let { transform(it) }
                    false -> null
                }
                this.status
            }catch(e: Throwable) {
                msg?.let { log.info { "Failed on: $it" } }
                throw JobStateException(result = newStatus, message = msg?:"Failed on: $msg", innerException = e)
            }
        }

        val status = processToStatus(lastInterceptor, newStatus)
        return JobState(tranformedValue, interceptors).also { it.status = status }
    }

    private suspend fun processBranch(predicate: suspend(U) -> Boolean, fn: suspend (JobState<U>) -> JobStatus, msg: String? = null): JobState<U>{
        val newStatus = when(predicate(value!!)){
            true -> {
                msg?.let{ log.info { "Entering branch: ${it}" } }
                fn(this)
            }
            false -> status
        }

        return JobState(value, interceptors).also { it.status = newStatus }
    }

    private suspend fun processToWorkerStatus(fn: suspend(U) -> WorkerStatus, msg: String? = null): JobState<U> {
        val lastInterceptor = WorkerInterceptor {_, _ ->
            if(inProgress()) {
                val result = fn(value!!)
                if(FAILED_JOB_STATUS.contains(result)){
                    msg?.let {log.info { "Failed on: $it" }}
                }
                result.toJobStatus()
            }else{
                this.status
            }
        }

        return process(lastInterceptor, JobStatus.TransientFailure)
    }

    private suspend fun <R: Any> processToWorkerResult(fn: suspend(U) -> WorkerResult<R>, msg: String? = null): JobState<R> {
        var transformedValue: WorkerResult<R>? = null
        val lastInterceptor = WorkerInterceptor {_, _ ->
            if(inProgress()) {
                val result = try{
                    fn(this.value!!)
                }catch (e: Throwable){
                    msg?.let { log.info { "Failed on: $it" } }
                    throw JobStateException(result = JobStatus.TransientFailure, message = msg?:"Failed on: $msg", innerException = e)
                }
                transformedValue = result           // The result need to be viewed outside this interceptor stage
                result.toJobStatus()
            }else{
                this.status
            }
        }

        val status = processToStatus(lastInterceptor, JobStatus.TransientFailure)
        return JobState(transformedValue?.value, interceptors).also { it.status = status }
    }

    private suspend fun processPredicate(newStatus: JobStatus, predicate: suspend (U) -> Boolean, msg: String? = null): JobState<U> {
        val lastInterceptor = WorkerInterceptor {_, _ ->
            if (inProgress() && !predicate(value!!)) {
                msg?.let { log.info("Failed on: $it") }
                newStatus
            }else{
                this.status
            }
        }

        return process(lastInterceptor, newStatus)

    }

    private suspend fun processOnFailure(fn: SideEffect<U>, msg: String? = null, allowedStatuses: List<JobStatus>): JobState<U>{
        if(allowedStatuses.contains(status)){
            msg?.let { log.info { "Running on failure: ${it}" } }
            fn(value!!)
        }

        return this
    }

    private suspend fun process(lastInterceptor: WorkerInterceptor, defaultStatus: JobStatus): JobState<U>{
        val firstInterceptor = when(interceptors.size){
            0 -> lastInterceptor
            else -> {
                interceptors.last().next = lastInterceptor
                interceptors.first()
            }
        }

        val endValue = firstInterceptor.onIntercept(firstInterceptor, defaultStatus)
        this.status = endValue

        return this
    }

    private suspend fun processToStatus(lastInterceptor: WorkerInterceptor, defaultStatus: JobStatus): JobStatus{
        val firstInterceptor = when(interceptors.size){
            0 -> lastInterceptor
            else -> {
                interceptors.last().next = lastInterceptor
                interceptors.first()
            }
        }

        return firstInterceptor.onIntercept(firstInterceptor, defaultStatus)
    }
}