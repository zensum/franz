package franz

typealias InterceptorStage = suspend(WorkerInterceptor, JobStatus) -> JobStatus

open class WorkerInterceptor(
    var jobState: JobState<Any>? = null,
    var next: WorkerInterceptor? = null,
    val onIntercept: InterceptorStage = { interceptor, default ->
        interceptor.executeNext(default)
    }
){
    suspend fun executeNext(default: JobStatus): JobStatus {
        val nextInterceptor = next
        return nextInterceptor?.onIntercept?.invoke(nextInterceptor, default) ?: JobStatus.Success
    }

    fun clone() =
        WorkerInterceptor(
            onIntercept = onIntercept
        )
}