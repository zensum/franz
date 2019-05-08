package franz

typealias InterceptorStage = suspend(WorkerInterceptor, JobStatus, JobState<Any>) -> JobStatus

open class WorkerInterceptor(
    var next: WorkerInterceptor? = null,
    val onIntercept: InterceptorStage = { interceptor, default, jobState ->
        interceptor.executeNext(default, jobState)
    }
){
    suspend fun executeNext(default: JobStatus, jobState: JobState<Any>): JobStatus {
        val nextInterceptor = next
        return nextInterceptor?.onIntercept?.invoke(nextInterceptor, default, jobState) ?: JobStatus.Success
    }
}