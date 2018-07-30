package franz

open class WorkerInterceptor(
    var next: WorkerInterceptor? = null,
    val onIntercept: (suspend (interceptor: WorkerInterceptor, default: JobStatus) -> JobStatus) = { interceptor, default ->
        interceptor.executeNext(default)
    }
){
    suspend fun executeNext(default: JobStatus): JobStatus {
        val nextInterceptor = next
        if(nextInterceptor != null){
            return nextInterceptor.onIntercept(nextInterceptor, default)
        }else{
            return JobStatus.Success
        }
    }
}