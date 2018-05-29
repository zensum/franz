package franz

class WorkerInterceptor(
    var next: WorkerInterceptor? = null,
    val onIntercept: (suspend (WorkerInterceptor) -> JobStatus) = {
        it.executeNext()
    }
){
    suspend fun executeNext(): JobStatus {
        val nextInterceptor = next
        if(nextInterceptor != null){
            return nextInterceptor.onIntercept(nextInterceptor)
        }else{
            return JobStatus.Success
        }
    }
}