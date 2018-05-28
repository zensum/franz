package franz

class WorkerInterceptor(
    var next: WorkerInterceptor? = null,
    val onIntercept: ((WorkerInterceptor) -> JobStatus) = {
        it.executeNext()
    }
){
    fun executeNext(): JobStatus {
        val nextInterceptor = next
        if(nextInterceptor != null){
            return nextInterceptor.onIntercept(nextInterceptor)
        }else{
            return JobStatus.Success
        }
    }
}