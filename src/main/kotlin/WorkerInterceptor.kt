package franz

class WorkerInterceptor(
    var next: WorkerInterceptor? = null,
    inline val onIntercept: ((WorkerInterceptor) -> JobStatus) = {
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

    inline fun <U> executeFinal(fn: (U) -> Boolean, value: U) = fn(value)
}