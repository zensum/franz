package franz

fun main(args: Array<String>) {
    WorkerBuilder()
            .subscribedTo("my-topic")
            .parallelism(1)
            .option("my-option", "some-value")
            .running {
                println("Do someshit with $key and $value")
                if (value == "crazy") {
                    permanentFailure(RuntimeException("yeah"))
                } else {
                    success
                }
            }
            .start()
}
