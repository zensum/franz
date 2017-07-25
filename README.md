# franz

Franz is a library for building Kafka-based workers in Kotlin.

```kotlin
import franz.WorkerBuilder

fun main(args: Array<String>) {
    WorkerBuilder.ofByteArray
            .subscribedTo("my-topic")
            .groupId("test")
            .running {
                println("I got a message with key $key containing $value")
                success
            }
            .start()
}
```

`franz` keeps track of what offsets are safe to commit and locally schedules retries for tasks that fail. This makes building a Kafka-based worker as simple as building one on top of a traditional message queue.

## Getting started

First add jitpack.io to your dependencies

``` gradle
maven { url 'https://jitpack.io' }
```

And then add a dependency

``` gradle
dependencies {
            compile 'com.github.zensum:franz:-SNAPSHOT'
}
```
