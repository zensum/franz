# franz

Franz is a library for building Kafka-based workers in Kotlin

```kotlin
import franz.WorkerBuilder

fun main(args: Array<String>) {
    WorkerBuilder()
            .subscribedTo("my-topic")
            .groupId("test")
            .parallelism(1)
            .running {
                println("I got a message with key $key containing $value")
                success
            }
            .start()
```

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
