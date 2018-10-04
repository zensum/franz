# franz

Franz is a library for building Kafka-based workers in Kotlin.
The purpose is the get messages from the message queue in a stream-like fashion and work with them step-by-step with the
use of worker functions where the end result for a message is either successful, a permanent failure or a transient failure.
Successful messages are what they sound like. The message is handled. Permanent failure messages are regarded as forever failed.
They failed in some fashion that is not recoverable and therefore nothing can be gained by trying it again. Transient failures are
retried by being fetched again the next time messages are fetched from the message queue and then run trough the the piped worker again.


```kotlin
import franz.WorkerBuilder
import franz.JobStatus

fun main(args: Array<String>) {
    WorkerBuilder.ofByteArray
            .subscribedTo("my-topic")
            .groupId("test")
            .handlePiped {
                it
                    .sideEffect { println("I got a message with key ${it.key} containing ${it.value}") }
                    .end()
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

[![](https://jitpack.io/v/zensum/franz.svg)](https://jitpack.io/#zensum/franz)

## Building worker functions
Franz is used by creating worker steps in the form of worker functions in its piped handler.
These are called sequentially.
```
WorkerBuilder.ofByteArray
            .subscribedTo("my-topic")
            .groupId("test")
            .handlePiped {
                it
                    .mapRequire { [parse the message in some form] }
                    .execute{ [Do something with the transformed value] }
                    .execute{ [Do something else ] }
                    .require{ [Do some third thing] }
                    .sideEffect { [Do something where the result don't matter }
                    .onPermanentFailure { [Run if the message status is permanent failure ] }
                    .end()
            }
```

### Available worker functions
Called from a JobState (after WorkerBuilder.handledPiped(fn: (JobState<Message<T, U>>) -> JobStatus))
* `.map(transform: transform<T,U>)` takes a transformation function, applies it to the message and sends the result down the pipe. If the function throws the message's status becomes a transient failure.
* `.mapRequire(transform: transform<T,U>)` Same as map() but results is a permanent failure if the function throws.
* `.execute(predicate: Predicate<U>)` runs a predicate function of the message. If the result is false the message's status becomes a transient failure.
* `.require(predicate: Predicate<U>)` same and execute but becomes a permanent failure 
* `.executeToResult(fn: suspend (U) -> WorkerResult)` takes a worker function returning a `WorkerResult` enum value and applies it to the message. The message status afterwards depends on the result. Useful when the worker function can either e successful, want to retry the message or fail the message.
* `.executeToEither(fn: suspend (U) -> WorkerResult)` Combination of `map` and `excuteToResult` worker function. Returns an `Either` object containing either a mapped value or a `WorkerResult` enum value. Has helper functions in `Either` class for instanciation.
* `.advanceIf(predicate: Predicate<U>)` Only continues down the pipe if the predicate results in true. Is otherwise considered a success.
* `.branchIf(predicate: Predicate<U>)` Creates a new piped worker branch if the predicate is true
* `.sideEffect(fn: (U) -> Unit)` applies the worker function on the message but does not care about the result
* `.onTransientFailure((U) -> Unit)` runs this worker function only when the message's status is a transient failure. Can't change the status of the message.
* `.onPermanentfailure((U) -> Unit)` same as onTransientFailure bit runs when the messages statis is a permanent failure.
* `.end()` ends the worker pipe and returns the messages status as its result. Any non failed statuses returns in a success.

## Interceptors
Franz supports the use of interceptor; that is, tiny pieces of code inserted before a worker function runs. They can capture the output from the workers and send its own result.
The standard WorkerIntercptor can be used as-is and takes a lamba expression in executes for each worker function encountered.
Worker function lambas expects the following type `suspend (interceptor: WorkerInterceptor, default: JobStatus) -> JobStatus`
### Example
```
WorkerBuilder.ofByteArray
            .subscribedTo("my-topic")
            .groupId("test")
            .install(WorkerInterceptor { i, d -> execute.next(d).also { println("Job returned $it})
            .handlePiped {
                it
                    .execute(true)
                    .end()
            }
            .start()

```
In this example the interceptor simply prints executes the next worker function and outputs the result of it