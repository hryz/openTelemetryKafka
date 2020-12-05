# OpenTelemetry instrumentation Kafka 

Distributed tracing is super important for modern architectures. 
They are essential part of observability (along with logs and metrics)

Distributed tracing is based on a simple concept of tracking 
the cause-effect relationship during the communication of services. 
When a service receives a request it extracts the `TraceId` and `SpanId` 
from its context, creates a new `SpanId` and sets its parent equal to 
the received `SpanId` and passes this pair to all outgoing requests.  
Therefore, one can see the communication graph: all the chains of the requests,
timings of each of them, some extra information (tags, events, etc.)

In the .Net world the HTTP communications are covered, 
gRPC and RabbitMQ are also covered, but Apache Kafka isn't. 
If you use Kafka and want to pass the tracing context you are on your own. 
This sample project illustrates a possible way how to integrate Kafka with 
the rest of the .Net OpenTelemetry technologies.  

## Stream Processors also need tracing!
If your services are not RPC/REST but rather stream processors 
they need Distributed Tracing nevertheless. Having it you can see how 
a single message produced to one topic is consumed by many services and how 
they produced messages in other topics as a reaction to your message and so on. 
Just like with HTTP/RPC with Kafka you can see the full graph of service interaction 
and possible deviations (eg multiple paths from 1 service to another that 
result in duplicating processing of a single update). 
Also, having this graph you can see the timing of each processing stage 
(along with internal events).  

## Design
The lib is super simple. It acts as the decorator for Kafka Consumer and Producer. 
It reads the message headers, starts the Consume activity and set the `Activity.Current` 
Then application code processed the message. If the application code does some HTTP/RPC calls
the context will be passed to them. Also, the application can record events, 
create a child Spans for long computations, add tags, an so on. 
When the processing is completed the application produces messages to other topics 
to update the downstream consumers. During the producing the context will be obtained 
from the `Activity.Current` and passed downstream. If the application uses 
some async operations it's OK. `Activity.Current` is Async.Local value and it will
be captured as part of the execution context and restored on a different thread. 
However, if the app uses some queues/background workers/partitioning strategies it must 
manually capture the current activity and pass as an extra parameter to the queue.

## Misc
.Net Activity API is less intuitive than the vendor's one (Zipkin) but it 
integrates nicely with other parts of the OpenTelemetry standard: 
other instrumentation libs, exporters, etc.  
In order to run the sample you need a local Zipkin/Jaeger:
```text
docker run -d -p 9411:9411 openzipkin/zipkin
```
