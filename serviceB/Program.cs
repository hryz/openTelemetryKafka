using System;
using System.Diagnostics;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Instrumentation.Kafka;
using OpenTelemetry.Trace;

namespace serviceB
{
    class Program
    {
        private const string BrokerAddress = "localhost:9092";
        static async Task Main(string[] args)
        {
            var source = new ActivitySource("Service B");
            
            var otel = Sdk.CreateTracerProviderBuilder()
                .AddSource("Service B")
                .AddKafkaInstrumentation()
                .AddHttpClientInstrumentation()
                .SetSampler(new ParentBasedSampler(new AlwaysOnSampler()))
                .AddZipkinExporter(x =>
                {
                    x.Endpoint = new Uri("http://localhost:9411/api/v2/spans");
                    x.ServiceName = "Service B";
                })
                //.AddJaegerExporter()
                .AddConsoleExporter()
                .Build();
            
            var producer = new ProducerBuilder<int, string>(
                    new ProducerConfig {BootstrapServers = BrokerAddress})
                .Build()
                .Instrument();

            var consumer = new ConsumerBuilder<int, string>(
                    new ConsumerConfig {BootstrapServers = BrokerAddress, GroupId = "service-b"})
                .Build()
                .Instrument();

            var channel = Channel.CreateBounded<(ConsumeResult<int, string>, Activity)>(3);
            Task.Run(async () =>
            {
                await foreach (var (msg, activity) in channel.Reader.ReadAllAsync())
                {
                    Activity.Current = activity; //restore the activity on the background thread
                    Activity.Current?.AddEvent(new ActivityEvent("dequeued"));
                    using (var subActivity = source.StartActivity("heavy computation"))
                    {
                        await Task.Delay(1000);
                    }
                    await Task.Delay(100);
                    producer.Produce("BtoC", new Message<int, string>()
                    {
                        Key = msg.Message.Key,
                        Value = msg.Message.Value + "(B)"
                    });
                    Activity.Current?.Stop();
                }
            });
            
            consumer.Subscribe("AtoB");
            while (true)
            {
                var msg = consumer.Consume();
                await Task.Delay(200);
                await channel.Writer.WriteAsync((msg, Activity.Current));
                Activity.Current?.AddEvent(new ActivityEvent("enqueued"));
            }
        }
    }
}
