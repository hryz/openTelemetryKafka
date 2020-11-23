using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Instrumentation.Kafka;
using OpenTelemetry.Trace;

namespace serviceA
{
    class Program
    {
        private const string BrokerAddress = "localhost:9092";
        static async Task Main(string[] args)
        {
            var source = new ActivitySource("Service A");
            Activity.DefaultIdFormat = ActivityIdFormat.W3C;
            Activity.ForceDefaultIdFormat = true;
            
            var otel = Sdk.CreateTracerProviderBuilder()
                .AddSource("Service A") //we're interested also in spans started here from nothing
                .AddKafkaInstrumentation()
                .AddHttpClientInstrumentation()
                .SetSampler(new ParentBasedSampler(new AlwaysOnSampler()))
                .AddZipkinExporter(x =>
                {
                    x.Endpoint = new Uri("http://localhost:9411/api/v2/spans");
                    x.ServiceName = "Service A";
                })
                //.AddJaegerExporter()
                .AddConsoleExporter()
                .Build();
            
            var producer = new ProducerBuilder<int, string>(
                    new ProducerConfig {BootstrapServers = BrokerAddress})
                .Build()
                .Instrument();

            for (int i = 0; i < 100; i++)
            {
                var act = source.StartActivity("scheduled computation");
                Activity.Current = act;
                await Task.Delay(1000);
                producer.Produce("AtoB", new Message<int, string>
                {
                    Key = i,
                    Value = "message " + i
                });
                Activity.Current?.Stop();
            }
        }
    }
}
