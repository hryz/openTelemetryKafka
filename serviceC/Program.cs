using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Instrumentation.Kafka;
using OpenTelemetry.Trace;

namespace serviceC
{
    class Program
    {
        private const string BrokerAddress = "localhost:9092";
        static async Task Main(string[] args)
        {
            var otel = Sdk.CreateTracerProviderBuilder()
                .AddKafkaInstrumentation()
                .AddHttpClientInstrumentation()
                .SetSampler(new ParentBasedSampler(new AlwaysOnSampler()))
                .AddZipkinExporter(x =>
                {
                    x.Endpoint = new Uri("http://localhost:9411/api/v2/spans");
                    x.ServiceName = "Service C";
                })
                //.AddJaegerExporter()
                .AddConsoleExporter()
                .Build();
            
            var producer = new ProducerBuilder<int, string>(
                    new ProducerConfig {BootstrapServers = BrokerAddress})
                .Build()
                .Instrument();

            var consumer = new ConsumerBuilder<int, string>(
                    new ConsumerConfig {BootstrapServers = BrokerAddress, GroupId = "service-c"})
                .Build()
                .Instrument();

            consumer.Subscribe("BtoC");
            var http = new HttpClient();
            while (true)
            {
                var msg = consumer.Consume();
                await Task.Delay(1000);
                await http.GetAsync("http://example.com");
                
                producer.Produce("CtoD", new Message<int, string>()
                {
                    Key = msg.Message.Key,
                    Value = msg.Message.Value + "(C)"
                });
                Activity.Current?.Stop();
            }
        }
    }
}
