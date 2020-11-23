using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using OpenTelemetry.Trace;

namespace OpenTelemetry.Instrumentation.Kafka
{
    internal static class KafkaActivitySource
    {
        public static readonly ActivitySource Source = new ActivitySource(Name, Version);
        public const string HeaderName = "traceparent";
        public const string Name = "OpenTelemetry.Instrumentation.Kafka";
        public const string Version = "1.0.0";
    }

    public static class OpenTelemetryExtensions
    {
        public static IConsumer<K, V> Instrument<K, V>(this IConsumer<K, V> it) => 
            new TrackableConsumer<K, V>(it);

        public static IProducer<K, V> Instrument<K, V>(this IProducer<K, V> it) =>
            new TrackableProducer<K, V>(it);

        public static TracerProviderBuilder AddKafkaInstrumentation(this TracerProviderBuilder builder) =>
            builder.AddSource(KafkaActivitySource.Name);
    }
    
    internal class TrackableConsumer<K, V> : IConsumer<K, V>
    {
        private readonly IConsumer<K, V> _inner;
        public TrackableConsumer(IConsumer<K, V> inner) => _inner = inner;

        static ConsumeResult<K, V> HandleResult(ConsumeResult<K, V> result)
        {
            if (result == null) return null;
            if (result.Message == null) return result;

            var parent = result.Message.Headers.TryGetLastBytes(KafkaActivitySource.HeaderName, out var header)
                ? Encoding.UTF8.GetString(header)
                : null;

            var tags = new Dictionary<string, object>
            {
                {"topic", result.Topic},
                {"partition", result.Partition.Value}
            };
            
            var activity = KafkaActivitySource.Source.StartActivity(
                "Processing Kafka Message", ActivityKind.Consumer, parent!, tags);
            activity?.AddEvent(new ActivityEvent("Consumed"));

            Activity.Current = activity;
            return result;
        }

        public ConsumeResult<K, V> Consume(int millisecondsTimeout) =>
            HandleResult(_inner.Consume(millisecondsTimeout));

        public ConsumeResult<K, V> Consume(CancellationToken cancellationToken = new CancellationToken()) =>
            HandleResult(_inner.Consume(cancellationToken));

        public ConsumeResult<K, V> Consume(TimeSpan timeout) =>
            HandleResult(_inner.Consume(timeout));

        #region Simple Delegation
        
        public void Dispose() => _inner.Dispose();
        public int AddBrokers(string brokers) => _inner.AddBrokers(brokers);
        public Handle Handle => _inner.Handle;
        public string Name => _inner.Name;
        public void Subscribe(IEnumerable<string> topics) => _inner.Subscribe(topics);
        public void Subscribe(string topic) => _inner.Subscribe(topic);
        public void Unsubscribe() => _inner.Unsubscribe();
        public void Assign(TopicPartition partition) => _inner.Assign(partition);
        public void Assign(TopicPartitionOffset partition) => _inner.Assign(partition);
        public void Assign(IEnumerable<TopicPartitionOffset> partitions) => _inner.Assign(partitions);
        public void Assign(IEnumerable<TopicPartition> partitions) => _inner.Assign(partitions);
        public void Unassign() => _inner.Unassign();
        public void StoreOffset(ConsumeResult<K, V> result) => _inner.StoreOffset(result);
        public void StoreOffset(TopicPartitionOffset offset) => _inner.StoreOffset(offset);
        public List<TopicPartitionOffset> Commit() => _inner.Commit();
        public void Commit(IEnumerable<TopicPartitionOffset> offsets) => _inner.Commit(offsets);
        public void Commit(ConsumeResult<K, V> result) => _inner.Commit(result);
        public void Seek(TopicPartitionOffset tpo) => _inner.Seek(tpo);
        public void Pause(IEnumerable<TopicPartition> partitions) => _inner.Pause(partitions);
        public void Resume(IEnumerable<TopicPartition> partitions) => _inner.Resume(partitions);
        public List<TopicPartitionOffset> Committed(TimeSpan timeout) => _inner.Committed(timeout);
        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) => _inner.Committed(partitions, timeout);
        public Offset Position(TopicPartition partition) => _inner.Position(partition);
        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout) => _inner.OffsetsForTimes(timestampsToSearch, timeout);
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) => _inner.GetWatermarkOffsets(topicPartition);
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => _inner.QueryWatermarkOffsets(topicPartition, timeout);
        public void Close() => _inner.Close();
        public string MemberId => _inner.MemberId;
        public List<TopicPartition> Assignment => _inner.Assignment;
        public List<string> Subscription => _inner.Subscription;
        public IConsumerGroupMetadata ConsumerGroupMetadata => _inner.ConsumerGroupMetadata;
        
        #endregion
    }
    
    public class TrackableProducer<K,V> : IProducer<K,V>
    {
        private readonly IProducer<K, V> _inner;
        public TrackableProducer(IProducer<K, V> inner) => _inner = inner;

        private static Message<K, V> InjectHeader(Message<K, V> msg)
        {
            if (msg == null)
                return null;

            if (Activity.Current == null || Activity.Current.Id == null)
                return msg;

            msg.Headers ??= new Headers();
            var header = Encoding.UTF8.GetBytes(Activity.Current.Id);
            msg.Headers.Add(KafkaActivitySource.HeaderName, header);
            Activity.Current.AddEvent(new ActivityEvent("Produced"));
            return msg;
        } 
        
        public Task<DeliveryResult<K, V>> ProduceAsync(string topic, Message<K, V> message, CancellationToken cancellationToken = new CancellationToken()) => 
            _inner.ProduceAsync(topic, InjectHeader(message), cancellationToken);

        public Task<DeliveryResult<K, V>> ProduceAsync(TopicPartition topicPartition, Message<K, V> message, CancellationToken cancellationToken = new CancellationToken()) =>
            _inner.ProduceAsync(topicPartition, InjectHeader(message), cancellationToken);

        public void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null) => 
            _inner.Produce(topic, InjectHeader(message), deliveryHandler);

        public void Produce(TopicPartition topicPartition, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null) => 
            _inner.Produce(topicPartition, InjectHeader(message), deliveryHandler);

        #region Simple Delegation
        
        public void Dispose() => _inner.Dispose();
        public int AddBrokers(string brokers) => _inner.AddBrokers(brokers);
        public Handle Handle => _inner.Handle;
        public string Name => _inner.Name;
        public int Poll(TimeSpan timeout) => _inner.Poll(timeout);
        public int Flush(TimeSpan timeout) => _inner.Flush(timeout);
        public void Flush(CancellationToken cancellationToken = new CancellationToken()) => _inner.Flush(cancellationToken);
        public void InitTransactions(TimeSpan timeout) => _inner.InitTransactions(timeout);
        public void BeginTransaction() => _inner.BeginTransaction();
        public void CommitTransaction(TimeSpan timeout) => _inner.CommitTransaction(timeout);
        public void AbortTransaction(TimeSpan timeout) => _inner.AbortTransaction(timeout);
        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) => 
            _inner.SendOffsetsToTransaction(offsets, groupMetadata, timeout);
        
        #endregion
    }
}
