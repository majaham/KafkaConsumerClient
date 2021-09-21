using Confluent.Kafka;
using System;
using System.Threading;

namespace KafkaConsumerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "127.0.0.1:9092,localhost:9092",
                //SslCaLocation = "/PathTO/cluster-ca-certificate.pem",
                //SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.ScramSha256,
                SaslUsername = "ickafka",
                SaslPassword = "493f586cc469a59987c8a9148669b9ecc570bb031bf3fa639e894be185331dce",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            
            using (var c = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                c.Subscribe("items-topic");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);

                            Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occurred: {e.Error.Reason}");

                        }
                    }
                }
                catch (OperationCanceledException ex)
                {
                   
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}
