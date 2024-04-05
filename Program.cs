namespace KafkaConsumerApp
{
    using System;
    using Confluent.Kafka;

    public class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", // Replace with your Kafka broker(s) address
                GroupId = "demo-topic-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("demo-topic");

                Console.WriteLine("Consumer application started. Press Ctrl+C to exit.");

                while (true)
                {
                    try
                    {
                        var message = consumer.Consume();
                        Console.WriteLine($"Received message: {message.Value}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occurred: {e.Error.Reason}");
                    }
                }
            }
        }
    }

}