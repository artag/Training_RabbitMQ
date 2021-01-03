using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReceiveLogs
{
    /// <summary>
    /// Publisher (producer).
    /// A producer is a user application that sends messages.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // The fanout exchange broadcasts all the messages to all the queues it knows.
                channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);

                // We want to hear about all log messages, not just a subset of them.
                // We're also interested only in currently flowing messages not in the old ones.

                // Step 1. Create temporary queue.
                // Create a non-durable, exclusive, autodelete queue with a generated (random) name.
                var queueName = channel.QueueDeclare().QueueName;

                // Step 2. Create binding (relationship between exchange and a queue).
                // From now on the logs exchange will append messages to our queue.
                channel.QueueBind(
                    queue: queueName,
                    exchange: "logs",           // set exchange
                    routingKey: string.Empty);  // routingKey is empty

                Console.WriteLine(" [*] Waiting for logs.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, eventArgs) =>
                {
                    var body = eventArgs.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($" [x] {message}");
                };

                channel.BasicConsume(
                    queue: queueName,
                    autoAck: true,
                    consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
