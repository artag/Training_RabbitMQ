using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReceiveLogsTopic
{
    // To receive all the logs:                         dotnet run "#"
    // To receive all logs from the facility "kern":    dotnet run "kern.*"
    // To receive only about "critical" logs:           dotnet run "*.critical"
    // Multiple bindings:                               dotnet run "kern.*" "*.critical"
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // Topic exchange
                // Messages sent to a topic exchange can't have an arbitrary routing_key -
                // it must be a list of words (list length max 255 bytes), delimited by dots.
                channel.ExchangeDeclare(
                    exchange: "topic_logs",
                    ExchangeType.Topic);            // <-- this

                // * (star) can substitute for exactly one word.
                // # (hash) can substitute for zero or more words.

                var queueName = channel.QueueDeclare().QueueName;

                if (args.Length < 1)
                {
                    Console.Error.WriteLine($"Usage: {Environment.GetCommandLineArgs()[0]} [binding_key...]");
                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                    Environment.ExitCode = 1;
                    return;
                }

                foreach (var bindingKey in args)
                {
                    channel.QueueBind(
                        queue: queueName,
                        exchange: "topic_logs",
                        routingKey: bindingKey);
                }

                Console.WriteLine($" [*] Waiting for messages. To exit press CTRL+C");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, eventArgs) =>
                {
                    var body = eventArgs.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = eventArgs.RoutingKey;
                    Console.WriteLine($" [x] Received '{routingKey}':'{message}'");
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
