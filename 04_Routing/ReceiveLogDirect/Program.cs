using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReceiveLogDirect
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using(var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // Direcrt exchange.
                // A message goes to the queues whose binding key exactly matches
                // the routing key of the message.
                channel.ExchangeDeclare(
                    exchange: "direct_logs",
                    ExchangeType.Direct);           // <-- this

                var queueName = channel.QueueDeclare().QueueName;

                if (args.Length < 1)
                {
                    Console.Error.WriteLine(
                        $"Usage: {Environment.GetCommandLineArgs()[0]} [info] [warning] [error]");
                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                    Environment.ExitCode = 1;
                    return;
                }

                foreach (var severity in args)
                {
                    channel.QueueBind(
                        queue: queueName,
                        exchange: "direct_logs",
                        routingKey: severity);      // <-- this
                }

                Console.WriteLine(" [*] Wainting for messages.");

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
