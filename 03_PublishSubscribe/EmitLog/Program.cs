using System;
using System.Text;
using RabbitMQ.Client;

namespace EmitLog
{
    /// <summary>
    /// Subscriber (consumer).
    /// A consumer is a user application that receives messages.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // The fanout exchange broadcasts all the messages to all the queues it knows.
                channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);

                var message = GetMessage(args);
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(
                    exchange: "logs",             // set exchange
                    routingKey: string.Empty,     // routingKey is empty
                    basicProperties: null,
                    body: body);

                Console.WriteLine($" [x] Sent {message}");
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static string GetMessage(string[] args)
        {
            return args.Length > 0
                ? string.Join(" ", args)
                : "info: Hello World!";
        }
    }
}
