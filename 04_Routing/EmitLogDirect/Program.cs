using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

namespace EmitLogDirect
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

                var severity = args.Length > 0
                    ? args[0]
                    : "info";
                var message = args.Length > 1
                    ? string.Join(" ", args.Skip(1).ToArray())
                    : "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(
                    exchange: "direct_logs",
                    routingKey: severity,           // <-- this
                    basicProperties: null,
                    body: body);

                Console.WriteLine($" [x] Sent '{severity}':'{message}'");
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
