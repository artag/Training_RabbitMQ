using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

namespace EmitLogTopic
{
    // To emit with a routing key "kern.critical" type:
    //    dotnet run "kern.critical" "A critical kernel error"
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

                var routingKey = args.Length > 0
                    ? args[0]
                    : "anonymous.info";
                var message = args.Length > 1
                    ? string.Join(" ", args.Skip(1).ToArray())
                    : "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(
                    exchange: "topic_logs",
                    routingKey: routingKey,         // <-- this
                    basicProperties: null,
                    body: body);

                Console.WriteLine($" [x] Sent '{routingKey}':'{message}'");
            }
        }
    }
}
