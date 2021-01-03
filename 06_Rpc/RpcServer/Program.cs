using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RpcServer
{
    // A server replies with a response message.
    class Program
    {
        static void Main(string[] args)
        {
            // Establishing the connection, channel and declaring the queue.
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(
                    queue: "rpc_queue",
                    durable: false,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                // We might want to run more than one server process.
                // In order to spread the load equally over multiple servers
                // we need to set the prefetchCount setting in channel.BasicQos.
                channel.BasicQos(
                    prefetchSize: 0,
                    prefetchCount: 1,
                    global: false);

                // Use BasicConsume to access the queue.
                var consumer = new EventingBasicConsumer(channel);
                channel.BasicConsume(
                    queue: "rpc_queue",
                    autoAck: false,
                    consumer: consumer);

                Console.WriteLine(" [x] Awaiting RPC requests");

                // Register a delivery handler in which
                // we do the work and send the response back.
                consumer.Received += (model, eventArgs) =>
                {
                    var response = string.Empty;

                    var body = eventArgs.Body.ToArray();
                    var props = eventArgs.BasicProperties;
                    var replyProps = channel.CreateBasicProperties();
                    replyProps.CorrelationId = props.CorrelationId;

                    try
                    {
                        var message = Encoding.UTF8.GetString(body);
                        var n = int.Parse(message);
                        Console.WriteLine($" [.] fib({message})");
                        response = Fib(n).ToString();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(" [.] " + e.Message);
                    }
                    finally
                    {
                        var responseBytes = Encoding.UTF8.GetBytes(response);
                        channel.BasicPublish(
                            exchange: string.Empty,
                            routingKey: props.ReplyTo,
                            basicProperties: replyProps,
                            body: responseBytes);

                        channel.BasicAck(
                            deliveryTag: eventArgs.DeliveryTag,
                            multiple: false);
                    }
                };

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static int Fib(int n)
        {
            if (n == 0 || n == 1)
                return n;

            return Fib(n - 1) + Fib(n - 2);
        }
    }
}
