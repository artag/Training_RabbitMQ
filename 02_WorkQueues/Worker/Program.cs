using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Worker
{
    /// <summary>
    /// Consumer (worker).
    /// Поведение: Round-robin dispatching.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // Включение Message durability (сохранение сообщений при перезапуске RabbitMQ сервера).
                // durable -> true
                channel.QueueDeclare(
                    queue: "task_queue",
                    durable: true,      // Message durability
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                // Включение Fair Dispatch
                // prefetchCount -> не обрабатывать более 1 сообщения
                // (сообщения ожидают в очереди "task_queue").
                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine(" [*] Wainting for messages.");

                // Note: it is possible to access the channel via
                //       ((EventingBasicConsumer)sender).Model here
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, eventArgs) =>
                {
                    var body = eventArgs.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);

                    // Hard work emulation.
                    int dots = message.Split('.').Length - 1;
                    Thread.Sleep(dots * 1000);

                    Console.WriteLine(" [x] Done");


                    // Включение manual message acknowledgments (подтверждение доставки и окончания обработки сообщения).
                    channel.BasicAck(
                        deliveryTag: eventArgs.DeliveryTag,
                        multiple: false);
                };

                // Включение manual message acknowledgments
                // autoAck -> false
                channel.BasicConsume(
                    queue: "task_queue",
                    autoAck: false,         // Manual message acknowledgments
                    consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
