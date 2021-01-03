using System;
using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RpcClient
{
    public class RpcClient : IDisposable
    {
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly string _replyQueueName;
        private readonly EventingBasicConsumer _consumer;
        private readonly BlockingCollection<string> _respQueue = new BlockingCollection<string>();
        private readonly IBasicProperties _props;

        public RpcClient()
        {
            // Establish a connection and channel and
            // declare an exclusive 'callback' queue for replies.
            var factory = new ConnectionFactory() { HostName = "localhost" };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
            _replyQueueName = _channel.QueueDeclare().QueueName;
            _consumer = new EventingBasicConsumer(_channel);

            _props = _channel.CreateBasicProperties();
            var correlationId = Guid.NewGuid().ToString();
            _props.CorrelationId = correlationId;           // Set to a unique value for every request.
            _props.ReplyTo = _replyQueueName;               // Set to the callback queue.

            // Subscribe to the 'callback' queue,
            // so that we can receive RPC responses.
            _consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var response = Encoding.UTF8.GetString(body);
                if (ea.BasicProperties.CorrelationId == correlationId)
                {
                    _respQueue.Add(response);
                }
            };
        }

        public string Call(string message)
        {
            // The request is sent to an rpc_queue queue.
            // 1. Generate a unique CorrelationId number and save it
            // to identify the appropriate response when it arrives.
            // 2. Publish the request message,
            // with two properties: ReplyTo and CorrelationId.
            var messageBytes = Encoding.UTF8.GetBytes(message);
            _channel.BasicPublish(
                exchange: string.Empty,
                routingKey: "rpc_queue",
                basicProperties: _props,
                body: messageBytes);

            _channel.BasicConsume(
                consumer: _consumer,
                queue: _replyQueueName,
                autoAck: true);

            // Finally we return the response back to the user.
            return _respQueue.Take();
        }

        public void Dispose()
        {
            _respQueue?.Dispose();
            _channel?.Dispose();
            _connection?.Dispose();
        }
    }
}
