using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using System;
using Messages;
using RabbitMQ.Client;
using Microsoft.Extensions.Options;
using System.Threading.Tasks;
using System.Threading;
using RabbitMQ.Client.Events;
using System.Text;
using Newtonsoft.Json;
using RabbitConsumer.Services;

namespace RabbitConsumer.Consummer
{
    public class RabbitConsummer : BackgroundService
    {
        private readonly RabbitMqConfiguration _config;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly IServiceProvider _serviceProvider;

        public RabbitConsummer(IOptions<RabbitMqConfiguration> options, IServiceProvider serviceProvider)
        {
            _config = options.Value;
            _serviceProvider = serviceProvider;

            var factory = new ConnectionFactory
            {
                HostName = _config.Host
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
            _channel.QueueDeclare(
                queue: _config.Queue,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null
                );
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consummer = new EventingBasicConsumer(_channel);
            consummer.Received += (sender, eventArgs) =>
            {
                var contentArray = eventArgs.Body.ToArray();
                var contentString = Encoding.UTF8.GetString(contentArray);
                var message = JsonConvert.DeserializeObject<MessageRabbit>(contentString);
                _channel.BasicAck(eventArgs.DeliveryTag, false);

                NotifyUser(message);
            };

            _channel.BasicConsume(_config.Queue, false, consummer);

            return Task.CompletedTask;
        }
        public void NotifyUser(MessageRabbit message)
        {
            using (var scope = _serviceProvider.CreateScope())
            {
                var notificationService = scope.ServiceProvider.GetRequiredService<INotificationService>();
                notificationService.NotifyUser(message.FromId, message.ToId, message.Content);
            }
        }
    }
}
