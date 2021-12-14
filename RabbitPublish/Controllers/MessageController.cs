using Messages;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;

namespace RabbitPublish.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class MessageController : ControllerBase
    {
        private readonly ConnectionFactory _factory;
        private readonly RabbitMqConfiguration _config;

        public MessageController(IOptions<RabbitMqConfiguration> options)
        {
            _config = options.Value;

            _factory = new ConnectionFactory
            {
                HostName = _config.Host
            };
        }

        [HttpPost]
        public IActionResult PostMessage([FromBody] MessageRabbit message)
        {
            using (var connection = _factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(
                        queue: _config.Queue,
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                        );

                    var messageSerialized = JsonConvert.SerializeObject(message);
                    var messageBytes = Encoding.UTF8.GetBytes(messageSerialized);

                    channel.BasicPublish(
                        exchange:"",
                        routingKey: _config.Queue,
                        basicProperties: null,
                        body: messageBytes
                        );
                }
            }
            return Accepted();
        }
    }
}
