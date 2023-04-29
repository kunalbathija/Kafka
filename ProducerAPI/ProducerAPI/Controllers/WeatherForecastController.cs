using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace ProducerAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class WeatherForecastController : ControllerBase
    {

        private readonly ILogger<WeatherForecastController> _logger;
        private readonly ProducerConfig _producerConfig;
        private readonly IConfiguration _configuration;

        public WeatherForecastController(ILogger<WeatherForecastController> logger,
            ProducerConfig producerConfig,
            IConfiguration configuration)
        {
            _logger = logger;
            _producerConfig = producerConfig;
            _configuration = configuration;
        }

        [HttpPost("Produce")]
        public async Task<ActionResult> Produce([FromBody] WeatherForecast weather)
        {
            string serializedData = JsonConvert.SerializeObject(weather);

            var topic = _configuration.GetSection("TopicName").Value;

            using(var producer = new ProducerBuilder<string, string>(_producerConfig).Build())
            {
                await producer.ProduceAsync(topic, new Message<string, string> { Key = weather.Id, Value = serializedData });
                producer.Flush(TimeSpan.FromSeconds(10));
                return Ok(true);    
            }
        }
    }
}