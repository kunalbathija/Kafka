// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Newtonsoft.Json;

Console.WriteLine("Hello, World!");

var config = new ConsumerConfig
{
    GroupId = "god-knows-why-this-is-required",
    BootstrapServers = "localhost:9092"
};

using(var consumer = new ConsumerBuilder<string, string>(config).Build())
{
    string topic = "kunal";
    consumer.Subscribe(topic);

    while (true)
    {
        var serializedData = consumer.Consume();
        Console.WriteLine($"Key: {serializedData.Key} Value: {serializedData.Value}");  
        var deserializedData = JsonConvert.DeserializeObject<WeatherForecast>(serializedData.Value);
        Console.WriteLine($"Id - {deserializedData.Id} \n Date - {deserializedData.Date}" +
            $"\n TemperatureC - {deserializedData.TemperatureC} \n TemperatureF - {deserializedData.TemperatureF}");
    }
}

public class WeatherForecast
{
    public string Id { get; set; }
    public DateTime Date { get; set; }

    public int TemperatureC { get; set; }

    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}