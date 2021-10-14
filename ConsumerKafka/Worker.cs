using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsumerKafka
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

            while (!stoppingToken.IsCancellationRequested)
            {

                var config = new ConsumerConfig
                {
                    BootstrapServers = "localhost:9092",
                    GroupId = "testeKafka",
                    AutoOffsetReset = AutoOffsetReset.Latest

                };

                using (var consumer = new ConsumerBuilder<string, string>(config).Build())
                {
                    consumer.Subscribe("KAFKA_TESTE");

                    var consumeResult = consumer.Consume(stoppingToken);

                    //processar mensagem
                    if (consumeResult != null)
                    {
                        Console.WriteLine(consumeResult.Message.Key);
                        Console.WriteLine(consumeResult.Message.Value);
                        Console.WriteLine(consumeResult.Offset);
                    }

                    consumer.Close();
                }

                await Task.Delay(5000, stoppingToken);
            }
        }
    }
}
