using Microsoft.AspNetCore.SignalR.Client;
using Polly;
using RealtimeChatApp.ConsumerService.Models;
using RealtimeChatApp.ConsumerService.RabbitMQ;
using System.Net.Http.Json;
using System.Text.Json;

namespace RealtimeChatApp.ConsumerService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConsumerRabbitMQ _consumer;
        private readonly IConfiguration _configuration;
        private HubConnection _hubConnection;

        public Worker(ILogger<Worker> logger, IConsumerRabbitMQ consumer, IConfiguration configuration)
        {
            _logger = logger;
            _consumer = consumer;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _consumer.ConsumeAsync<MessageModel>("chat-message-save", async (message) =>
            {
                try
                {
                    if (_hubConnection == null || _hubConnection.State != HubConnectionState.Connected)
                    {
                        _logger.LogWarning("Yeniden bağlanılıyor...");
                        await EnsureHubConnectedAsync(stoppingToken);
                    }


                    await _hubConnection.InvokeAsync("SendMessageFromWorker", new ChatMessageDto
                    {
                        SenderNumber = message.SenderNumber,
                        ReceiverNumber = message.ReceiverNumber,
                        Content = message.Content,
                        SentAt = message.SentAt
                    });



                    _logger.LogInformation($"✅ SignalR Hub'a mesaj gönderildi: {JsonSerializer.Serialize(message)}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "❌ SignalR Hub çağrısı başarısız oldu.");
                }
            });
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            await EnsureHubConnectedAsync(cancellationToken);
            await base.StartAsync(cancellationToken);
        }

        private async Task EnsureHubConnectedAsync(CancellationToken cancellationToken)
        {
            var workerAuth = _configuration.GetSection("WorkerAuth");
            var userName = workerAuth["ServiceUser"];
            var password = workerAuth["ServiceSecret"];
            var authEndpoint = workerAuth["WorkerAuthEndpoint"];

            // POLLY: Retry 4 times with 5 seconds delay + Circuit Breaker
            var policy = Policy
                .Handle<Exception>() // retry on ANY exception
                .WaitAndRetryAsync(
                    retryCount: 4,
                    sleepDurationProvider: attempt => TimeSpan.FromSeconds(5),
                    onRetry: (exception, delay, retryAttempt, context) =>
                    {
                        _logger.LogWarning("⚠ Retry {retryAttempt}/4 after error: {message}", retryAttempt, exception.Message);
                    })
                .WrapAsync(
                    Policy.Handle<Exception>()
                          .CircuitBreakerAsync(
                              exceptionsAllowedBeforeBreaking: 1,
                              durationOfBreak: TimeSpan.FromSeconds(5),
                              onBreak: (ex, ts) =>
                              {
                                  _logger.LogError("🛑 Circuit broken! Breaking for {seconds} sec. Reason: {msg}", ts.TotalSeconds, ex.Message);
                              },
                              onReset: () =>
                              {
                                  _logger.LogInformation("🔄 Circuit closed. Ready again.");
                              })
                );

            // Execute with policy
            await policy.ExecuteAsync(async () =>
            {
                using var http = new HttpClient();

                var loginResponse = await http.PostAsJsonAsync(authEndpoint, new
                {
                    username = userName,
                    password = password
                });

                if (!loginResponse.IsSuccessStatusCode)
                {
                    throw new Exception($"Worker login failed. Status: {loginResponse.StatusCode}");
                }

                var json = await loginResponse.Content.ReadFromJsonAsync<WorkerLoginResponse>();
                var token = json?.Token;

                if (string.IsNullOrEmpty(token))
                {
                    throw new Exception("Token not received.");
                }

                _hubConnection = new HubConnectionBuilder()
                    .WithUrl($"https://localhost:7281/workerhub?access_token={token}", options =>
                    {
                        options.AccessTokenProvider = () => Task.FromResult(token);
                    })
                    .WithAutomaticReconnect()
                    .Build();

                await _hubConnection.StartAsync(cancellationToken);

                _logger.LogInformation("✅ Worker Service connected to SignalR WorkerHub.");
            });
        }
    }
}