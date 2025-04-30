using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddSignalR();
builder.Services.AddCors(options =>
{
    options.AddPolicy("CorsPolicy", builder =>
    {
        builder.WithOrigins("http://localhost:5500")
            .AllowAnyMethod()
            .AllowAnyHeader()
            .AllowCredentials();
    });
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseCors("CorsPolicy");
app.UseHttpsRedirection();

app.MapHub<TemperatureHub>("/temperature-hub");

app.MapGet("/temperature-events", async (HttpContext context) =>
{
    context.Response.Headers.Add("Content-Type", "text/event-stream");
    context.Response.Headers.Add("Cache-Control", "no-cache");
    context.Response.Headers.Add("Connection", "keep-alive");

    while (!context.RequestAborted.IsCancellationRequested)
    {
        var temperature = new 
        {
            value = Math.Round(Random.Shared.NextDouble() * 20 + 15, 1),
            unit = "C",
            timestamp = DateTime.UtcNow
        };

        await context.Response.WriteAsync($"event: temperature\n");
        await context.Response.WriteAsync($"data: {System.Text.Json.JsonSerializer.Serialize(temperature)}\n\n");
        await context.Response.Body.FlushAsync();

        await Task.Delay(1000, context.RequestAborted);
    }
});

app.Run();

public class TemperatureHub : Microsoft.AspNetCore.SignalR.Hub
{
    private static System.Timers.Timer _timer;
    private static readonly object _lock = new object();
    private static IHubContext<TemperatureHub> _hubContext;

    public TemperatureHub(IHubContext<TemperatureHub> hubContext)
    {
        _hubContext = hubContext;
    }

    public override async Task OnConnectedAsync()
    {
        await base.OnConnectedAsync();
        
        lock (_lock)
        {
            if (_timer == null)
            {
                _timer = new System.Timers.Timer(1000);
                _timer.Elapsed += async (sender, e) => await BroadcastTemperature();
                _timer.AutoReset = true;
                _timer.Start();
            }
        }
    }

    private static async Task BroadcastTemperature()
    {
        try
        {
            var temperature = new 
            {
                value = Math.Round(Random.Shared.NextDouble() * 20 + 15, 1),
                unit = "C",
                timestamp = DateTime.UtcNow
            };

            await _hubContext.Clients.All.SendAsync("ReceiveTemperature", temperature);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error broadcasting temperature: {ex.Message}");
        }
    }
}
