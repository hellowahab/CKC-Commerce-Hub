using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

/*
 * Connection URI configuration.
 * - amqp:// specifies the AMQP protocol.
 * - guest:guest are the default username and password for local development.
 * - localhost:5672 points to the local RabbitMQ instance running on port 5672.
 * - %2f represents the URL-encoded default virtual host '/'.
 */
const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";

/*
 * Connection Configuration builder.
 * Configures the connection attributes:
 * - Uri defines the network target.
 * - ContainerId uniquely identifies this client container/application instance.
 *   This is a requirement in the AMQP 1.0 standard to coordinate links and clients.
 */
ConnectionSettings settings = ConnectionSettingsBuilder.Create()
    .Uri(new Uri(brokerUri))
    .ContainerId("tutorial-receive")
    .Build();

/*
 * AMQP Environment and Connection Setup.
 * - IEnvironment manages connection lifecycles, background threads, and I/O channels.
 *   It acts as the runtime context/engine for the modern RabbitMQ AMQP client.
 * - CreateConnectionAsync opens the socket and negotiates the AMQP protocol handshake.
 */
IEnvironment environment = AmqpEnvironment.Create(settings);
IConnection connection = await environment.CreateConnectionAsync();

try
{
    /*
     * Queue Declaration using Management interface.
     * - connection.Management() retrieves the management context.
     * - QueueType.QUORUM specifies a Quorum Queue. Quorum Queues are RabbitMQ's modern
     *   replicated queues based on the Raft consensus protocol. They offer high data safety,
     *   durability, and replication, replacing the deprecated classic mirrored queues.
     * - DeclareAsync makes the network call to ensure the queue exists on the broker.
     */
    IManagement management = connection.Management();
    IQueueSpecification queueSpec = management.Queue("hello").Type(QueueType.QUORUM);
    await queueSpec.DeclareAsync();

    /*
     * Consumer Initialization and Handlers.
     * - ConsumerBuilder creates a consumer instance bound to the "hello" queue.
     * - MessageHandler registers an asynchronous callback for processing incoming messages.
     * - ctx.Accept() acknowledges the message, telling the broker that it was successfully processed.
     * - BuildAndStartAsync starts the consumer loop in the background.
     */
    IConsumer consumer = await connection.ConsumerBuilder()
        .Queue("hello")
        .MessageHandler((ctx, message) =>
        {
            Console.WriteLine($"Received a message: {message.BodyAsString()}");
            ctx.Accept();
            return Task.CompletedTask;
        })
        .BuildAndStartAsync();

    try
    {
        /*
         * Graceful Shutdown Implementation.
         * - Wait for messages until the user interrupts the program (CTRL+C).
         * - CancellationTokenSource controls async delay.
         * - Console.CancelKeyPress intercepts terminal shutdown, letting us cancel the token
         *   gracefully rather than aborting immediately.
         */
        Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // Prevent immediate program termination
            cts.Cancel();    // Signal the cancellation token
        };
        
        // Wait indefinitely until the cancellation token is cancelled
        await Task.Delay(Timeout.Infinite, cts.Token);
    }
    catch (OperationCanceledException)
    {
        // Caught when Task.Delay is cancelled via the CancellationToken
    }
    finally
    {
        // Close the consumer asynchronously to stop receiving new messages
        await consumer.CloseAsync();
    }
}
finally
{
    // Ensure both the active connection and the background environment are shut down cleanly
    await connection.CloseAsync();
    await environment.CloseAsync();
}