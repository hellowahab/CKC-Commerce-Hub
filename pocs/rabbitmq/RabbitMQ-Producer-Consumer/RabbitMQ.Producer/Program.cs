using System.Text;
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
    .ContainerId("tutorial-send")
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
     * Publisher Initialization.
     * Builds and starts a message publisher targeted at the "hello" queue.
     * The publisher runs in a nested try-finally block to ensure it is closed properly.
     */
    IPublisher publisher = await connection.PublisherBuilder().Queue("hello").BuildAsync();
    try
    {
        /*
         * Constructing the Message.
         * AMQP 1.0 messages transport raw byte payloads. We encode the string
         * "Hello World!" into UTF-8 bytes to construct the AmqpMessage.
         */
        const string body = "Hello World!";
        var message = new AmqpMessage(Encoding.UTF8.GetBytes(body));
        
        /*
         * Publishing and awaiting confirmation.
         * AMQP 1.0 is a peer-to-peer protocol where messages are transferred with a
         * delivery state. We publish and wait for the broker to acknowledge receipt.
         */
        PublishResult pr = await publisher.PublishAsync(message);
        
        /*
         * Handling the Publish Outcome State.
         * The outcome determines if the message was successfully stored or rejected by the broker.
         */
        switch (pr.Outcome.State)
        {
            // Accepted: The broker successfully accepted ownership of the message and queued it.
            case OutcomeState.Accepted:
                break;
                
            // Released: The broker did not accept the message, but released it (it may be retried/redelivered).
            case OutcomeState.Released:
                Console.Error.WriteLine($"Released message: {pr.Message.BodyAsString()}");
                Environment.Exit(1);
                break;
                
            // Rejected: The broker rejected the message because it was invalid or could not be processed.
            case OutcomeState.Rejected:
                Console.Error.WriteLine($"[Publisher] Message: {pr.Message.BodyAsString()} rejected with error: {pr.Outcome.Error}");
                Environment.Exit(1);
                break;
                
            // Catch-all for any other outcome states (such as Modified or custom extensions).
            default:
                Console.Error.WriteLine($"Unexpected publish outcome: {pr.Outcome.State}");
                Environment.Exit(1);
                break;
        }

        Console.WriteLine($" [x] Sent {body}");
    }
    finally
    {
        // Always close the publisher to release client resources, channels, and pending publish tasks.
        await publisher.CloseAsync();
    }
}
finally
{
    // Always close the connection and the client environment to cleanly terminate sockets,
    // shutdown worker threads, and release operating system resources.
    await connection.CloseAsync();
    await environment.CloseAsync();
}