using RabbitMQ.Client;
using System.Text;

if( args.Length != 2 )
{
	Console.WriteLine( "Usage: AckProblemTest <queueName> <targetCount>" );
	return;
}
var queueName = args[ 0 ];
var targetCount = int.Parse( args[ 1 ] );

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = factory.CreateConnection();
using var sendingChannel = connection.CreateModel();
sendingChannel.ConfirmSelect();

var messageBody = Encoding.UTF8.GetBytes( "Hello world" );
int pass = 1;

while( true )
{
	Console.WriteLine( "-----------------" );
	Console.WriteLine( "Pass: " + pass++ );

	sendingChannel.QueuePurge( queueName );

	for( var i = 1; i <= targetCount; i++ )
	{
		var properties = sendingChannel.CreateBasicProperties();
		properties.MessageId = i.ToString();
		sendingChannel.BasicPublish( "", queueName, properties, messageBody );
	}
	sendingChannel.WaitForConfirmsOrDie();

	Thread.Sleep( 200 );
	var count = sendingChannel.MessageCount( queueName );
	if( count != targetCount )
	{
		Console.WriteLine( $"Start count is wrong: {count}!!!" );
		return;
	}
	Console.WriteLine( "Sent: " + count );

	var receivingChannel = connection.CreateModel();
	// Without this quorum queue would limit us to 2000 unacked messages
	receivingChannel.BasicQos( 0, 65535, false );

	var receivedDeliveryTags = new List<ulong>();
	while( true )
	{
		var receivedMessage = receivingChannel.BasicGet( queueName, false );
		if( receivedMessage == null )
		{
			break;
		}
		receivedDeliveryTags.Add( receivedMessage.DeliveryTag );
	}
	if( receivedDeliveryTags.Count != targetCount )
	{
		Console.WriteLine( $"We received less messages than sent: {receivedDeliveryTags.Count}!!!" );
		return;
	}
	foreach( var tag in receivedDeliveryTags )
	{
		receivingChannel.BasicAck( tag, false );
	}
	receivingChannel.Close();
	receivingChannel.Dispose();
	Console.WriteLine( $"Received and acknowledged: {receivedDeliveryTags.Count} msgs" );

	Thread.Sleep( 1000 );
	var countAfterDelete = sendingChannel.MessageCount( queueName );

	Console.WriteLine( "Count after delete: " + countAfterDelete );

	if( countAfterDelete != 0 )
	{
		// wait a bit more and recheck
		Thread.Sleep( 5000 );
		countAfterDelete = sendingChannel.MessageCount( queueName );
		if( countAfterDelete != 0 )
		{
			Console.WriteLine( $"Not all messages were deleted, {countAfterDelete} remained!" );
			return;
		}
		Console.WriteLine( "False positive, all messages were deleted" );
	}
}