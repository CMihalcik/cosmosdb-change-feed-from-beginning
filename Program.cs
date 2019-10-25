using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace change_feed_read_from_beginning
{
    class Program
    {
        static string CosmosEndpoint = "";
        static string CosmosKey = "";
        static string databaseName = "";
        static string leaseContainerName = "leaseContainer";
        static string sourceContainerName = "sourceContainer";
        static string processorName = "changeFeedTime";
        static string processorInstanceName = "consoleHost";
        static CosmosClient cosmosClient = new CosmosClient(CosmosEndpoint, CosmosKey);

        static async Task Main(string[] args)
        {
            ChangeFeedProcessor changeFeedProcessor = await StartChangeFeedProcessorAsync();
            Thread.Sleep(TimeSpan.FromSeconds(5));
            Container container = cosmosClient.GetContainer(databaseName, sourceContainerName);
            ToDo secondToDo = new ToDo() {Name = "Second"};
            await container.CreateItemAsync(secondToDo, new PartitionKey(secondToDo.id));
            Console.ReadLine();
            await changeFeedProcessor.StopAsync();
        }
        
        private static async Task<ChangeFeedProcessor> StartChangeFeedProcessorAsync()
        {

            // Delete the source container if it exists
            try
            {
                await cosmosClient.GetContainer(databaseName, sourceContainerName)
                    .DeleteContainerAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"No container to delete");
            }            
            
            // Create the source container and add an item
            await cosmosClient.GetDatabase(databaseName).CreateContainerAsync(sourceContainerName, "/id");
            Container sourceContainer = cosmosClient.GetContainer(databaseName, sourceContainerName);
            ToDo firstToDo = new ToDo() { Name = "First" };
            await sourceContainer.CreateItemAsync(firstToDo, new PartitionKey(firstToDo.id));
            
            // Delete the lease container if it exists 
            try
            {
                await cosmosClient.GetContainer(databaseName, leaseContainerName)
                    .DeleteContainerAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"No container to delete");
            }

            // Create a new lease container
            await cosmosClient.GetDatabase(databaseName).CreateContainerAsync(leaseContainerName, "/id");
            Container leaseContainer = cosmosClient.GetContainer(databaseName, leaseContainerName);
            
            ChangeFeedProcessor changeFeedProcessor = sourceContainer
                .GetChangeFeedProcessorBuilder<ToDo>("changeFeedBeginning", Program.HandleChangesAsync)
                .WithInstanceName("consoleHost")
                .WithLeaseContainer(leaseContainer)
                .WithStartTime(DateTime.MinValue.ToUniversalTime())
                .WithPollInterval(TimeSpan.FromSeconds(1))
                .Build();
            
            Console.WriteLine("Starting Change Feed Processor...");
            await changeFeedProcessor.StartAsync();
            Console.WriteLine("Change Feed Processor started.");
            return changeFeedProcessor;
        }
        
        static async Task HandleChangesAsync(IReadOnlyCollection<ToDo> changes, CancellationToken cancellationToken)
        {
            Console.WriteLine("Started handling changes...");
            foreach (ToDo todo in changes)
            {
                Console.WriteLine($"Changed item: {todo.Name}");
                await Task.Delay(10);
            }

            Console.WriteLine("Finished handling changes.");
        }
    }

}
