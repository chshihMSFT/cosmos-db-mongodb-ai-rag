using System;
using Microsoft.Extensions.Configuration;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using System.IO;
using Newtonsoft.Json;
using Azure;
using Azure.AI.OpenAI;
using System.Security.Authentication;


namespace cosmos_db_mongodb_ai_rag
{
    internal class Program
    {
        //Default Values
        static private String FilePath = "";
        static private String FileInput = "";
        static private String FileOutputEmbedding = "";
        static private bool UsingExistsEmbeddingSample = true;
        static private bool NeedUpdateMongoData = false;

        static private String OpenAIEndpoint = "";
        static private String OpenAIKey = "";
        static private String OpenAIDeployname = "";
        static private String OpenAIPrompt = "";
        static private int OpenAISearchItems = 3;
        static private OpenAIClient openAIClient;

        static private String MongoDBconnstring = "";
        static private String MongoDBdatabase = "";
        static private String MongoDBcollection = "";

        static private MongoUrl mongoUrl;
        static private MongoClientSettings mongoClientSettings;
        static private IMongoDatabase mongoDatabase;
        static private IMongoCollection<BsonDocument> mongoCollection;
        static private MongoClient mongoClient;

        static async Task Main(string[] args)
        {
            //Initialing parameters
            try
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Initialing parameters");
                IConfigurationRoot configuration = new ConfigurationBuilder()
                    .AddJsonFile("appSettings.json")
                    .Build();

                FilePath = configuration["FilePath"];
                FileInput = configuration["FileInput"];
                FileOutputEmbedding = configuration["FileOutputEmbedding"];
                UsingExistsEmbeddingSample = Boolean.Parse(configuration["UsingExistsEmbeddingSample"]);
                NeedUpdateMongoData = Boolean.Parse(configuration["NeedUpdateMongoData"]);

                OpenAIEndpoint = configuration["OpenAIEndpoint"];
                OpenAIKey = configuration["OpenAIKey"];
                OpenAIDeployname = configuration["OpenAIDeployname"];
                OpenAIPrompt = configuration["OpenAIPrompt"];
                OpenAISearchItems = Int32.Parse(configuration["OpenAISearchItems"]);
                openAIClient = new(new Uri(OpenAIEndpoint), new AzureKeyCredential(OpenAIKey));

                MongoDBconnstring = configuration["MongoDBconnstring"];
                MongoDBdatabase = configuration["MongoDBdatabase"];
                MongoDBcollection = configuration["MongoDBcollection"];
                mongoUrl = new MongoUrl(MongoDBconnstring);
                mongoClientSettings = MongoClientSettings.FromUrl(mongoUrl);
                mongoClientSettings.ReadPreference = ReadPreference.Nearest;
                mongoClientSettings.SslSettings = new SslSettings() { EnabledSslProtocols = SslProtocols.Tls12 };
                mongoClient = new MongoClient(mongoClientSettings);
                mongoDatabase = mongoClient.GetDatabase(MongoDBdatabase);
                mongoCollection = mongoDatabase.GetCollection<BsonDocument>(MongoDBcollection);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, reading sample data, error: {ex.Message.ToString()}");
            }

            List<Customer> customers = new List<Customer>();
            if (!UsingExistsEmbeddingSample)
            {
                //1. reading sample data
                try
                {
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, reading sample data");
                    string jsonString = File.ReadAllText(FilePath + FileInput);
                    customers = Newtonsoft.Json.JsonConvert.DeserializeObject<List<Customer>>(jsonString);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, reading sample data, error: {ex.Message.ToString()}");
                }

                //2. generating embeddings for sample data and flush into local file
                try
                {
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, generating embeddings for sample data");

                    using (System.IO.StreamWriter file =
                        new System.IO.StreamWriter(FilePath + FileOutputEmbedding, false))
                    {
                        int i = 0;
                        foreach (Customer c in customers)
                        {
                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, get embeddings: {{{c.id}, {c.firstName}, {c.lastName}}}");

                            var tempString = Newtonsoft.Json.JsonConvert.SerializeObject(c).ToString();
                            EmbeddingsOptions embeddingOptions = new(tempString);
                            var returnValue = openAIClient.GetEmbeddings(OpenAIDeployname, embeddingOptions);

                            c.dataembedding = returnValue.Value.Data[0].Embedding.ToArray();
                            c.tokenlen = c.dataembedding.Length;

                            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, saving file: {{{c.id}, {c.firstName}, {c.lastName}, {c.tokenlen}}}");
                            file.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(c).ToString());
                            i++;
                        }
                        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, processed {i} docs");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, generating embeddings for sample data, error: {ex.Message.ToString()}");
                }
            }
            else if (UsingExistsEmbeddingSample)
            {
                try
                {
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, reading existing sample embeddings");

                    using (System.IO.StreamReader file =
                        new System.IO.StreamReader(FilePath + FileOutputEmbedding))
                    {
                        String line = "";
                        while ((line = file.ReadLine()) != null)
                        {
                            Customer c = Newtonsoft.Json.JsonConvert.DeserializeObject<Customer>(line);
                            customers.Add(c);
                        }
                    }
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, processed {customers.Count} records");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, reading existing sample embeddings, error: {ex.Message.ToString()}");
                }

            }

            //3. inserting sample data with embeddings into MongoDB for afterward vector search
            if (NeedUpdateMongoData)
            {
                try
                {
                    int i = 0;
                    foreach (Customer c in customers)
                    {
                        c.timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff");
                        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, insert MongoDB {MongoDBdatabase}.{MongoDBcollection} {{{c.id}, {c.firstName}, {c.lastName}, {c.timestamp}}}");                        
                        BsonDocument customer = c.ToBsonDocument();
                        var result = await mongoCollection.ReplaceOneAsync(
                                        filter: new BsonDocument("_id", c.id),
                                        options: new ReplaceOptions { IsUpsert = true },
                                        replacement: customer);
                        i++;
                    }
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, inserted {i} docs");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, insert MongoDB {MongoDBdatabase}.{MongoDBcollection}, error: {ex.Message.ToString()}");
                }
            }

            //4. serching vector
            try
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, searching prompt \"{OpenAIPrompt}\"");
                EmbeddingsOptions embeddingPromptOption = new(OpenAIPrompt);
                var returnPromptValue = openAIClient.GetEmbeddings(OpenAIDeployname, embeddingPromptOption);
                var queryVector = returnPromptValue.Value.Data[0].Embedding.ToArray();

                BsonDocument[] pipeline = new BsonDocument[]
                {
                    BsonDocument.Parse($"{{$search: {{cosmosSearch: {{ vector: [{string.Join(',', queryVector)}], path: 'dataembedding', k: {OpenAISearchItems}}}, returnStoredSource:true}}}}"),	                
	                BsonDocument.Parse($"{{$project: {{embedding: 0}}}}"),
                };
                var searchResult = mongoCollection.Aggregate<BsonDocument>(pipeline).ToList();

                foreach (var doc in searchResult)
                {
                    Customer c = BsonSerializer.Deserialize<Customer>(doc);
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, {{{c.id}, {c.firstName}, {c.lastName}, {c.emailAddress}, {c.timestamp}}}");
                }
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, retrieved {searchResult.Count} docs");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, searching prompt, error: {ex.Message.ToString()}");
            }
            
        }

    }
}

