public class videos                // "videos" is table name

{   
    public string Id { get; set; }
    public string name { get; set; }
    public string type { get; set; }
    public float size { get; set; }
    public string added_time { get; set; }
    
}

using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Marten;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
 
namespace MartenAzureDocDbDemo
{
    public static class QuotesPost
    {
        [FunctionName("QuotesPost")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "Batch_POC")]HttpRequestMessage req, 
            TraceWriter log)    // Http trigger, Batch_POC is database name
        {
            log.Info("C# HTTP trigger function processed a request.");
 
            Quote quote = await req.Content.ReadAsAsync<videos>();  // Get data from above class
 
            using (var store = DocumentStore
                .For("host=batch-poc.postgres.database.azure.com;database=Batch_POC;password=Arpatech@1234;username=adminuser@batch-poc"))
            {
                using (var session = store.LightweightSession())
                {
                    session.Store(quote);   // store in Videos table in database
 
                    session.SaveChanges();
                }
            }
 
            return req.CreateResponse(HttpStatusCode.OK, $"Added new Video with Name={videos.name}");
        }
    }
}