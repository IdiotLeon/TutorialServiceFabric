using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;

namespace VotingWeb.Controllers
{
    [Produces("application/json")]
    [Route("api/Votes")]
    public class VotesController : Controller
    {
        private readonly HttpClient httpClient;
        private readonly FabricClient fabricClient;
        private readonly StatelessServiceContext serviceContext;

        public VotesController(HttpClient httpClient, StatelessServiceContext context, FabricClient fabricClient)
        {
            this.httpClient = httpClient;
            this.fabricClient = fabricClient;
            this.serviceContext = context;
        }

        [HttpGet("")]
        public async Task<IActionResult> Get()
        {
            Uri serviceName = VotingWeb.GetVotingDataServiceName(this.serviceContext);
            Uri proxyAddress = this.GetProxyAddress(serviceName);

            var partitions = await this.fabricClient.QueryManager.GetPartitionListAsync(serviceName);

            var result = new List<KeyValuePair<string, int>>();

            foreach (var partition in partitions)
            {
                string proxyUrl = $"{proxyAddress}/api/VoteData?PartitionKey={ ((Int64RangePartitionInformation)partition.PartitionInformation).LowKey}&PartitionKind = Int64Range";

                using (var response = await this.httpClient.GetAsync(proxyUrl))
                {
                    if (response.StatusCode != System.Net.HttpStatusCode.OK)
                    {
                        if (response.StatusCode != System.Net.HttpStatusCode.OK)
                        {
                            continue;
                        }

                        result.AddRange(JsonConvert.DeserializeObject<List<KeyValuePair<string, int>>>(await response.Content.ReadAsStringAsync()));
                    }
                }
            }

            return this.Json(result);
        }

        // PUT api/Votes/name
        [HttpPut("{name}")]
        public async Task<IActionResult> Put(string name)
        {
            var serviceName = VotingWeb.GetVotingDataServiceName(this.serviceContext);
            var proxyAddress = this.GetProxyAddress(serviceName);
            long partitionKey = this.GetPartitionKey(name);
            string proxyUrl = $"{proxyAddress}/api/VoteData/{name}?PartitionKey={partitionKey}&PartitionKind=Int64Range";

            var putContent = new StringContent($"{{'name':'{name}'}}", Encoding.UTF8, "application/json");
            putContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            using (var response = await this.httpClient.PutAsync(proxyUrl, putContent))
            {
                return new ContentResult()
                {
                    StatusCode = (int)response.StatusCode,
                    Content = await response.Content.ReadAsStringAsync()
                };
            }
        }

        // DELETE api/votes/name
        [HttpDelete("{name}")]
        public async Task<IActionResult> Delete(string name)
        {
            var serviceName = VotingWeb.GetVotingDataServiceName(this.serviceContext);
            var proxyAddress = this.GetProxyAddress(serviceName);
            long partitionKey = this.GetPartitionKey(name);
            string proxyUrl = $"{proxyAddress}/api/VoteData/{name}?PartitionKey={partitionKey}&PartitionKind=Int64Range";

            using (var response = await this.httpClient.DeleteAsync(proxyUrl))
            {
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    return this.StatusCode((int)response.StatusCode);
                }
            }
            return new OkResult();
        }

        /// <summary>
        /// Constructs a reverse proxy URL for a given service.
        /// Example: http://localhost:19081/VotingApplication/VotingData/
        /// </summary>
        /// <param name="serviceName"></param>
        /// <returns></returns>
        private Uri GetProxyAddress(Uri serviceName)
        {
            return new Uri($"http://localhost:19081{serviceName.AbsolutePath}");
        }

        /// <summary>
        /// Creates a partition key from the given name.
        /// Uses the zero-based numeric position in the alphabet of the first letter of the name (0-25).
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private long GetPartitionKey(string name)
        {
            return Char.ToUpper(name.First()) - 'A';
        }
    }
}
