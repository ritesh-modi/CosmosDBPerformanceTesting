using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace CosmosDBPerformance
{
    public class EmbeddedDocuments
    {
        private CosmosClient client = null;
        private Container container = null;
        public EmbeddedDocuments()
        {
            client = new CosmosClient("<<insert your connection string>>");
            container = client.GetContainer("<<insert you database name>>", "<<insert your collection name>>");
        }

        public async Task<IEnumerable<dynamic>> GetData(string query, bool changeOptions, int bufferSize, int maxConcurrency)
        {
            QueryRequestOptions options = new QueryRequestOptions();
            
            if (changeOptions)
            {
                options.MaxConcurrency = maxConcurrency;
                options.MaxBufferedItemCount = bufferSize;
            }

            var results = new List<dynamic>();
            FeedIterator<dynamic> feeds = container.GetItemQueryIterator<dynamic>(query, null, options);

            while (feeds.HasMoreResults)
            {
                var response = await feeds.ReadNextAsync().ConfigureAwait(false);
                results.AddRange(response);
            }

            return results;

        }

    }
}
