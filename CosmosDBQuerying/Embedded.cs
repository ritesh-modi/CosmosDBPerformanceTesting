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
            client = new CosmosClient("AccountEndpoint=https://structtest.documents.azure.com:443/;AccountKey=1QKOx2RKwkhN5DyAXFLTmp3C4AJ15XxZREefLMmaTghucyCnkOWhIo77z5b3HdML3CX1IFffXMqsYXe7aK3QZg==;");
            container = client.GetContainer("ecommerce-large", "OrderManagemenr");
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
