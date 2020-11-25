using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace CosmosDBPerformance
{
    public class ReferenceDocument
    {
        private CosmosClient client = null;
        private Container container = null;
        public ReferenceDocument()
        {
            CosmosClientOptions options = new CosmosClientOptions {
             PortReuseMode = PortReuseMode.PrivatePortPool
             
              
            };
            client = new CosmosClient("<<insert your connection string>>");
            container = client.GetContainer("<<insert you database name>>", "<<insert your collection name>>");

        }


        
        public async Task<IEnumerable<dynamic>> GetReferenceData(string parentQuery, string childQuery, bool changeOptions, int bufferSize, int maxConcurrency)
        {
            QueryRequestOptions options = new QueryRequestOptions();
            if (changeOptions)
            {
                options.MaxConcurrency = maxConcurrency;
                options.MaxBufferedItemCount = bufferSize;
            }

            var orders = new List<dynamic>();
            var orderDetails = new List<dynamic>();

            FeedIterator<dynamic> feedOrders = container.GetItemQueryIterator<dynamic>(parentQuery, null,options);

            while (feedOrders.HasMoreResults)
            {
                var response = await feedOrders.ReadNextAsync().ConfigureAwait(false);
                orders.AddRange(response);
            }

            FeedIterator<dynamic> feedOrderdetails = container.GetItemQueryIterator<dynamic>(childQuery, null, options);

            while (feedOrderdetails.HasMoreResults)
            {
                var response = await feedOrderdetails.ReadNextAsync().ConfigureAwait(false);
                orderDetails.AddRange(response);
            }

            IEnumerable<dynamic> results = orders.GroupJoin(orderDetails,
                                 parent => parent.SalesOrderId,
                                 child => child.SalesOrderId,
                                 (parent, children) => new
                                 {
                                     orders = parent,

                                     orderDetails = children.ToList()


                                 });
            return results;


        }

        public async Task<IEnumerable<dynamic>> GetReferenceData1(string parentQuery, string childQuery, bool changeOptions, int bufferSize, int maxConcurrency, bool changeDegree, int maxDegree)
        {
            QueryRequestOptions options = new QueryRequestOptions();
            if (changeOptions) {
                options.MaxConcurrency = maxConcurrency;
                options.MaxBufferedItemCount = bufferSize;
            }
            
            
            var orders = new List<dynamic>();
            var orderDetails = new List<dynamic>();


            ParallelOptions o = new ParallelOptions();
            if (changeDegree)
            {
                o.MaxDegreeOfParallelism = maxDegree; 
            }

            Parallel.Invoke(o,
                    () =>
                    {
                        FeedIterator<dynamic> feedOrders = container.GetItemQueryIterator<dynamic>(parentQuery, null, options);

                        while (feedOrders.HasMoreResults)
                        {
                            var response = feedOrders.ReadNextAsync().GetAwaiter().GetResult();
                            orders.AddRange(response);
                        }
                    },
                    () =>
                    {

                        FeedIterator<dynamic> feedOrderdetails = container.GetItemQueryIterator<dynamic>(childQuery, null, options);

                        while (feedOrderdetails.HasMoreResults)
                        {
                            var response = feedOrderdetails.ReadNextAsync().GetAwaiter().GetResult();
                            orderDetails.AddRange(response);
                        }
                    }
                    );





            IEnumerable<dynamic> results = orders.GroupJoin(orderDetails,
                                 parent => parent.SalesOrderId,
                                 child => child.SalesOrderId,
                                 (parent, children) => new
                                 {
                                     orders = parent,

                                     orderDetails = children.ToList()


                                 });
            return results;


        }
        
    }
}
