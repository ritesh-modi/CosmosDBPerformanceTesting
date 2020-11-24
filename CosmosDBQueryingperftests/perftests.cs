using Microsoft.Xunit.Performance;
using System;
using System.Collections.Generic;
using System.Text;
using CosmosDBPerformance;
using Xunit;

namespace CosmosDbQueryPerfTests
{
    public class PerfTests
    {

        public static IEnumerable<object[]> Data =>
        new List<object[]>
        {
            new object[] { false, 0, 0 },
            new object[] {true, 5000, 200},
        };


        [Benchmark(Timeout = 100000)]
        [MemberData(nameof(Data))]

        public void TestHierarchysimplequery(bool changeOptions, int buffer, int concurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;


            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.EmbeddedDocuments h = new CosmosDBPerformance.EmbeddedDocuments();
                using (iteration.StartMeasurement())
                {
                    h.GetData("SELECT TOP 5000 * FROM C", changeOptions, buffer, concurrency).GetAwaiter().GetResult();

                }
            }
        }

        [Benchmark(Timeout = 100000)]
        [MemberData(nameof(Data))]

        public void TestHierarchyWherequery(bool changeOptions, int buffer, int concurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;


            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.EmbeddedDocuments h = new CosmosDBPerformance.EmbeddedDocuments();
                using (iteration.StartMeasurement())
                {
                    h.GetData("select top 1000 * from c where c.SalesOrderId='71783'", changeOptions, buffer, concurrency).GetAwaiter().GetResult();

                }
            }
        }


        [Benchmark(Timeout = 100000)]
        [MemberData(nameof(Data))]

        public void TestHierarchyJoinquery(bool changeOptions, int buffer, int concurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;


            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.EmbeddedDocuments h = new CosmosDBPerformance.EmbeddedDocuments();
                using (iteration.StartMeasurement())
                {
                    h.GetData("SELECT top 1000 c.SalesOrderId, f.SalesOrderDetailID FROM c join  f in c.orderdetails", changeOptions, buffer, concurrency).GetAwaiter().GetResult();

                }
            }
        }

        [Benchmark(Timeout = 100000)]
        [MemberData(nameof(Data))]

        public void TestHierarchySumGroupquery(bool changeOptions, int buffer, int concurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;


            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.EmbeddedDocuments h = new CosmosDBPerformance.EmbeddedDocuments();
                using (iteration.StartMeasurement())
                {
                    h.GetData("SELECT top 1000 sum(c.TotalDue), c.CustomerId from c group by c.CustomerId", changeOptions, buffer, concurrency).GetAwaiter().GetResult();

                }
            }
        }

        [Benchmark(Timeout = 100000)]
        [MemberData(nameof(Data))]

        public void TestHierarchyFunctionquery(bool changeOptions, int buffer, int concurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;


            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.EmbeddedDocuments h = new CosmosDBPerformance.EmbeddedDocuments();
                using (iteration.StartMeasurement())
                {
                    h.GetData("select top 1000 CONCAT(c.SalesOrderNumber, c.AccountNumber) from c where ENDSWITH(c.SalesOrderNumber, '4', false)", changeOptions, buffer, concurrency).GetAwaiter().GetResult();

                }
            }
        }

        [Benchmark(Timeout = 100000)]
        [MemberData(nameof(Data))]
        public void TestReferenceWhereQuery(bool changeOptions, int buffer, int concurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"SELECT top 1000 * from c where IS_DEFINED(c.CustomerId) and c.SalesOrderId='71783'";
            string orderDetails = @"SELECT top 1000 * from c where IS_DEFINED(c.SalesOrderDetailID) and c.SalesOrderId='71783'";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData(orders, orderDetails, changeOptions, buffer, concurrency).GetAwaiter().GetResult();

                }
            }
        }


        [Benchmark(Timeout = 100000)]
        [MemberData(nameof(Data))]
        public void TestReferenceFunctionQuery(bool changeOptions, int buffer, int concurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"select top 1000 CONCAT(c.SalesOrderNumber, c.AccountNumber), c.SalesOrderId from c where ENDSWITH(c.SalesOrderNumber, '4', false)";
            string orderDetails = @"SELECT top 1000 * from c where IS_DEFINED(c.SalesOrderDetailID) and c.SalesOrderId='71783'";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData(orders, orderDetails, changeOptions, buffer, concurrency).GetAwaiter().GetResult();

                }
            }
        }

        [Benchmark(Timeout = 100000)]
        [MemberData(nameof(Data))]
        public void TestReferenceSumGroupQuery(bool changeOptions, int buffer, int concurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"SELECT top 1000 sum(c.TotalDue), c.CustomerId,c.SalesOrderId from c group by c.CustomerId, c.SalesOrderId";
            string orderDetails = @"SELECT top 1000 * from c where IS_DEFINED(c.SalesOrderDetailID)";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData(orders, orderDetails, changeOptions, buffer, concurrency).GetAwaiter().GetResult();

                }
            }
        }

        [Benchmark(Timeout = 100000)]
        [MemberData(nameof(Data))]
        public void TestReferenceSimpleQuery(bool changeOptions, int buffer, int concurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"SELECT top 5000 * from c where IS_DEFINED(c.CustomerId)";
            string orderDetails = @"SELECT top 5000 * from c where IS_DEFINED(c.SalesOrderDetailID)";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData(orders, orderDetails, changeOptions, buffer, concurrency).GetAwaiter().GetResult();

                }
            }
        }

        [Benchmark(Timeout = 100000)]
        [InlineData(false, 0, 0)]
        [InlineData(true, 5000, 200)]
        public void TestReferenceJoinQuery(bool changeOptions, int bufferSize, int maxConcurrency)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"SELECT top 1000 c.SalesOrderId, f.SalesOrderDetailID FROM c join  f in c.orderdetails";
            string orderDetails = @"SELECT top 1000 * from c where IS_DEFINED(c.SalesOrderDetailID)";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData(orders, orderDetails, changeOptions, bufferSize, maxConcurrency).GetAwaiter().GetResult();

                }
            }
        }


        [Benchmark(Timeout = 100000)]
        [InlineData(false, 0, 0, false, 0)]
        [InlineData(true, 5000, 200, true, 4)]

        public void TestReferenceParallelSimpleQuery(bool changeOptions, int bufferSize, int maxConcurrency, bool changeDegree, int maxDegree)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"SELECT top 5000 * from c where IS_DEFINED(c.CustomerId) ";
            string orderDetails = @"SELECT top 5000 * from c where IS_DEFINED(c.SalesOrderDetailID)";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData1(orders, orderDetails, changeOptions, bufferSize, maxConcurrency, changeDegree, maxDegree).GetAwaiter().GetResult();

                }
            }
        }

        [Benchmark(Timeout = 100000)]
        [InlineData(false, 0, 0, false, 0)]
        [InlineData(true, 5000, 200, true, 4)]
        public void TestReferenceParallelWhereQuery(bool changeOptions, int bufferSize, int maxConcurrency, bool changeDegree, int maxDegree)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"SELECT top 1000 * from c where IS_DEFINED(c.CustomerId) and c.SalesOrderId='71783'";
            string orderDetails = @"SELECT top 1000 * from c where IS_DEFINED(c.SalesOrderDetailID) and c.SalesOrderId='71783'";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData1(orders, orderDetails, changeOptions, bufferSize, maxConcurrency, changeDegree, maxDegree).GetAwaiter().GetResult();

                }
            }
        }


        [Benchmark(Timeout = 100000)]
        [InlineData(false, 0, 0, false, 0)]
        [InlineData(true, 5000, 200, true, 4)]
        public void TestReferenceParallelFunctionQuery(bool changeOptions, int bufferSize, int maxConcurrency, bool changeDegree, int maxDegree)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"select top 1000 CONCAT(c.SalesOrderNumber, c.AccountNumber), c.SalesOrderId from c where ENDSWITH(c.SalesOrderNumber, '4', false)";
            string orderDetails = @"SELECT top 1000 * from c where IS_DEFINED(c.SalesOrderDetailID) and c.SalesOrderId='71783'";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData1(orders, orderDetails, changeOptions, bufferSize, maxConcurrency, changeDegree, maxDegree).GetAwaiter().GetResult();
                }
            }
        }


        [Benchmark(Timeout = 100000)]
        [InlineData(false, 0, 0, false, 0)]
        [InlineData(true, 5000, 200, true, 4)]
        public void TestReferenceParallelSumGroupQuery(bool changeOptions, int bufferSize, int maxConcurrency, bool changeDegree, int maxDegree)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"SELECT top 1000 sum(c.TotalDue), c.CustomerId,c.SalesOrderId from c group by c.CustomerId, c.SalesOrderId";
            string orderDetails = @"SELECT top 1000 * from c where IS_DEFINED(c.SalesOrderDetailID) and c.SalesOrderId='71783'";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData1(orders, orderDetails, changeOptions, bufferSize, maxConcurrency, changeDegree, maxDegree).GetAwaiter().GetResult();

                }
            }
        }


        [Benchmark(Timeout = 100000)]
        [InlineData(false, 0, 0, false, 0)]
        [InlineData(true, 5000, 200, true, 4)]
        public void TestReferenceParallelJoinQuery(bool changeOptions, int bufferSize, int maxConcurrency, bool changeDegree, int maxDegree)
        {
            BenchmarkConfiguration bc = BenchmarkConfiguration.Instance;
            bc.MinIteration = 30;
            bc.MaxIteration = 30;

            string orders = @"SELECT top 1000 c.SalesOrderId, f.SalesOrderDetailID FROM c join  f in c.orderdetails";
            string orderDetails = @"SELECT top 1000 * from c where IS_DEFINED(c.SalesOrderDetailID)";

            foreach (var iteration in Benchmark.Iterations)
            {
                CosmosDBPerformance.ReferenceDocument h = new CosmosDBPerformance.ReferenceDocument();
                using (iteration.StartMeasurement())
                {
                    h.GetReferenceData1(orders, orderDetails, changeOptions, bufferSize, maxConcurrency, changeDegree, maxDegree).GetAwaiter().GetResult();

                }
            }
        }


    }
}
