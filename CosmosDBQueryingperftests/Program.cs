using System;
using System.Reflection;
using Microsoft.Xunit.Performance.Api;

namespace helloworldperftests
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var harness = new XunitPerformanceHarness(args)) {
                var entryassemblypath = Assembly.GetEntryAssembly().Location;
                harness.RunBenchmarks(entryassemblypath);
            }
        }
    }
}
