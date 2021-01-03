using System;

namespace RpcClient
{
    // A client sends a request message.
    class Program
    {
        static void Main(string[] args)
        {
            using (var rpcClient = new RpcClient())
            {
                Console.WriteLine(" [x] Requesting fib(30)");
                var response = rpcClient.Call("30");
                Console.WriteLine($" [.] Got '{response}'");
            }
        }
    }
}
