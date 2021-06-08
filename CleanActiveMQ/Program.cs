using System;
using System.Collections.Generic;
using System.Linq;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;

namespace CleanActiveMQ
{
    class Program
    {
        private static ConnectionFactory _factory = new ConnectionFactory("tcp://localhost:61616");
        private static Connection _connection = null;
        private static Session _session = null;
        private static bool _closeRequested = false;

        static void Main(string[] args)
        {
            while (!_closeRequested)
            {
                try
                {
                    if (_connection == null) 
                    {
                        _connection = _factory.CreateConnection() as Connection;
                        _connection.Start();
                        _session = _connection.CreateSession() as Session;
                    }

                    PrintMenu();
                    var queues = GetAllQueues();
                    PrintQueues(queues);

                    Console.Write("> ");
                    var command = Console.ReadLine();
                    if (command == "d")
                    {
                        foreach (var q in queues)
                        {
                            SessionUtil.DeleteQueue(_session, q);
                        }
                    }

                    if (command == "q")
                    {
                        _closeRequested = true;
                    }
                }
                catch (Exception exc)
                {
                    _connection?.Close();
                    _connection = null;
                }
            }

            Console.WriteLine();
        }

        private static void PrintQueues(List<string> queues)
        {
            if (!queues.Any())
            {
                Console.WriteLine("no queues");
                return;
            }

            Console.WriteLine(string.Join(", " + Environment.NewLine, queues));
        }

        private static void PrintMenu()
        {
            Console.Clear();
            Console.WriteLine("Clean all tables util...");
            Console.WriteLine("________________________");
            Console.WriteLine();
            Console.WriteLine("r ... refresh");
            Console.WriteLine("d ... delete");
            Console.WriteLine("q ... quit");
            Console.WriteLine("________________________");
            Console.WriteLine();
        }

        static List<string> GetAllQueues()
        {
            List<string> queues = new List<string>();
            string QUEUE_ADVISORY_DESTINATION = "ActiveMQ.Advisory.Queue";
            IDestination dest = SessionUtil.GetTopic(_session, QUEUE_ADVISORY_DESTINATION);

            using (IMessageConsumer consumer = _session.CreateConsumer(dest))
            {
                IMessage advisory;

                while ((advisory = consumer.Receive(TimeSpan.FromSeconds(3))) != null)
                {
                    ActiveMQMessage amqMsg = advisory as ActiveMQMessage;

                    if (amqMsg.DataStructure != null)
                    {
                        DestinationInfo info = amqMsg.DataStructure as DestinationInfo;
                        if (info?.Destination?.IsQueue ?? false)
                        {
                            queues.Add(((ActiveMQQueue)info.Destination).QueueName);
                        }
                    }
                }
            }
            
            return queues;
        }
    }
}
