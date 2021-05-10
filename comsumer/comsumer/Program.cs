using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using FilesRdWt.BaseLib;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Configuration;

namespace comsumer
{
    /*
     *消費者處理消息
     */
    class Program
    {
        public static long OffsetKafka = 0;
        static void Main(string[] args)
        {
           OffsetKafka= GetOffset();
            SetConsoleCtrlHandler(cancelHandler, true);
            Consume(getKafkaBroker(), getTopicName());
            //Consume(getKafkaBroker(), "testMulitiPartition");
        }

        private static  long GetOffset()
        {
            long offset = 0;
            //IniFileWr.IniFileSetVal("./config.ini","topic","test","0");
            string a = IniFileWr.IniFileGetVal("./config.ini", "topic", "test");
            if (a=="")
            {
                offset = 0;
            }
            offset = Convert.ToInt64(IniFileWr.IniFileGetVal("./config.ini", "topic", "test"));
            return offset;
        }
        private static int GetPartitionId()
        {
            int partitionId = 0;
            //IniFileWr.IniFileSetVal("./config.ini","topic","test","0");
            string a = IniFileWr.IniFileGetVal("./config.ini", "topic", "partitionId");
            if (a == "")
            {
                partitionId = 0;
            }
            partitionId = Convert.ToInt32(IniFileWr.IniFileGetVal("./config.ini", "topic", "partitionId"));
            Console.WriteLine(" PartitionID:{0}", partitionId);
            return partitionId;
        }
        private static void Consume(string broker, string topic)
        {
            string[] temp = broker.Split(',');
            Uri[] url =new Uri[1];
            int index = 0;
            foreach(string item in temp)
            {
                url[index] = new Uri(item);
                index++;
            }
            var options = new KafkaOptions(url);
            var router = new BrokerRouter(options);
            OffsetPosition[] off =new OffsetPosition[1];
            //off
            int partionId = GetPartitionId();
            off[0] = new OffsetPosition(partionId, 9999);
            //off[1] = new OffsetPosition(1, 0);
            //off[2] = new OffsetPosition(2, 0);
            Consumer consumer = new Consumer(new ConsumerOptions(topic, router),off);
            //var tt2 = consumer.GetTopicOffsetAsync(topic, 2, -1);
            ////var tt = consumer.GetOffsetPosition();
            //var test = CreateOffsetFetchRequest(topic,1);
            //var test2 = CreateFetchRequest(topic, 0);
            ////consumer.SetOffsetPosition(new OffsetPosition(consumer.GetOffsetPosition()));
            ////Consume returns a blocking IEnumerable (ie: never ending stream)
            bool flag = false;
            //var t =consumer.Consume();
            //var tt = consumer.GetOffsetPosition();
            List<OffsetPosition> tt3;

            var msgs = consumer.Consume();
            
            var needHandle = from msgItem in msgs
                where msgItem.Meta.Offset >= (OffsetKafka+1) & msgItem.Meta.PartitionId==2//读取指定分区的消息
                select msgItem;
            /*
            foreach (var message in consumer.Consume())
            {
                //if (!flag)
                //{
                    
                    tt3 = consumer.GetOffsetPosition();
                    OffsetPosition[] str = tt3.ToArray();
                    consumer.SetOffsetPosition(str);
                    //consumer.Dispose();
                    //break;
                    
                    flag = true;
                //}
                OffsetKafka = message.Meta.Offset;
                Console.WriteLine("1 Response: Partition {0},Offset {1} : {2}",
                    message.Meta.PartitionId, message.Meta.Offset, message.Value.ToUtf8String());
            }
             */
            foreach (var message in needHandle)
            {
                //if (!flag)
                //{

                tt3 = consumer.GetOffsetPosition();
                OffsetPosition[] str = tt3.ToArray();
                consumer.SetOffsetPosition(str);
                //consumer.Dispose();
                //break;

                flag = true;
                //}
                OffsetKafka = message.Meta.Offset;
                Console.WriteLine("1 Response: Partition {0},Offset {1} : {2}",
                    message.Meta.PartitionId, message.Meta.Offset, message.Value.ToUtf8String());
            }
            //consumer.Dispose();
            //OffsetPosition[] str = tt3.ToArray();
            //var consumer2 = new Consumer(new ConsumerOptions(topic, router), str);
            //foreach (var message in consumer2.Consume())
            //{

            //    Console.WriteLine("2 Response: Partition {0},Offset {1} : {2}",
            //        message.Meta.PartitionId, message.Meta.Offset, message.Value.ToUtf8String());
            //}

        }

        private static string getKafkaBroker()
        {
            string KafkaBroker = string.Empty;
            var KafkaBrokerKeyName = "KafkaBroker";

            if (!ConfigurationManager.AppSettings.AllKeys.Contains(KafkaBrokerKeyName))
            {
                KafkaBroker = "http://localhost:9092";
            }
            else
            {
                KafkaBroker = ConfigurationManager.AppSettings[KafkaBrokerKeyName];
            }
            return KafkaBroker;
        }

        private static string getTopicName()
        {
            string TopicName = string.Empty;
            var TopicNameKeyName = "Topic";

            if (!ConfigurationManager.AppSettings.AllKeys.Contains(TopicNameKeyName))
            {
                throw new Exception("Key \"" + TopicNameKeyName + "\" not found in Config file -> configuration/AppSettings");
            }
            else
            {
                TopicName = ConfigurationManager.AppSettings[TopicNameKeyName];
            }
            return TopicName;
        }
        public static OffsetFetchRequest CreateOffsetFetchRequest(string topic, int partitionId = 0)
        {
            return new OffsetFetchRequest
            {
                ConsumerGroup = "DefaultGroup",
                Topics = new List<OffsetFetch>(new[] 
        		                          {
        		                          	new OffsetFetch
        		                          	{
        		                          		Topic = topic,
        		                          		PartitionId = partitionId
        		                          	}
        		                          })
            };
        }
        public static FetchRequest CreateFetchRequest(string topic, int offset, int partitionId = 0)
        {
            return new FetchRequest
            {
                CorrelationId = 1,
                Fetches = new List<Fetch>(new[]
                        {
                            new Fetch
                                {
                                    Topic = topic,
                                    PartitionId = partitionId,
                                    Offset = offset
                                }
                        })
            };
        }

        public static OffsetRequest CreateOffsetRequest(string topic, int partitionId = 0, int maxOffsets = 1, int time = -1)
        {
            return new OffsetRequest
            {
                CorrelationId = 1,
                Offsets = new List<Offset>(new[]
                        {
                            new Offset
                                {
                                    Topic = topic,
                                    PartitionId = partitionId,
                                    MaxOffsets = maxOffsets,
                                    Time = time
                                }
                        })
            };
        }

        /// <summary>
        /// 控制臺關閉事件
        /// </summary>
        /// <param name="CtrlType"></param>
        /// <returns></returns>
        public delegate bool ControlCtrlDelegate(int CtrlType);
        [DllImport("kernel32.dll")]
        private static extern bool SetConsoleCtrlHandler(ControlCtrlDelegate HandlerRoutine, bool Add);
        private static ControlCtrlDelegate cancelHandler = new ControlCtrlDelegate(HandlerRoutine);

        public static bool HandlerRoutine(int CtrlType)
        {
            switch (CtrlType)
            {
                case 0://Ctrl+C关闭 
                case 2://按控制台关闭按钮关闭
                    //IniFileWr.IniFileSetVal("./config.ini", "topic", "test", OffsetKafka.ToString()); 
                    break;
            }
            //Console.ReadLine();
            return false;
        }  
    }
}
