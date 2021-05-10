using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Configuration;

namespace producer
{
    /*
     *生產者發送消息
     */
    class Program_old
    {
        static void Main1(string[] args)
        {
            int MsgCnt = 0;
            do
            {
                MsgCnt++;
                Produce(GetKafkaBroker(), getTopicName());
                //System.Threading.Thread.Sleep(3000);
                if (MsgCnt==1)
                {
                    break;
                }
            } while (true);
            
        }

        private static void Produce(string broker, string topic)
        {
            
            string[] temp = broker.Split(',');
            Uri[] url = new Uri[3];
            int index = 0;
            foreach (string item in temp)
            {
                url[index] = new Uri(item);
                index++;
            }
            var options = new KafkaOptions(url);
            var router = new BrokerRouter(options);
            var client = new Producer(router);

            
            Message[] events = new Message[20];
            for (int i = 0; i < 10; i++)
            {
                var currentDatetime = DateTime.Now;
                var key = currentDatetime.Second.ToString() + currentDatetime.Millisecond;
                events[i] = new Message("Hello World " + currentDatetime.ToString("HHmmssfff"), key)
                {
                    Meta = new MessageMetadata() { PartitionId = 0 }
                }
                ;
            }
            short ackTag = 0;
            TimeSpan tspan=new TimeSpan(20);
            Console.WriteLine(DateTime.Now.ToString("HHmmssfff")+"start write");
            Task<List<ProduceResponse>> x = client.SendMessageAsync(topic, events, 1, tspan);//.Wait(2000);
            //當服務端連接不上時，停留在此位置，服務端開啟時，自動循環執行；
            bool result = x.IsCompleted;
            int cnt = 0;//計數
            while (!x.IsCompleted)
            {
                cnt ++;
                try
                {
                    var a = x.Result;
                    //bool result = x.IsCompleted;
                    foreach (ProduceResponse p in x.Result)
                    {
                        long re = p.Offset;
                        //Console.WriteLine("Write success,offset{0}", re);
                    }
                    //x.Wait(2000);
                    //Console.WriteLine("Produced: Key: {0}. Message: {1}", events[0].Key, events[0].Value.ToUtf8String());
                }
                catch (Exception ea)
                {
                    Console.WriteLine(ea.ToString());

                }
                if (cnt == 2)
                {
                    Console.WriteLine("循環內執行{0}次，{1}", cnt, events[0].Value.ToUtf8String());
                }

            }
            Console.WriteLine(DateTime.Now.ToString("HHmmssfff") + "end write");
            Console.ReadLine();
            if (!x.IsCompleted)
            {
                Console.WriteLine("方法未結束");
            }
            
            
            using (client) { }
        }

        private static string GetKafkaBroker()
        {
            string KafkaBroker = string.Empty;
            const string kafkaBrokerKeyName = "KafkaBroker";

            if (!ConfigurationManager.AppSettings.AllKeys.Contains(kafkaBrokerKeyName))
            {
                KafkaBroker = "http://localhost:9092";
            }
            else
            {
                KafkaBroker = ConfigurationManager.AppSettings[kafkaBrokerKeyName];
            }
            return KafkaBroker;
        }
        private static string getTopicName()
        {
            string TopicName = string.Empty;
            const string topicNameKeyName = "Topic";

            if (!ConfigurationManager.AppSettings.AllKeys.Contains(topicNameKeyName))
            {
                throw new Exception("Key \"" + topicNameKeyName + "\" not found in Config file -> configuration/AppSettings");
            }
            else
            {
                TopicName = ConfigurationManager.AppSettings[topicNameKeyName];
            }
            return TopicName;
        }
    }
}
