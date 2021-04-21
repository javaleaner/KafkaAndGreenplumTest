using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Configuration;
namespace KafkaLib
{
    public class KafkaProducer
    {
        private delegate long WriteMsg(string[]msg);
        public delegate void Log(string type,string info);
        
        private string _brokerUri;
        private string _topic;
        private Producer client;
        private List<Thread> threadList;
        private readonly WriteMsg _sendMassage;
        public Log WriteLog;
        public KafkaProducer(string brokerUri,string topic)
        {
            _brokerUri = brokerUri;
            _topic = topic;
            IniClient();
            threadList=new List<Thread>();
            _sendMassage += Produce;
        }
        /// <summary>
        /// 初始化producer
        /// </summary>
        private void IniClient()
        {
            string[] temp = _brokerUri.Split(',');
            Uri[] url = new Uri[3];
            int index = 0;
            foreach (string item in temp)
            {
                url[index] = new Uri(item);
                index++;
            }
            var options = new KafkaOptions(url);
            var router = new BrokerRouter(options);
            client = new Producer(router);
        }

        public void SendMsg(string[] msg)
        {
            _sendMassage.BeginInvoke(msg, MsgCallBack, msg);
            
        }

        public void MsgCallBack(IAsyncResult iar)
        {
            AsyncResult ar = (AsyncResult) iar;
            var temp = (WriteMsg) ar.AsyncDelegate;
            long a=temp.EndInvoke(iar);
            string[] msgs = (string[]) iar.AsyncState;
            WriteLog(_topic, string.Format("<{0}><{1}>",a,msgs[0]));
        }
        /// <summary>
        /// 發送消息
        /// </summary>
        /// <param name="msg"></param>
        public long Produce(string[] msg)
        {
            //string[] msg = (string[])msgObj;
            long msgOffset = 0;
            var currentDatetime = DateTime.Now;
            var key = currentDatetime.Second.ToString();
            var events = new[] { new Message(msg[0] + currentDatetime.ToString("MMddHHmmssfff"), key) };
            short ackTag = 0;
            TimeSpan tspan = new TimeSpan(20);
            Task<List<ProduceResponse>> x = client.SendMessageAsync(_topic, events, 1, tspan);//.Wait(2000);
            //當服務端連接不上時，停留在此位置，服務端開啟時，自動循環執行；
            bool result = x.IsCompleted;
            int cnt = 0;//計數
            while (!x.IsCompleted)
            {
                cnt++;
                try
                {
                    var a = x.Result;
                    //bool result = x.IsCompleted;
                    foreach (ProduceResponse p in x.Result)
                    {
                        msgOffset = p.Offset;
                        //Console.WriteLine("Write success,offset{0}", re);
                    }
                    //x.Wait(2000);
                    //Console.WriteLine("Produced: Key: {0}. Message: {1}", key, events[0].Value.ToUtf8String());
                }
                catch (Exception)
                {


                }
                

            }
            return msgOffset;



            //using (client) { }
        }

        public void StopSend()
        {
            client.Stop();
        }
        /*
        public  string GetKafkaBroker()
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
        public string getTopicName()
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
         */
    }
}
