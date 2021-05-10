using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
namespace KafkaLib
{
    public class KafkaConsumer
    {
        private string _ipStr;
        private IPAddress _ipAddress;
        private int _port;
        private IPEndPoint _localEndPoint ;
        private TcpClient _client;
        private KafkaConnection kConnection;
        public bool IsConnectOk;
        public string ConnectStatus;//-1,1,2
        public KafkaConsumer(string ip, int port)
        {
            try
            {
                _ipAddress=IPAddress.Parse(ip);
            }
            catch (Exception)
            {
                
            }
            _port = port;
            _localEndPoint = new IPEndPoint(_ipAddress, _port);
        }
        public void Connect()
        {
            
            

            // Connect to the remote endpoint
            try
            {
                _client = new TcpClient() { ReceiveTimeout = 3000, SendTimeout = 1000 };
                //_client.ReceiveTimeout = -1;//设置超时期限无限大
                _client.ConnectAsync(_localEndPoint.Address, _port).ConfigureAwait(false);
                
                // IsConnectOk = _clientSocket.Connected;
                IsConnectOk = _client.Connected;
                ConnectStatus = "3";
                //_socketList.Add(_clientSocket);//存储socket
            }
            catch (Exception ex)
            {
                IsConnectOk = false;
            }
        }
        public void MessageRead()
        {

        }
        public void DisConnect()
        {
            try
            {

                if (_client != null)
                {
                    
                    
                    _client.Close();
                    //_client.Dispose();
                    IsConnectOk = false;
                    ConnectStatus = "-1";
                }
                
                
            }
            catch (Exception e)
            {
                // Log.Instance().AddLog(string.Format("{0} 斷開CNC連接失敗{1}", DeviceIp, e.Message));
                //_logTxt.RecordLog(DeviceName, e.Message);
            }
        }
    }
}
