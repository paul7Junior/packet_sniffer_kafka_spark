""""
Sniffer + Kafka producer 

Sniff packet and push it to topic with Kafka Producer
""""

from scapy.all import sniff
from kafka import KafkaProducer
from scapy.all import hexdump, PcapWriter
import base64
from json import dumps

   
###########################################

    
def packet_to_json(packet):
    packet_dict = {}
    for line in packet:
        if '###' in line:
            layer = line.strip('#[] ')
            packet_dict[layer] = {}
        elif '=' in line:
            key, val = line.split('=', 1)
            packet_dict[layer][key.strip()] = val.strip()
    print(dumps(packet_dict))
    return dumps(packet_dict)
  
class KafkaProducer(object):

    def __init__(self):
        host = '127.0.0.1'
        port = 9092
        server_string = "{host}:{port}".format(host=host, port=port)
        self.producer = KafkaProducer(bootstrap_servers=server_string,
        value_serializer=lambda x: dumps(packet_to_json(x)).encode())
        
        
    def write(self, packet):
        packet = packet.show2(dump=True).split('\n')
        future = self.producer.send('quickstart-events', packet)
        result = future.get(timeout=30)

k = KafkaProducer()

captured = sniff(iface='', filter='ip', lfilter=lambda x: x.haslayer('IP'),
          prn=k.write, store=1)
          

          
