from scapy.all import sniff
from kafka import KafkaProducer
from scapy.all import hexdump, PcapWriter
import base64


# class stdout(object):
#   def write(self, packet):
#     print("Timestamp {time}, {summary}".format(time=packet.time, summary=packet.summary()))
#     print("Src IP: {src}, Dst IP: {dst}".format(src=packet['IP'].src, dst=packet['IP'].dst))
#     hexdump(packet)
#     print(base64.encodestring(str(packet)))


def callback(p):
  print(p)
  

class stdout(object):
  def write(self, packet):
    print("Timestamp {time}, {summary}".format(time=packet.time, summary=packet.summary()))
    print("Src IP: {src}, Dst IP: {dst}".format(src=packet['IP'].src, dst=packet['IP'].dst))
    hexdump(packet)
    print('£££££££££££££££££££££££££££££')
    print(packet.__class__)
    print(str(packet))
    print(base64.encodebytes(hexdump(packet)))

w = stdout()


  
captured = sniff(iface='', filter='ip', lfilter=lambda x: x.haslayer('IP'),
          prn=w.write, store=0)



##########################################
# basic scapy

def callback(p):
  return base64.encodestring(str(p).encode('utf-8'))

captured = sniff(iface='', filter='ip', lfilter=lambda x: x.haslayer('IP'),
          prn=callback, store=1)
          
          
          

          
###########################################

class KafkaIn(object):

    def __init__(self):
        host = '127.0.0.1'
        port = 9092
        server_string = "{host}:{port}".format(host=host, port=port)
        self.producer = KafkaProducer(bootstrap_servers=server_string)
    
    def write(self, packet):
        packet_bytes = base64.encodestring(str(packet).encode())
        if packet.haslayer('DNS'):
            future = self.producer.send('quickstart-events', packet_bytes)
            # future = self.producer.send('catchall', packet_bytes)
        else:
            future = self.producer.send('quickstart-events', packet_bytes)
        result = future.get(timeout=30)

k = KafkaIn()

captured = sniff(iface='', filter='ip', lfilter=lambda x: x.haslayer('IP'),
          prn=k.write, store=0)
          




          
