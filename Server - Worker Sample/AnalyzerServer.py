import rpyc
import os, subprocess, json
import time
from threading import Thread
from kafka import KafkaProducer


SERVER_IP = "IP_ADDRESS"


class AnalyzerService(rpyc.Service):
    class exposed_Analyzer(object):   # exposing names is not limited to methods :)
        def __init__(self, item, info, callback):
            self.filename = item["itemName"]
            self.db_id = item["id"]
            self.app_id = item["appId"]
            self.app_version = item["version"]
            self.app_path = self.get_item_path(item["path"], item["itemName"])
            # self.analyze_path = self.get_item_path(item["path"], item{"itemName"})
            self.kafka_producer = self.connect_kafka_producer(info["kafka_server"])
            self.kafka_topic = info["kafka_topic"]
            self.callback = rpyc.async_(callback)   # create an async callback
            self.active = True
            self.thread = Thread(target=self.work)
            self.thread.start()

        def stop(self):   # this method has to be exposed too
            self.active = False
            print("stopped")

        def work(self):
            while self.active:
                try:
                    print(self.filename, self.app_id, self.app_path)
                    analyze_output = subprocess.check_output(["reverse-apk-test", self.app_path])

                    if len(analyze_output) > 0:
                        analyze_result = self.parser(analyze_output)
                        
                        self.publish_message(self.kafka_producer, self.kafka_topic, "analyze_result", analyze_result)
                        
                        self.callback(self.db_id, True)
                        
                    self.stop()
                except Exception as e:
                    print(e)
                    self.callback(self.db_id, False)
                    self.stop()

        def publish_message(self, producer_instance, topic_name, key, value):
            try:
                key_bytes = bytes(key, encoding='utf-8')
                value_bytes = bytes(value, encoding='utf-8')
                producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
                producer_instance.flush()
                print('Message published successfully.')
            except Exception as ex:
                print('Exception in publishing message')
                print(str(ex))

        def connect_kafka_producer(self, kafka_server):
            _producer = None
            try:
                _producer = KafkaProducer(bootstrap_servers=[kafka_server], api_version=(0, 10))
            except Exception as ex:
                print('Exception while connecting Kafka')
                print(str(ex))
            finally:
                return _producer

        def parser(self, raw_result):
            report_line = {}
            all_report = []
            parsed_result = {}
            lines = raw_result.decode().split("[!{tail}!]")[:-1]
            for line in lines:
                headers, content = line.split("[!{head}!]")
                report_line["head"] = headers
                report_line["content"] = content
                if content != "\n":
                    all_report.append(report_line.copy())
            parsed_result["title"] = self.filename
            parsed_result["app_id"] = self.app_id
            parsed_result["report"] = all_report.copy()
            parsed_result = json.dumps(parsed_result)
            return parsed_result

        def get_item_path(self, path, item_name):
            # Check if file in space or cosmos
            item_dir = self.app_id + "-" + self.app_version
            space_path = os.path.join(path, item_dir)
            cosmos_path = os.path.join("/root/cosmos", item_dir)

            if os.path.isdir(space_path):
                return os.path.join(space_path, item_name+".apk")
            elif os.path.isdir(cosmos_path):
                return os.path.join(cosmos_path, item_name+".apk")
            else:
                print("File not found!!")

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    ThreadedServer(AnalyzerService, hostname=SERVER_IP, port=18871).start()
