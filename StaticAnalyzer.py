import mysql.connector
import os, subprocess, threading, rpyc, time, sys


# Enum Class for worker machines
class INFO:
    WORKER = [{'ip': "IP_ADDRESS", 'port': 18871, 'ready': 1, 'analyzing_item': ""},
              {'ip': "IP_ADDRESS", 'port': 18871, 'ready': 1, 'analyzing_item': ""}]
    #WORKER = [{'ip': "localhost", 'port': 18871}, ]
    SERVER = {'kafka_server': "elastic.analyzx.com:9092", 'elastic': "", 'kafka_topic': "StaticAnalyzeOutput"}
    #WORKER2 = {'ip': "ip", 'port': 18871}
    #WORKER3 = {'ip': "ip", 'port': 18871}


class Analyzer:
    def __init__(self, server, workers=None, thread_count=1):
        self.lastid = 0
        self.conn = None
        self.workers = workers
        self.thread_count = thread_count
        self.analyzed_ids = []
        #self.base_file_path = "/root/static_analyzer"
        self.base_file_path = "/root/space/apks"
        self.base_file_path2 = "/root/cosmos/apks"
        self.item_list = []
        self.item_count = 1

    def get_id_list(self):
        # get id list from db for the count of [itemcount]
        self.conn = mysql.connector.connect("CONNECTION") # open connection to db
        item_list = {}
        self.item_list = []
        print("getting id list")
        try:
            cursor = self.conn.cursor()
            query = "QUERY"
            cursor.execute(query, (self.lastid, self.item_count))
            result = cursor.fetchall()
            # print(result)
            for item in result:
                item_list = item
                self.item_list.append(item_list.copy())
            #self.lastid = self.lastid + self.itemcount
            print("id list ready")
            return True
        except Exception as e:
            print(e)
            cursor.close()
            self.conn.close()
            return False

    def save_result_to_db(self, item_id):
        # Save the analysis result to db
        try:
            cursor = self.conn.cursor()
            query = "QUERY"
            cursor.execute(query)
            self.conn.commit()
            get_last_id = "QUERY"
            cursor.execute(get_last_id)

            result = cursor.fetchall()
            for id in result:
                last_id = id[0]
            self.update_id_from_db(item_id, last_id)
        except Exception as e:
            print(e)

    def update_id_from_db(self, item_id, last_id):
        # Update the analysis id of given id
        try:
            print(last_id,item_id)
            cursor = self.conn.cursor()
            query = "UPDATE QUERY"
            cursor.execute(query, (last_id, item_id))
            self.conn.commit()
        except Exception as e:
            print(e)

    def analyze_item(self, item):
        # analyze the given item
        try:
            item_path = os.path.join(item["path"], item["itemName"]+".apk")
            analyze_output = subprocess.check_output(["reverse-apk", item_path])
            analyze_path = os.path.join(item["path"], item["itemName"]+".txt")
            with open(analyze_path, "w") as analyze_file:
                analyze_file.write(analyze_output)
        except Exception as e:
            print(e)

    def remote_analyzer(self, item, worker):
        # Start the worker machines and analyze on them
                print(item)
                conn = rpyc.connect(worker["ip"], worker["port"])
                bgsrv = rpyc.BgServingThread(conn)
                a = conn.root.Analyzer(item, INFO.SERVER, self.on_analyze_finished)
                worker["ready"] = 0
                worker["analyzing_item"] = item["id"]

    def on_analyze_finished(self, item_id, status):
        #print(item)
        # with open(item+".txt", "wb")as analyze_file:
        #     analyze_file.write(analyze_result)
        if status:
            self.save_result_to_db(item_id)
            print("Analyze is done!! :%s" % item_id)
        else:
            print("Analyze return error. :%s" % item_id)
        self.analyzed_ids.append(item_id)
        for worker in self.workers:
            if item_id == worker["analyzing_item"]:
                worker["ready"] = 1
        for item in self.item_list:
            if item_id == item["id"]:
                item["done"] = 1

    def run(self):
        while True:
            if not any(item["done"] == 0 for item in self.item_list):
                self.get_id_list()
            for item in self.item_list:
                if item["done"] == 0:
                    self.call_worker(item)

    def call_worker(self, item):
        if self.workers is not None:
            for worker in self.workers:
                print(worker)
                if worker["ready"]:
                    print("ready")
                    self.remote_analyzer(item, worker)


if __name__ == "__main__":
    start_time = time.time()
    analyzer = Analyzer(INFO.SERVER, INFO.WORKER)
    analyzer.run()