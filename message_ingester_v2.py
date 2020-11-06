import os, json, socket
from datetime import datetime
from uuid import uuid4
import pandas as pd
import pyarrow as pa
from websocket import create_connection, WebSocket

class MyWebSocket(WebSocket):
    def recv_frame(self):
        frame = super().recv_frame()
        return frame

class CustomWSListener:
    def __init__(self, host, port, endpoint=None):
        self.msg_counter = 0
        self.msg_buffer = []
        uri = f"ws://{host}:{port}/{endpoint}" if endpoint else f"ws://{host}:{port}/"
        self.ws = create_connection(uri, sockopt=((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),), class_=MyWebSocket)

    def custom_reducer(self):
        try:
            now = datetime.now()
            uid = str(uuid4())
            df = pd.DataFrame({
                'Timestamp': [now.isoformat()],
                'Result' : [sum([x['data']['amount'] for x in self.msg_buffer ])],
                'UID' : [uid]
            })
            table = pa.Table.from_pandas(df)
            path = f"{os.getcwd()}/{now.year}/{now.month}/{now.day}/{now.hour}/{uid}/data.parquet"
            print(f"Writting data to: {path}")
            directory = os.path.dirname(path)
            if not os.path.exists(directory): os.makedirs(directory)
            pa.pq.write_table(table, path)
        except Exception as e:
            print(e)

    def run(self):
        while True:
            msg = self.ws.recv_data()[1]
            print('.')
            json_msg = json.loads(msg)
            if json_msg['data']['organization'] == 'bank' and json_msg['data']['credit_score'] == 1:
                self.msg_buffer.append(json_msg)
                if self.counter < 1000:
                    self.msg_counter += 1
                else:
                    self.custom_reducer()
                    self.msg_counter = 0
                    self.msg_buffer = []
            else:
                pass

if __name__ == '__main__':
    print("Starting Listener...")
    listener = CustomWSListener(host='localhost', port=9090, endpoint='data')
    listener.run()