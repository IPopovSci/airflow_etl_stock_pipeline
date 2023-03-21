import json
import os
from dotenv import load_dotenv

load_dotenv()
data_type_list=['stocks','news']
symbol_list=json.loads(os.environ['symbol'])
