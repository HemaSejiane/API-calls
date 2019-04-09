from pypardot.client import PardotAPI
import json
from pprint import pprint
import pandas as pd
from pandas.io.json import json_normalize

p = PardotAPI(email='',password='',user_key='')
data=p.prospects.query(created_after='yesterday')
dataprospect=data['prospect']
df=pd.DataFrame(json_normalize(dataprospect),columns=['email','id','first_name','last_name'])
export_csv = df.to_csv ('pth to target file location', index = None, header=True)


