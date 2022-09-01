"""

  """
##


from datetime import time

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import read_data_according_to_type as read_data
from persiantools.jdatetime import JalaliDate


stch_url = 'imahdimir/d-status-changes'

cjdt = 'JDateTime'
cjd = 'JDate'
cd = 'Date'
row = 'Row'
cdt = 'DateTime'
cid = 'TSETMC_ID'
cnews = 'NewStatus'
ciso = 'iso'
ctime = 'Time'

market_start_time = time(9 , 0 , 0)
market_end_time = time(12 , 30 , 0)

def main() :
  pass
  ##
  rp_stch = GithubData(stch_url)
  ##
  rp_stch.clone()
  ##
  dsfp = rp_stch.data_fp
  ds = read_data(dsfp)
  ##
  ds = ds.sort_values([cid , cjdt , row] , ascending = [False , False , True])
  ##
  ds = ds.drop_duplicates([cid , cjdt] , keep = 'first')
  ##
  ds[cjd] = ds[cjdt].str[:10]
  ##
  ds[cjd] = ds[cjd].apply(lambda x : JalaliDate.fromisoformat(x))
  ##
  ds[cd] = ds[cjd].apply(lambda x : x.to_gregorian())
  ##
  ds[ciso] = ds[cd].astype(str) + ds[cjdt].str[10 :]
  ##
  ds[cdt] = pd.to_datetime(ds['iso'] , format = '%Y-%m-%dT%H:%M:%S')
  ##
  ds = ds[[cid , cdt , cnews]]

  ##

##


if __name__ == '__main__' :
  main()
  print('done')


##