"""

  """

##


import pandas as pd
from githubdata import GithubData
from githubdata.main import _clean_github_url as cln_github_url
from mirutil import utils as mu
from mirutil.df_utils import read_data_according_to_type as read_data
from mirutil.df_utils import save_as_prq_wo_index as sprq
from persiantools.jdatetime import JalaliDate


class ReposAddresses :
  stch = 'imahdimir/d-status-changes'
  targ = 'imahdimir/d-clean-d-firm-status-change'

ra = ReposAddresses()

class ColNames :
  id = 'TSETMC_ID'
  jdt = 'JDateTime'
  d = 'Date'
  jd = 'JDate'
  ns = 'NewStatus'
  iso = 'iso'
  dt = 'DateTime'
  t = 'Time'
  r = 'Row'

c = ColNames()

def main() :
  pass
  ##
  rp_stch = GithubData(ra.stch)
  ##
  rp_stch.clone()
  ##
  dsfp = rp_stch.data_fp
  ds = read_data(dsfp)
  ##
  ds = ds.sort_values([c.id , c.jdt , c.r] , ascending = [False , False , True])
  ##
  msk = ds.duplicated([c.id , c.jdt] , keep = False)
  df1 = ds[msk]
  len(df1)
  ##
  ds = ds.drop_duplicates([c.id , c.jdt] , keep = 'first')
  ##
  ds[c.jd] = ds[c.jdt].str[:10]
  ##
  ds[c.jd] = ds[c.jd].apply(lambda x : JalaliDate.fromisoformat(x))
  ##
  ds[c.d] = ds[c.jd].apply(lambda x : x.to_gregorian())
  ##
  ds[c.iso] = ds[c.d].astype(str) + ds[c.jdt].str[10 :]
  ##
  ds[c.dt] = pd.to_datetime(ds['iso'] , format = '%Y-%m-%dT%H:%M:%S')
  ##
  ds = ds[[c.id , c.jdt , c.dt , c.ns]]
  ##
  rp_targ = GithubData(ra.targ)
  rp_targ.clone()
  ##
  dsfp = rp_targ.data_fp
  ##
  sprq(ds , dsfp)
  ##
  cur_url = cln_github_url(rp_targ.user_name + '/b-' + rp_targ.repo_name)
  ##
  msg = 'builded by:'
  msg += ' ' + cur_url
  ##
  tokp = '/Users/mahdi/Dropbox/tok.txt'
  tok = mu.get_tok_if_accessible(tokp)
  ##
  rp_targ.commit_and_push(msg , user = rp_targ.user_name , token = tok)

  ##

  rp_stch.rmdir()
  rp_targ.rmdir()

  ##

##


if __name__ == '__main__' :
  main()
  print('done')


##