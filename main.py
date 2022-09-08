"""

  """

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq
from persiantools.jdatetime import JalaliDate


class GDUrl :
    cur = 'https://github.com/imahdimir/u-d-firm-status-changes'
    src = 'https://github.com/imahdimir/d-0-firm-status-change'
    trg = 'https://github.com/imahdimir/d-firm-status-changes'

gu = GDUrl()

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
    gd_src = GithubData(gu.src)
    gd_src.overwriting_clone()
    ##
    ds = gd_src.read_data()
    ##
    c2s = [c.id , c.jdt , c.r]
    ds = ds.sort_values(c2s , ascending = [False , False , True])
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
    ds = ds.astype(str)

    ##

    gd_trg = GithubData(gu.trg)
    gd_trg.overwriting_clone()
    ##
    dtrp = gd_trg.data_fp
    sprq(ds , dtrp)
    ##

    msg = 'date updated by:'
    msg += gu.cur
    ##
    gd_trg.commit_and_push(msg)

    ##


    gd_src.rmdir()
    gd_trg.rmdir()


    ##

##
if __name__ == '__main__' :
    main()
    print('done')


##
