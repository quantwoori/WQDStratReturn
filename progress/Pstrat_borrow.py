from dbms.DBmssql import MSSQL
from dbms.DBquant import PyQuantiwise

import pandas as pd

from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict


class BorrowSignalEffect:
    def __init__(self):
        self.server = MSSQL.instance()
        self.qt = PyQuantiwise()
        self.dt = datetime.now() - relativedelta(months=11, days=18)

    def stks2chk(self) -> List:
        s = self.server.select_db(
            database="WSOL",
            schema="dbo",
            column=["stk"],
            table="sig",
            distinct="stk"
        )
        return list(sum(s, ()))

    def get_signal(self, stk:str, sigtyp:str='shortsig_v2_10_5', sigdfmt="%Y-%m-%d"):
        cond = [
            f"date >= '{self.dt.strftime(sigdfmt)}'",
            f"stk = '{stk}'",
            f"sigtyp = '{sigtyp}'"
        ]
        cond = ' and '.join(cond)
        col = ['date', 'sigstren', 'sig']

        d = self.server.select_db(
            database="WSOL",
            schema="dbo",
            column=col,
            table="sig",
            condition=cond
        )
        d = pd.DataFrame(d, columns=col)
        d = d.set_index('date')
        d = d.sort_index()
        d.index = d.index.map(lambda x: f"{x[:4]}{x[5:7]}{x[8:10]}")
        return d

    def match_prc(self, stk, rolling:int, qtdfmt='%Y%m%d'):
        p = self.qt.stk_data(
            stock_code=stk,
            start_date=(self.dt - relativedelta(days=rolling)).strftime(qtdfmt),
            end_date=datetime.now().strftime(qtdfmt),
            item="수정주가"
        )
        p.VAL = p.VAL.astype('float32')
        p.columns = ['date', '_', 'prc']
        p = p.set_index('date')
        p = p.sort_index()
        if rolling > 0:
            return p[['prc']].pct_change().rolling(window=rolling).mean()
        else:
            return p[['prc']].pct_change()

    def match_idx(self, rolling:int, idx='IKS200', qtdfmt='%Y%m%d'):
        i = self.qt.ind_data(
            index_code=idx,
            start_date=(self.dt - relativedelta(days=rolling)).strftime(qtdfmt),
            end_date=datetime.now().strftime(qtdfmt),
            item='종가지수'
        )
        i.VAL = i.VAL.astype('float32')
        i.columns = ['date', '_', 'idx']
        i = i.set_index('date')
        i = i.sort_index()
        if rolling > 0 :
            return i[['idx']].pct_change().rolling(window=rolling).mean()
        else:
            return i[['idx']].pct_change()

    def report(self, stk, high_thres:int=0, low_thres:int=0) -> (Dict, Dict):
        """
        Report Weekly result
        % format
            {date : [return, outperform],}
        """
        s = self.get_signal(stk)
        p = self.match_prc(stk, rolling=0)
        i = self.match_idx(rolling=0)
        res = pd.concat([s, p, i], axis=1)
        res['out'] = res['prc'] - res['idx']
        res = res.sort_index()
        res[['prc', 'idx', 'out']] = res[['prc', 'idx', 'out']].shift(1)

        abnorm_dates_h = res.loc[(res.sig == 'abnormal_high') & (res.sigstren > high_thres)].index
        abnorm_dates_l = res.loc[(res.sig == 'abnormal_low') & (res.sigstren < low_thres)].index
        result_high, result_low = dict(), dict()
        for i in abnorm_dates_h:
            result_high[i] = [res[i:]['prc'][0:5].sum() * -1, res[i:]['out'][0:5].sum() * -1]

        for i in abnorm_dates_l:
            result_low[i] = [res[i:]['prc'][0:5].sum(), res[i:]['out'][0:5].sum()]

        return result_high, result_low


if __name__ == '__main__':
    se = BorrowSignalEffect()
    d = se.report('251270', 5000, -5000)
    t0 = pd.DataFrame(d[0])
    t1 = pd.DataFrame(d[1])

