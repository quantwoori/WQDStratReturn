from dbms.DBmssql import MSSQL
from dbms.DBquant import PyQuantiwise

import pandas as pd

from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict


class PairSignalEffect:
    def __init__(self):
        self.server = MSSQL.instance()
        self.qt = PyQuantiwise()
        self.dt = datetime.now() - relativedelta(months=1, days=18)

    def stks2chk(self, signame_like=r'%pt_v1_%') -> List:
        cnd = f"sigtyp like '{signame_like}'"
        s = self.server.select_db(
            database="WSOL",
            schema="dbo",
            column=["stk"],
            table="sig",
            distinct='stk',
            condition=cnd
        )
        return list(sum(s, ()))

    def sigs2chk(self) -> List:
        s = self.server.select_db(
            database="WSOL",
            schema="dbo",
            column=["sigtyp"],
            table="sig",
            distinct="sigtyp"
        )
        return [sigs for sigs in sum(s, ()) if 'pt_v1_' in sigs]

    def get_signal_pairs(self, signal:str, longshort:str, sigdfmt:str="%Y-%m-%d"):
        cond = [
            f"date >= '{self.dt.strftime(sigdfmt)}'",
            f"sigtyp = '{signal}'"
        ]
        if longshort == 'long':
            cond.append("sig = 'spread_buy'")
        else:
            cond.append("sig = 'spread_sell'")
        cond = ' and '.join(cond)
        col = ['date', 'sigstren', 'sig', 'stk']
        d = self.server.select_db(
            database="WSOL",
            schema="dbo",
            column=col,
            table="sig",
            condition=cond
        )
        d = pd.DataFrame(d, columns=col)
        d['date'] = d['date'].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d").strftime('%Y%m%d')
        )
        d = d.set_index('date')
        d = d.sort_index()
        return d

    def match_prc(self, stk, rolling:int, qtdfmt='%Y%m%d'):
        p = self.qt.stk_data(
            stock_code=stk,
            start_date=(self.dt - relativedelta(days=rolling)).strftime(qtdfmt),
            end_date=datetime.now().strftime(qtdfmt),
            item='수정주가'
        )
        p.VAL = p.VAL.astype('float32')
        p.columns=['date', '_', 'prc']
        p = p.set_index('date')
        p = p.sort_index()

        if rolling > 0:
            return p[['prc']].pct_change().rolling(window=rolling).mean()
        else:
            return p[['prc']].pct_change()


if __name__ == "__main__":
    pse = PairSignalEffect()
    r = pse.stks2chk()
    rs = pse.sigs2chk()

    longshort = pd.DataFrame(None)
    for pair_sig in rs:
        rss_l = pse.get_signal_pairs(pair_sig, 'long')
        rss_s = pse.get_signal_pairs(pair_sig, 'short')
        rss_lp = pse.match_prc(rss_l.stk[0], 0)
        rss_sp = pse.match_prc(rss_s.stk[0], 0)

        longs = pd.concat([rss_l, rss_lp], axis=1).dropna()
        shorts = pd.concat([rss_s, rss_sp], axis=1).dropna()

        longshort[pair_sig] = longs['prc'] - shorts['prc']
