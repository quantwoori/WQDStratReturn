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

    def get_signal(self, stk:str, sigtyp_like:str=r'%pt_v1_%', sigdfmt:str="%Y-%m-%d"):
        cond = [
            f"date >= '{self.dt.strftime(sigdfmt)}'"
            f"stk = '{stk}'"
            f"sigtyp"
        ]


if __name__ == "__main__":
    pse = PairSignalEffect()
    r = pse.stks2chk()