import pandas as pd

from progress.Pstrat_borrow import BorrowSignalEffect

if __name__ == "__main__":
    bse = BorrowSignalEffect()
    stks = bse.stks2chk()

    report = pd.DataFrame(None)
    for s in stks:
        d = bse.report(s, 5000, -5000)
        t0 = pd.DataFrame(d[0])
        t1 = pd.DataFrame(d[1])

        # return compared to KOSPI200 return
        t = pd.concat([t0[1:], t1[1:]], axis=1)
        t.index = [s]
        report = pd.concat([report, t])

    rpt = report[sorted(list(report.columns))]
    rpt.index = [f"A{stk}" for stk in rpt.index]
    rpt.to_csv('stk_wise_return.csv')

    # Report
    c = rpt.sum()
    cdiv = rpt.shape[0] - rpt.isna().sum()
    (c / cdiv).to_csv('borrow_mp.csv')