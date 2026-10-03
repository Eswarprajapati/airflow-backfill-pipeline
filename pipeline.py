from contextlib import closing
"""Deterministic partition loading for a local demo and an Airflow DAG."""
import csv,sqlite3,json
from pathlib import Path
from datetime import date,timedelta

def date_range(start,end):
    start=date.fromisoformat(start); end=date.fromisoformat(end)
    if end<start: raise ValueError('End date precedes start date')
    if (end-start).days>366: raise ValueError('Backfill is limited to 367 days')
    while start<=end:
        yield start.isoformat(); start+=timedelta(days=1)

def load_day(day,source_root='data',database='warehouse.sqlite'):
    date.fromisoformat(day)
    file=Path(source_root)/(day+'.csv')
    if not file.is_file(): raise FileNotFoundError('Missing source partition: '+day)
    with file.open(newline='') as f:
        reader=csv.DictReader(f)
        if set(reader.fieldnames or [])!={'order_id','amount_cents','status'}:
            raise ValueError('Unexpected schema')
        rows=list(reader)
    parsed=[]; seen=set()
    for row in rows:
        amount=int(row['amount_cents'])
        if not row['order_id'] or row['order_id'] in seen or amount<0 or row['status'] not in {'completed','cancelled'}:
            raise ValueError('Invalid or duplicate order in '+day)
        seen.add(row['order_id']); parsed.append((day,row['order_id'],amount,row['status']))
    with closing(sqlite3.connect(database)) as con, con:
        con.execute('CREATE TABLE IF NOT EXISTS orders(day TEXT,order_id TEXT,amount_cents INTEGER,status TEXT,PRIMARY KEY(day,order_id))')
        con.execute('CREATE TABLE IF NOT EXISTS runs(day TEXT PRIMARY KEY,row_count INTEGER,total_cents INTEGER)')
        # Replacement plus state update is one transaction; reruns do not append duplicates.
        con.execute('DELETE FROM orders WHERE day=?',[day])
        con.executemany('INSERT INTO orders VALUES (?,?,?,?)',parsed)
        total=sum(r[2] for r in parsed if r[3]=='completed')
        con.execute('INSERT OR REPLACE INTO runs VALUES (?,?,?)',[day,len(parsed),total])
    return {'day':day,'rows':len(parsed),'completed_cents':total}

def backfill(start,end,source_root='data',database='warehouse.sqlite'):
    return [load_day(day,source_root,database) for day in date_range(start,end)]
