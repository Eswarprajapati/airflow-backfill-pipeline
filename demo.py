import argparse,json
from pipeline import backfill
if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--start',default='2026-01-01'); p.add_argument('--end',default='2026-01-02')
    args=p.parse_args(); print(json.dumps(backfill(args.start,args.end),indent=2))
