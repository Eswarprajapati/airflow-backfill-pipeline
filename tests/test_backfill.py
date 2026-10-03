from contextlib import closing
import tempfile,sqlite3,unittest
from pathlib import Path
from pipeline import load_day,backfill,date_range
class BackfillTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.db=str(Path(self.temp.name)/'test.sqlite')
    def test_backfill_totals(self):
        r=backfill('2026-01-01','2026-01-02',database=self.db)
        self.assertEqual([x['completed_cents'] for x in r],[1200,3000])
    def test_rerun_does_not_duplicate(self):
        for _ in range(2): backfill('2026-01-01','2026-01-02',database=self.db)
        with closing(sqlite3.connect(self.db)) as c:
            self.assertEqual(c.execute('select count(*) from orders').fetchone()[0],4)
    def test_missing_partition_does_not_mark_success(self):
        load_day('2026-01-01',database=self.db)
        with self.assertRaises(FileNotFoundError): load_day('2026-01-03',database=self.db)
        with closing(sqlite3.connect(self.db)) as c: self.assertEqual(c.execute('select count(*) from runs').fetchone()[0],1)
    def test_invalid_partition_preserves_previous_data(self):
        load_day('2026-01-01',database=self.db)
        root=Path(self.temp.name); (root/'2026-01-01.csv').write_text('order_id,amount_cents,status\no1,-10,completed\n')
        with self.assertRaises(ValueError): load_day('2026-01-01',root,self.db)
        with closing(sqlite3.connect(self.db)) as c: self.assertEqual(c.execute('select row_count from runs').fetchone()[0],2)
    def test_date_bounds(self):
        with self.assertRaises(ValueError): list(date_range('2026-01-02','2026-01-01'))
        self.assertEqual(list(date_range('2026-01-01','2026-01-02')),['2026-01-01','2026-01-02'])
