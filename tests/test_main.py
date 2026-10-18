import datetime as dt
import io
import lzma
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch
import main

class Tests(unittest.TestCase):
    day = dt.date(2018, 4, 12)
    def payload(self, ms=10, ask=123456, bid=123450):
        return lzma.compress(struct.pack('>3i2f', ms, ask, bid, 1.0, 2.0))
    def test_zero_based_month(self):
        self.assertIn('/2018/03/12/12h_ticks.bi5',main.hour_url('EURUSD',self.day,12))
    def test_decode_and_bin_layout(self):
        rows=main.decode_hour(self.payload(),self.day,12,100000)
        self.assertAlmostEqual(rows[0][1],1.2345)
        self.assertAlmostEqual(rows[0][2],1.23456)
        b=main.encode_block(rows)
        self.assertEqual(len(b),28)
        self.assertEqual(struct.unpack('<i',b[:4])[0],1)
        self.assertEqual(struct.unpack('<q',b[4:12])[0],1523534400010)
        self.assertAlmostEqual(struct.unpack('<d',b[12:20])[0],1.2345)
    def test_reject_html(self):
        with self.assertRaises(lzma.LZMAError):main.decode_hour(b'<html>error</html>',self.day,1,1000)
    def test_reject_bad_record_length(self):
        with self.assertRaises(ValueError):main.decode_hour(lzma.compress(b'abc'),self.day,1,1000)
    def test_reject_bad_timestamp(self):
        with self.assertRaises(ValueError):main.decode_hour(self.payload(ms=3600000),self.day,1,1000)
    def test_reject_crossed_quote(self):
        with self.assertRaises(ValueError):main.decode_hour(self.payload(ask=1,bid=2),self.day,1,1000)
    def test_empty(self):self.assertEqual(main.decode_hour(b'',self.day,1,1000),[])
    def test_index_preserved(self):
        text='BRIDX1\nSYM|EURUSD|Custom description|0|5\nFILE|EURUSD/a.BIN|2018-04-12|28\n'
        s,e=main.parse_index(text)
        self.assertIn('Custom description',s['EURUSD']);self.assertEqual(len(e),1)
    def test_unknown_index_fails_safe(self):
        with self.assertRaises(ValueError):main.parse_index('BRIDX1\nUNKNOWN|value\n')
    def test_partial_day_never_published(self):
        def fake(s,d,h,c,cache):return {'hour':h,'state':'failed'} if h==5 else {'hour':h,'state':'ok','rows':[(h*3600000,1.0,1.1)]}
        with tempfile.TemporaryDirectory() as t, patch('main.fetch_hour',side_effect=fake):
            p,r=main.build_day('EURUSD',self.day,{'workers':2},Path(t)/'cache',Path(t)/'out')
            self.assertFalse(p.exists());self.assertFalse(r['upload_eligible'])
    def test_202_is_failure(self):
        class Response(io.BytesIO):
            code=202
            headers={}
        cfg={'instruments':{'EURUSD':{'price_divisor':100000}},'attempts':1,'timeout':1}
        with tempfile.TemporaryDirectory() as t,patch('urllib.request.urlopen',return_value=Response(b'')):
            r=main.fetch_hour('EURUSD',self.day,0,cfg,Path(t))
            self.assertEqual(r['state'],'failed');self.assertEqual(r['status'],202)
    def test_retry_after(self):self.assertEqual(main.retry_delay('20',0),20)

if __name__=='__main__':unittest.main()
