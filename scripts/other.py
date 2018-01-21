#!/etc/env python

import sys
import hashlib

def md5_encode(text):
    text = text.strip().encode('utf8')
    m = hashlib.md5()
    m.update(text)
    text_md5 = m.hexdigest()
    return(text_md5)

for line in sys.stdin:
    text=line.strip()
    text_md5=md5_encode(text)
    print '\t'.join([text,text_md5])