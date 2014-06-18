# -*- coding: utf-8 -*-

# --------------
# Revise picURL
# --------------

import os, csv
from ftplib import FTP

ACTION = '*Action(SiteID=Italy|Country=IT|Currency=EUR|Version=745|CC=UTF-8)'

class EbayFx(csv.DictWriter):
    '''build csv fx format files'''
    def __init__(self, filename, fieldnames):
        self.fobj = open(filename, 'wb')
        csv.DictWriter.__init__(self, self.fobj, fieldnames, delimiter=';', quotechar='"')
        self.writeheader()
    def close(self,):
        self.fobj.close()
       
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self.close()

        
ftp = FTP('ftp.arredo-luce.com')
ftp.login('arredo-luce@elettrone1.com', 'AS84(x{,]S51')

img_fnames = []
noimg_fnames = []
for el in ftp.nlst('photos'):
    fn, ext = os.path.splitext(el)
    try:
        if len(fn) != 7: raise
        int(fn)
        img_fnames.append(el)
    except:
        noimg_fnames.append(el)

user_in = ''
if len(noimg_fnames) > 3:
    print 'You may want to fix this files:'
    for fn in noimg_fnames:
        if fn != 'index.php' and fn !='.' and fn!='..': print fn
    print
    user_in = raw_input('continue? (x to exit)')


if user_in.lower() != 'x':
    # Fx action revise picurl
    smartheaders = (ACTION, 'ItemID', 'PicURL')
    with EbayFx('revise_picurl.csv', smartheaders) as wrt:
        for el in img_fnames:
    
        csvf = os.path.join('csv', 'input', 'picurl.csv')
        with open(csvf, 'wb') as csvobj:
            W = csv.writer(csvobj, delimiter=';', quotechar='"')
            for fn in img_fn_list:
                W.writerow([fn,])

        u = UnicodeConverter('utf8')

        # counting 
        fdb = os.path.join('dbs', 'mem.db')
        mem = anydbm.open(fdb, 'c')
        mem['revise_picurl'] = str(int(mem['revise_picurl'])+1)
        nn = mem['revise_picurl']



        # Carica SF report nel db
        fcsv = get_f('FileEx', os.path.join('csv', 'input'))
        if fcsv:
            sfreport = dbTable(
            name = "Sfreport",
            cols_def = [('GaCode','TEXT PRIMARY KEY NOT NULL'),
                        ('Tipo', 'TEXT'),
                        ('ItemID', 'TEXT'),
                        ('Title', 'TEXT'),
                        ('CategEbNum', 'TEXT'),
                        ('Price', 'REAL'),
                        ('QtyOnline', 'INTEGER'),])

            def line_controller(row):
                def tipo(s):
                    return s[-2:] if '-' in s else ''
                return (u.de(row[1])[:7],               # GaCode
                        tipo(u.de(row[1])),             # Tipo
                        u.de(row[0]),                   # ItemID
                        u.de(row[13]),                  # Title
                        u.de(row[15]),                  # Categ ebay
                        price(u.de(row[8]).split()[1]), # prezzo
                        qty(row[5]),)                   # QtyOnline

            sfreport.reset()                
            sfreport.load(fcsv, line_controller)
            mem['sfreport_datetime'] = time.ctime(os.path.getmtime(fcsv))
            os.remove(fcsv)
        else:
            print 'No sfreport csv found, fall back to', '\t',\
                  mem['sfreport_datetime'], '\n'


        # Carica i picURL nel db dal csv
        picurl = dbTable(
            name = "PicURL",
            cols_def = [('GaCode','TEXT PRIMARY KEY'),
                        ('PicUrl','TEXT'),])
        picurl.reset()

        def line_controller(row):
            fn, fext = os.path.splitext(u.de(row[0]))
            url_pre = u'http://arredo-luce.com/photos/'
            return (fn[-7:],                #GaCode
                    url_pre+fn[-7:]+fext,)  #picUrl
        
        fcsv = os.path.join('csv', 'input', 'picurl.csv')
        picurl.load(fcsv, line_controller, skip=False)

        # PicURL Revise
        purl_headers = ('*Action', 'ItemID', 'PicURL')
        purl_1row = {k:k for k in purl_headers}
        purl_1row['*Action'] = '*Action(SiteID=Italy|Country=IT|Currency=EUR|Version=403|CC=UTF-8)'

        revise_picurl = sf(purl_headers, purl_1row)

        purl_query = 'SELECT ItemID, PicURL FROM PicURL, Sfreport USING(GaCode)'

        def tras(row):
            return {'*Action':'Revise',
                    'ItemID':row['ItemID'],
                    'PicURL':row['PicUrl'].encode('iso-8859-1'),}
        
        revise_picurl.create('revise_picurl_'+nn+'.csv', purl_query, tras)
    else:
        print 'bye bye'

