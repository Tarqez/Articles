# -*- coding: utf-8 -*-
import sys, csv, os, xlrd, zipfile, jinja2

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import load_only
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, Unicode, Boolean, PickleType
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError


# -----------------------------
# DataStore def with Sqlalchemy
# -----------------------------

db_file = os.path.join('db', 'db.sqlite')
engine = create_engine('sqlite:///'+db_file, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Art(Base):
    __tablename__ = 'articles'

    id = Column(Integer, primary_key=True)
    ga_code = Column(Unicode, unique=True, index=True, nullable=False)
    brand = Column(Unicode, default=u'')    
    mnf_code = Column(Unicode, default=u'')
    description = Column(Unicode, default=u'')
    category = Column(Unicode, default=u'')
    units_of_sale = Column(Unicode, default=u'')
    min_of_sale = Column(Integer, default=1)
    
    # customers unit prices, VAT included
    price_b = Column(Float, default=0.0) 
    price_c = Column(Float, default=0.0)
    price_d = Column(Float, default=0.0)
    price_dr = Column(Float, default=0.0)    
    
    ebay_qty = Column(Integer, default=0)
    ebay_price = Column(Float, default=0.0)
    
    # val >= 0 - subtract to ebay_qty
    # val = -1 - set ebay_qty = 0
    # val < -1 - ebay listing to be close
    sale_control = Column(Integer, default=0)    
    ebay_itemid = Column(Unicode, default=u'')    
    notes = Column(Unicode, default=u'')

    changes = Column(Integer, default=0)
    
    attr_bit = {'ebay_itemid':2**3, 'ebay_qty':2**2, 'ebay_price':2**1, 'sale_control':2**0}    

    def set_change_for(self, *attributes):
        if self.changes is None: self.changes = 0
        for attr in attributes:
            if attr in self.attr_bit:
                self.changes |= self.attr_bit[attr]
            else: raise Exception('Error: attribute not exsist or not setable for changes')
            
    def reset_change_for(self, *attributes):
        if self.changes is None: self.changes = 0
        for attr in attributes:
            if attr in self.attr_bit:
                self.changes &= ~self.attr_bit[attr]
            else: raise Exception('Error: attribute not exsist or not resetable for changes')

   

class Sequence(Base):
    __tablename__ = 'sequences'

    id = Column(Integer, primary_key=True)
    number = Column(Integer, default=0)


class Categs(Base):
    __tablename__ = 'categs'

    id = Column(Integer, primary_key=True)
    cat_name = Column(Unicode, unique=True, index=True, nullable=False)
    store_cat_n = Column(Unicode, nullable=False)
    ebay_cat_n = Column(Unicode, default = u'')
    
                                  
Base.metadata.create_all(engine)


# ----------------------------------------
# A csv.DictWriter specialized with Fx csv
# ----------------------------------------

class EbayFx(csv.DictWriter):
    '''Subclass csv.DictWriter, define delimiter and quotechar and write headers'''
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





# Constants
# ---------

DATA_PATH = os.path.join('..', 'Google Drive', 'data')
ACTION = '*Action(SiteID=Italy|Country=IT|Currency=EUR|Version=745|CC=UTF-8)' # smartheaders CONST
FTPURL = 'garofoli.ftpimg.eu'
PICURL = 'PicURL=http://'+FTPURL+'/nopic.jpg' # smartheaders CONST
EMAIL = 'info.garofoli@gmail.com'
PHONE = '0835/385078'
INVOICE_FORM_URL = 'http://garofoli.ftpimg.eu/invoice_form'



# Fruitful functions
# ------------------

def price(p, m='1'):
    '''Return float price from string.
        Strip and convert italian format and consider the moltiplicatore m.
        Empty values default to 0 (for price) an 1 (for moltiplicatore).'''
    p = p.strip()
    p = 0.0 if p=='' else float(p.replace('.','').replace(',','.'))
    m = m.strip()
    m = 1.0 if m=='' else float(m)
    return p/m

def filenames_in(folder):
    "return a list of filenames inside folder"
    t = list()
    for d in os.listdir(folder):
        el = os.path.join(folder, d)
        if os.path.isfile(el): t.append(el)
    return t

def ebay_qty_calculation(dsource_row):
    'Reduce by 1 all stores except m9a'
    # TODO m9a is not being used anymore
    excluded_stores = {'m93':'closing store', # not used yet
                       'm94':'closing store',
                       'm95':'closing store',
                       'm97':'closing store',
                       'm9a':'small and highly unreliable'}
    tot = 0
    for k in dsource_row.keys():
        if k == 'ga_code':
            pass
        elif k == 'm9a':
            tot += dsource_row[k]
        else:
            tot += (dsource_row[k]-1 if dsource_row[k]>0 else 0)
    return tot


def ebay_price_calc(b, c, d, dr):
    'Return ebay price based on b, c, d, dr prices (must be float)'

##    # D line price
##    if d == 0: d = max(b,c,dr) # in case d=0 try to get anyway a price
##    if 0 < d < 1: d = 1
##    return d

    # B line price (all prices >= 0)
    if b == 0: b = max(c,d,dr)
    if b < 30: return b
    elif b < 50: return b + 2.44
    else: return b
    

def fx_fname(action_name, session):
    'Build & return Fx filename with a sequence number suffix'

    seq = session.query(Sequence).first()
    if seq: seq.number += 1
    else: seq = Sequence(number=0)      
    session.add(seq)
    session.commit()
    
    return action_name+'_'+str(seq.number).zfill(4)+'.csv'

def store_cat_n(cat_name, session):
    'Returns ebay StoreCategory num from DtSt'

    categ = session.query(Categs).filter(Categs.cat_name == cat_name.lower())\
               .first()
    if categ: return categ.store_cat_n
    else: return ''

def ebay_cat_n(cat_name, session):
    'Returns ebay Category num from DtSt'

    categ = session.query(Categs).filter(Categs.cat_name == cat_name.lower())\
               .first()
    if categ: return categ.ebay_cat_n
    else: return ''    


def ebay_title(brand, description, mnf_code):
    'Return cleaned *Title composed by func parameters'
    mnf_code = mnf_code+' -'
    max_desc_len = 80 - len(brand) - len(mnf_code) -2
    title = ' '.join((brand, mnf_code, description[:max_desc_len]))
    title = title.replace('<', '').replace('>', '').replace('&', '').replace('"', '')
    return title

def ebay_template(tpl_name, context):
    'Return one line html ebay templated for description field'
    try:
        template_loader = jinja2.FileSystemLoader(
            searchpath=os.path.join(os.getcwd(), 'templates', tpl_name))
        template_env = jinja2.Environment(loader=template_loader)

        template = template_env.get_template('index.htm')
        output = template.render(context)
        res = ' '.join(output.split()).encode('iso-8859-1')
        return res
    except:
        print sys.exc_info()[0]
        print sys.exc_info()[1]


def items_with_img():
    'Return gacodes list for items with an img on FTP server'

    from ftplib import FTP
    ftp = FTP(FTPURL)
    ftp.login('garofoli@ftpimg.eu', 'KGbA7ZmAq$&9')

    def is_correct_img_fname(fn):
        'True if img filename is <ddddddd>.jpg d is a digit'
        name, ext = os.path.splitext(fn)
        try:
            if len(name) != 7: raise
            int(name)
            return True
        except:
            return False

    # ga_codes list for all images on FTP server
    return [el[:-4] for el in ftp.nlst() if is_correct_img_fname(el)] 



# Void functions
# --------------

def dtst_ebay_price_qty_alignment(session):
    '''Read Fx report "attivo" and on DataStore
    - overwrite ebay_itemid with a value or blank 
    - sets or resets changes for ebay_qty and ebay_price
    finally 
    - check if there are listings out of DataStore
    - check if there are listings with OutOfStockControl=false'''

    folder = os.path.join(DATA_PATH, 'ebay_report_attivo')
    for fname in filenames_in(folder):
        with open(fname, 'rb') as f:
            all_rows = csv.reader(f, delimiter=';', quotechar='"')
            all_rows.next()
            for row in all_rows:
                ga_code_from_ebay = row[1][:-3] if len(row[1])>7 else row[1]
                price_from_ebay = float(price(row[8].replace('EUR', '')))
                qty_from_ebay = int(row[5])
                try:
                    art = session.query(Art).filter(Art.ga_code == ga_code_from_ebay).first()
                    if art:
                        # set/reset changes
                        ebay_qty = art.ebay_qty - art.sale_control if art.sale_control>= 0 and art.ebay_qty - art.sale_control>= 0 else 0
                        if ebay_qty - qty_from_ebay != 0:
                            art.set_change_for('ebay_qty') # set if different
                        else:
                            art.reset_change_for('ebay_qty') # reset if equal

                        if abs(art.ebay_price - price_from_ebay)>0.01:
                            art.set_change_for('ebay_price') # set if different
                        else:
                            art.reset_change_for('ebay_price') # reset if equal

                        # itemid overwrite
                        art.ebay_itemid = row[0].strip()
                        art.set_change_for('ebay_itemid') # remeber overwritten itemid

                        session.add(art)
                    else: # alert for items out of DataStore
                        print 'You have itemID', row[0], 'listing out of data store'
                            
                    if row[22].lower() == 'false':# check if OutOfStockControl=false
                        print 'ItemID', row[0], 'listing has OutOfStockControl=false'
                    
                except ValueError:
                    print 'rejected line:'
                    print dsource_row
                    print sys.exc_info()[0]
                    print sys.exc_info()[1]
                    print sys.exc_info()[2]
        os.remove(fname)
        session.commit()

        # all just not overwritten itemids
        arts_offline = session.query(Art).filter(Art.changes/Art.attr_bit['ebay_itemid']%2 == 0)
        for art in arts_offline:
            art.ebay_itemid = u''
            art.changes = 0 # keep DSt ordered, reset all changes
            session.add(art)

        # all just overwritten itemids
        arts_online = session.query(Art).filter(Art.changes/Art.attr_bit['ebay_itemid']%2 == 1)
        for art in arts_online:
            art.reset_change_for('ebay_itemid')
            session.add(art)    

    session.commit()


#######################################################



def _qty_dsource_rows(fxls):
    'Yield a dict of store-qty'

##    # test support part
##    folder = os.path.join(DATA_PATH, 'test')
##    fnames = filenames_in(folder) # a list
##    fxls = fnames[0] # we have the xls file obj
    
    qty = dict() # dict of dicts

    # Load xls file in a dict of dicts
    with xlrd.open_workbook(fxls) as wbk:
        sh = wbk.sheet_by_index(0)
        for r in range(sh.nrows):
            try:
                c = str(int(sh.row_values(r)[4])).zfill(7) # ga_code
                m = 'm'+sh.row_values(r)[11].lower() # i.e. m9a                  
                q = int(sh.row_values(r)[9]) # never find q=0            

                # initialization
                if c not in qty:
                    qty[c] = dict()
                    qty[c]['ga_code'] = c # usefull when yeld a row
   
                # increment
                qty[c][m] = qty[c].get(m,0) + q
               
            except ValueError:
                pass # discard line with no quantity
            except:
                print sys.exc_info()[0]
                print sys.exc_info()[1]

    # stats
    print 'C/V stores stats'
    print '----------------', '\n'

    print 'Num of items in all stores:', len(qty), '\n'

    store_stats = dict()

    for gacode in qty:
        for m in qty[gacode]:
            # init
            if m not in store_stats:
                store_stats[m] = {'itms':0, 'pcs':0}
            # inc    
            store_stats[m]['itms'] = store_stats[m]['itms'] + 1
            store_stats[m]['pcs'] = store_stats[m]['pcs'] + qty[gacode][m]
        yield qty[gacode]

    print 'Pcs in all stores:', 


                
#######################################################
                    
def qty_dsource_rows(fxls):
    'Yield a row of stores quantities'
    #Able to read DisponibilitaContovendita.xls
    import sqlite3, xlrd, sys

    #conn = sqlite3.connect('db.sqlite')
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    all_stores = ('m9b', 'm9c', 'm90', 'm91', 'm92', 'm96', 'm98')
    # 6 June, removed Andria stores 9C, 94, 95. They are moving, possible missing items
    # 23 Sep, added 9C
    # 11 August, removed m9a Matera. Highly unreliable with few items
    # 13 September removed 93 (closing) and 97 (problems). 
    m9_table_declaration_part = ' INTEGER DEFAULT 0, '.join(all_stores)

    # create temp table
    cur.execute('CREATE TEMP TABLE m9 (ga_code TEXT PRIMARY KEY NOT NULL, '+
                m9_table_declaration_part+
                ' INTEGER DEFAULT 0)')

    excluded_stores_items_counter = dict()

    # Load xls files in a temporary sqlite table
    with xlrd.open_workbook(fxls) as wbk:
        sh = wbk.sheet_by_index(0)
        for r in range(sh.nrows):
            try:
                c = str(int(sh.row_values(r)[4])).zfill(7)
                q = int(sh.row_values(r)[9])
                m = 'm'+sh.row_values(r)[11].lower() # i.e. m9a 
                if m in all_stores:
                    # sta già nel db?
                    rn = cur.execute('SELECT COUNT(*) FROM m9 WHERE ga_code = ?', (c,)).fetchone()[0]
                    if rn == 1: # ci sta e UPDATE
                        cur.execute('UPDATE OR FAIL m9 SET '+m+'='+m+'+? WHERE ga_code=?', (q, c))
                    else: # non ci sta e allora INSERT
                        cur.execute('INSERT OR FAIL INTO m9 (ga_code, '+m+') VALUES (?, ?)', (c, q))
                else:
                    excluded_stores_items_counter[m] = excluded_stores_items_counter.get(m, 0) + q
                    
            except ValueError:
                pass # discard line with no quantity               
            except:
                print sys.exc_info()[0]
                print sys.exc_info()[1]              

    conn.commit()
    
    # reporting part            
    print 'Report from C/V stores'
    print '----------------------', '\n'

    print cur.execute('SELECT COUNT(*) FROM m9').fetchone()[0], 'different codes in all stores', '\n'

    sql_select_tot_qty = 'SELECT SUM('+'), SUM('.join(all_stores)+') FROM m9'
    rset = cur.execute(sql_select_tot_qty).fetchone()

    print 'Total number of items store by store:'
    print len(all_stores)*'%-8s ' % all_stores
    print len(all_stores)*'%-8s ' % tuple(rset), '\n'

    print 'Excluded stores (to add if interesting)'
    for k in excluded_stores_items_counter:
        print excluded_stores_items_counter[k], 'items inside', k

    # generate quantity row
    rset = cur.execute('SELECT * FROM m9')
    for row in rset: yield row

    conn.close()
        
def qty_loader(session):
    "Load ('ga_code', 'ebay_qty') into DtSt"
    folder = os.path.join(DATA_PATH, 'quantities')
    for fname in filenames_in(folder):
        with zipfile.ZipFile(fname, 'r') as zipf:
            zipf.extractall(folder)
        os.remove(fname)
    fnames = filenames_in(folder)
    if fnames: # if no files exit, do not set all qty=0
        # missing zero qty rows hack
        all_ds_ids = [id_tuple[0] for id_tuple in session.query(Art.id).all()] # all DtSt ids in a list
            
        for fname in filenames_in(folder):        
            dsource_rows = qty_dsource_rows(fname)
            dstore_row = dict()
            for dsource_row in dsource_rows:
                try:
                    dstore_row['ga_code']=dsource_row['ga_code']
                    dstore_row['ebay_qty']=ebay_qty_calculation(dsource_row)
                    
                    art = session.query(Art).filter(Art.ga_code == dstore_row['ga_code']).first()                    
                    if art: # exsist                    
                        all_ds_ids.remove(art.id) # because exsists, remove from ids list

                        if art.ebay_qty != dstore_row['ebay_qty']: # if qty is to update
                            if dstore_row['ga_code'] == '0006968': print 'quantità diverse'
                            for attr, value in dstore_row.items(): # update
                                setattr(art, attr, value)
                            if (art.ebay_itemid != u''): # if it is online
                                art.set_change_for('ebay_qty') # set qty change
                            session.add(art)
                    else:
                        art = Art()
                        for attr, value in dstore_row.items():
                            setattr(art, attr, value)
                        session.add(art)
                except ValueError:
                    print 'rejected line:'
                    print dsource_row
                    print sys.exc_info()[0]
                    print sys.exc_info()[1]
                    print sys.exc_info()[2]
            os.remove(fname)        
            session.commit()

        # for remaining ids set qty=0
        for art_id in all_ds_ids:
            art_zero_qty = session.query(Art).filter(Art.id == art_id).first() # surely exsist in DtSt
            if (art_zero_qty.ebay_itemid != u'') and (art_zero_qty.ebay_qty > 0): # if is also online and with qty>0               
                art_zero_qty.set_change_for('ebay_qty') # set qty change                                
            art_zero_qty.ebay_qty = 0 # set qty=0
            session.add(art)
        session.commit()
            


def price_loader(session):
    "Load ('ga_code', 'price_b', 'price_c', 'price_d', 'price_dr') into DtSt"
    folder = os.path.join(DATA_PATH, 'prices')

    for fname in filenames_in(folder):
        with open(fname, 'rb') as f:
            dsource_rows = csv.reader(f, delimiter=';', quotechar='"')
            dsource_rows.next()
            dstore_row = dict()
            for dsource_row in dsource_rows:
                try:
                    dstore_row['ga_code']=dsource_row[0]
                    dstore_row['price_b']=price(dsource_row[1][14:], dsource_row[1][:6])
                    dstore_row['price_c']=price(dsource_row[2][14:], dsource_row[2][:6])
                    dstore_row['price_d']=price(dsource_row[3][14:], dsource_row[3][:6])
                    dstore_row['price_dr']=price(dsource_row[4][14:], dsource_row[4][:6])
                                        
                    dstore_row['ebay_price']=ebay_price_calc(dstore_row['price_b'],
                                                             dstore_row['price_c'],
                                                             dstore_row['price_d'],
                                                             dstore_row['price_dr'],)
                    
                    art = session.query(Art).options(load_only('id', 'price_b', 'price_c', 'price_d', 'price_dr', 'changes'))\
                          .filter(Art.ga_code == dstore_row['ga_code']).first()
                    if art: # set price change if online and different
                        if (art.ebay_itemid != u'') and abs(art.ebay_price - dstore_row['ebay_price']) > 0.01:
                            art.set_change_for('ebay_price')
                    else: art = Art()
                    for attr, value in dstore_row.items():
                        setattr(art, attr, value)
                    session.add(art)
                except ValueError:
                    print 'rejected line:'
                    print dsource_row
                    print sys.exc_info()[0]
                    print sys.exc_info()[1]
                    print sys.exc_info()[2]
        os.remove(fname)
        session.commit()


def anagrafica_loader(session):
    "Load ('ga_code', 'brand', 'mnf_code', 'description', 'category', 'units_of_sale', 'min_of_sale') into DtSt"
    folder = os.path.join(DATA_PATH, 'anagrafica')

    for fname in filenames_in(folder):
        with open(fname, 'rb') as f:
            dsource_rows = csv.reader(f, delimiter=';', quotechar='"')
            dsource_rows.next()
            dstore_row = dict()
            for dsource_row in dsource_rows:
                try:
                    dstore_row['ga_code']=dsource_row[0]
                    dstore_row['brand']=' '.join(dsource_row[4].split()[1:]).title()
                    dstore_row['mnf_code']=dsource_row[6]
                    dstore_row['description']=dsource_row[2].decode('iso.8859-1')
                    dstore_row['category']=' '.join(dsource_row[5].split()[1:])
                    dstore_row['units_of_sale']=dsource_row[3].split().pop(0)
                    dstore_row['min_of_sale']=int(float((dsource_row[7].strip() or '0,0').replace('.', '').replace(',', '.')))
                    
                    art = session.query(Art).options(load_only('id', 'ga_code', 'brand', 'mnf_code', 'description', 'category', 'units_of_sale', 'min_of_sale'))\
                          .filter(Art.ga_code == dstore_row['ga_code']).first()

                    if not art: art = Art()
                    for attr, value in dstore_row.items():
                        setattr(art, attr, value)
                    session.add(art)
                except ValueError:
                    print 'rejected line:'
                    print dsource_row
                    print sys.exc_info()[0]
                    print sys.exc_info()[1]
                    print sys.exc_info()[2]
        os.remove(fname)
        session.commit()
                

def revise_qty(session):
    'Fx revise quantity action'
    smartheaders = (ACTION, 'ItemID', '*Quantity')
    arts = session.query(Art)\
           .options(load_only('id', 'ebay_qty', 'ebay_itemid', 'changes'))\
           .filter(Art.ebay_itemid != u'',
                   Art.changes/Art.attr_bit['ebay_qty']%2 == 1)
    fout_name = os.path.join(DATA_PATH, 'fx_output', fx_fname('revise_qty', session))
    with EbayFx(fout_name, smartheaders) as wrt:
        for article in arts:
            ebay_qty = article.ebay_qty - article.sale_control if (article.sale_control >= 0 and article.ebay_qty >= article.sale_control) else 0
            fx_revise_row = {ACTION:'Revise',
                             'ItemID':article.ebay_itemid,
                             '*Quantity':ebay_qty,}
            wrt.writerow(fx_revise_row)
            article.reset_change_for('ebay_qty')
            session.add(article)
        session.commit()

           

def revise_price(session):
    'Fx revise price'
    smartheaders=(ACTION, 'ItemID', '*StartPrice', 'ShippingProfileName=GLS_paid')
    arts = session.query(Art)\
           .options(load_only('id', 'ebay_price', 'ebay_itemid', 'changes'))\
           .filter(Art.ebay_itemid != u'', # must be already online
                   Art.ebay_price >= 1,
                   Art.changes/Art.attr_bit['ebay_price']%2 == 1)
    fout_name = os.path.join(DATA_PATH, 'fx_output', fx_fname('revise_price', session))
    with EbayFx(fout_name, smartheaders) as wrt:
        for art in arts:
            if art.ebay_price >=1: # can't change price in less than 1 on ebay
                fx_revise_row = {ACTION:'Revise',
                                 'ItemID':art.ebay_itemid,
                                 '*StartPrice':art.ebay_price,
                                 'ShippingProfileName=GLS_paid':''}
                wrt.writerow(fx_revise_row)
                art.reset_change_for('ebay_price')
                session.add(art)
        session.commit()

def ebay_price_db_update(session):
    'In case you change ebay_price_calc() and want to update DB for a next price_revise'
    arts = session.query(Art)
    for art in arts:
        ebay_price = ebay_price_calc(art.price_b, art.price_c, art.price_d, art.price_dr,)
        if abs(art.ebay_price - ebay_price) > 0.01:
            art.ebay_price = ebay_price            
            if (art.ebay_itemid != u''): # set price change if online
                art.set_change_for('ebay_price')
            session.add(art)
    session.commit()        

def revise_shipping_profile(session):
    'Fx revise shipping profile'
    smartheaders=(ACTION, 'ItemID', 'ShippingProfileName=GLS_paid')
    arts = session.query(Art).filter(Art.ebay_itemid != u'',
                                    Art.price_b >= 30)
    fout_name = os.path.join(DATA_PATH, 'fx_output', fx_fname('revise_shipping_profile', session))
    with EbayFx(fout_name, smartheaders) as wrt:
        for art in arts:
            fx_revise_row = {ACTION:'Revise',
                             'ItemID':art.ebay_itemid,
                             'ShippingProfileName=GLS_paid':'GLS_free'}
            wrt.writerow(fx_revise_row)   
    

def revise_picurl(session, revise_also_articles_with_defaut_image=True):
    '''Fx revise PicURL with FTP server hosted images'''

    # all ebay listed items
    ebay_listed_arts = session.query(Art).filter(Art.ebay_itemid != u'')
    
    smartheaders=(ACTION, 'ItemID', PICURL)
    fout_name = os.path.join(DATA_PATH, 'fx_output', fx_fname('revise_picurl', session))
    gacodes_of_images = items_with_img()

    with EbayFx(fout_name, smartheaders) as wrt:
        for art in ebay_listed_arts:
            fx_revise_row = {ACTION:'Revise',
                             'ItemID':art.ebay_itemid,
                             PICURL:''}
            
            if art.ga_code in gacodes_of_images:
                fx_revise_row[PICURL] = 'http://'+FTPURL+'/'+art.ga_code+'.jpg'
                wrt.writerow(fx_revise_row)
            elif revise_also_articles_with_defaut_image:
                wrt.writerow(fx_revise_row)          
            
    ftp.close()



def revise_storecategory(session):
    'Fx revise StoreCategory'
    smartheaders=(ACTION, 'ItemID', 'StoreCategory')
    arts = session.query(Art).filter(Art.ebay_itemid != u'') # all listed items
    fout_name = os.path.join(DATA_PATH, 'fx_output', fx_fname('revise_StoreCategory', session))
    with EbayFx(fout_name, smartheaders) as wrt:
        for article in arts[8700:]:
            fx_revise_row = {ACTION:'Revise',
                             'ItemID':article.ebay_itemid,
                             'StoreCategory':store_cat_n(article.category, session)}
            wrt.writerow(fx_revise_row)

def revise_template(session):
    'Fx revise Description'
    smartheaders = (ACTION, 'ItemID', 'Description')
    arts = session.query(Art).filter(Art.ebay_itemid != u'') # all listed items
    fout_name = os.path.join(DATA_PATH, 'fx_output', fx_fname('revise_template', session))
    with EbayFx(fout_name, smartheaders) as wrt:
        #for art in arts[:3730]:
        #for art in arts[3730:7460]:
        #for art in arts[7460:11190]:
        #for art in arts[11190:14920]:
        for art in arts:
            title = ebay_title(art.brand, art.description, art.mnf_code)
            context = {'ga_code':art.ga_code,
                       'title':title,
                       'description':'',
                       'email':EMAIL,
                       'phone':PHONE,
                       'invoice_form_url':INVOICE_FORM_URL,}
            ebay_description = ebay_template('garofoli', context)            
            fx_revise_row = {ACTION:'Revise',
                             'ItemID':art.ebay_itemid,
                             'Description':ebay_description,}
            wrt.writerow(fx_revise_row)

def add(session):
    'Fx add action'
    smartheaders = (ACTION,
                    '*Category=50584',
                    '*Title',
                    'Description',
                    PICURL,
                    '*Quantity',
                    '*StartPrice',
                    'StoreCategory=1',
                    'CustomLabel',
                    '*ConditionID=1000',
                    '*Format=StoresFixedPrice',
                    '*Duration=GTC',
                    'OutOfStockControl=true',
                    '*Location=Matera',
                    'VATPercent=22',
                    '*ReturnsAcceptedOption=ReturnsAccepted',
                    'ReturnsWithinOption=Days_30',
                    'ShippingCostPaidByOption=Buyer',                    
                    # Regole di vendita
                    'PaymentProfileName=PayPal-Bonifico',
                    'ReturnProfileName=Reso1',
                    'ShippingProfileName=GLS_paid',
                    # specifiche oggetto
                    'C:Marca',
                    'C:Modello',
                    'C:Genere',
                    'Counter=BasicStyle',)
    
    arts = session.query(Art).filter(Art.ebay_itemid == u'',
                                     Art.units_of_sale == 'PZ',
                                     Art.min_of_sale <= 1, #0 ia as 1
                                     Art.ebay_price > 1,
                                     Art.sale_control >= 0,
                                     Art.ebay_qty > 0)

    fout_name = os.path.join(DATA_PATH, 'fx_output', fx_fname('add', session))
    gacodes_of_images = items_with_img()
    with EbayFx(fout_name, smartheaders) as wrt:
        for art in arts:
            title = ebay_title(art.brand, art.description, art.mnf_code)
            context = {'ga_code':art.ga_code,
                       'title':title,
                       'description':'',
                       'email':EMAIL,
                       'phone':PHONE,
                       'invoice_form_url':INVOICE_FORM_URL,}
            ebay_description = ebay_template('garofoli', context)
            fx_add_row = {ACTION:'Add',
                          '*Title':title.encode('iso-8859-1'),
                          'Description':ebay_description,
                          '*Quantity':art.ebay_qty,
                          '*StartPrice':art.ebay_price,
                          'ShippingProfileName=GLS_paid':'',
                          'CustomLabel':art.ga_code,
                          PICURL:'http://'+FTPURL+'/'+art.ga_code+'.jpg' if art.ga_code in gacodes_of_images else '',
                          'StoreCategory=1':store_cat_n(art.category, session),
                          '*Category=50584':ebay_cat_n(art.category, session),
                          'C:Marca':art.brand,
                          'C:Modello':art.mnf_code,
                          'C:Genere':art.category}
            wrt.writerow(fx_add_row)
                        


def end(session):
    'Fx end action'
    smartheaders=(ACTION, 'ItemID', 'EndCode=NotAvailable')
##    arts = session.query(Art).filter(Art.ebay_itemid != u'',
##                   Art.sale_control >= 0,
##                   Art.ebay_price <= 10,
##                   Art.ebay_qty == 0,)

    arts = session.query(Art).filter(Art.ebay_itemid != u'', Art.price_b < 50)
    fout_name = os.path.join(DATA_PATH, 'fx_output', fx_fname('end', session))
    with EbayFx(fout_name, smartheaders) as wrt:
        for art in arts:
            fx_end_row = {ACTION:'End',
                          'ItemID':art.ebay_itemid,}
            wrt.writerow(fx_end_row)
            art.ebay_itemid = u'' # SET ebay_itemid = u'' 
            session.add(art)
        session.commit()                

    
        
def update_qty():
    ses = Session()
    qty_loader(ses)
    revise_qty(ses)
    ses.close()

def update_price():
    ses = Session()
    price_loader(ses)
    revise_price(ses)
    ses.close()
    

def allinea():
    ses = Session()
    dtst_ebay_price_qty_alignment(ses)
    ses.close()

def notsell(ga_code, notes=u''):
    'Set sale_control to -2'
    ses = Session()
    item = ses.query(Art).filter(Art.ga_code == ga_code).first()
    item.sale_control = -2
    item.notes = notes
    ses.add(item)
    ses.commit()
    ses.close()
