# -*- coding: cp1252 -*-
import os, xlrd
DATA_PATH = os.path.join('data_for_test')
def filenames_in(folder):
    "return a list of filenames inside folder"
    t = list()
    for d in os.listdir(folder):
        el = os.path.join(folder, d)
        if os.path.isfile(el): t.append(el)
    return t


def qty_dsource_rows(fxls=None):
    'Yield a row of stores quantities'
    #Able to read DisponibilitaContovendita.xls
    import sqlite3, xlrd, sys

    # test support part
    folder = os.path.join(DATA_PATH)
    fnames = filenames_in(folder) # a list
    fxls = fnames[0] # we have the xls file obj    

    #conn = sqlite3.connect('db.sqlite')
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    all_stores = ('m9a', 'm9b', 'm9c', 'm90', 'm91', 'm92',
                  'm93', 'm94', 'm95', 'm96', 'm97',  'm98', 'mgt')

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
    #for row in rset: yield row
    return rset

    conn.close()


def _qty_dsource_rows(fxls=None):
    'Yield a dict of store-qty'

    # test support part
    folder = os.path.join(DATA_PATH)
    fnames = filenames_in(folder) # a list
    fxls = fnames[0] # we have the xls file obj
    
    qty = dict() # dict of dicts

    # Load xls file in a dict of dicts
    with xlrd.open_workbook(fxls) as wbk:
        sh = wbk.sheet_by_index(0)
        for r in range(sh.nrows):
            try:
                c = str(int(sh.row_values(r)[4])).zfill(7)
                m = 'm'+sh.row_values(r)[11].lower() # i.e. m9a                  
                q = int(sh.row_values(r)[9]) # never find q=0            

                # initialization
                if c not in qty:
                    qty[c] = dict()
   
                # incrementation
                qty[c][m] = qty[c].get(m,0) + q
               
            except ValueError:
                pass # discard line with no quantity
            except:
                print sys.exc_info()[0]
                print sys.exc_info()[1]





    # stats
    print 'C/V stores stats'
    print '----------------', '\n'

    print 'All stores articles:', len(qty)

    store_stats = dict()

    for gacode in qty:
        for m in qty[gacode]:
            # init
            if m not in store_stats:
                store_stats[m] = {'itms':0, 'pcs':0}
            # inc    
            store_stats[m]['itms'] += 1
            store_stats[m]['pcs'] += qty[gacode][m]
        #yield gacode, qty[gacode]

    all_pcs = 0
    for m in store_stats:
        all_pcs += store_stats[m]['pcs']
    
    print 'All stores pieces:', all_pcs, '\n'
    print '   ', 2*'%-8s' % ('items','pcs')
    print '   ', 2*'%-8s' % ('-----','---')

    for m in store_stats:
        print m, 2*'%-8s' % (store_stats[m]['itms'], store_stats[m]['pcs'])

    return qty




rset = qty_dsource_rows()
diz = _qty_dsource_rows()


for row in rset:
    ga_code = row['ga_code']
    if ga_code not in diz: raise Exception("No ga_code found in diz")
    for k in row.keys():
        if k == 'ga_code': pass
        elif row[k] > 0:
            if row[k] - diz[ga_code][k] != 0:
                raise Exception('Store qty mismatch for '+ga_code+' in '+k)

    for k in diz[ga_code]:
        if diz[ga_code][k] - row[str(k)] != 0:
            raise Exception('Store qty mismatch for '+ga_code+' in '+k)

print 'Test success!'

    
    




