import re
import time
import MySQLdb
from decimal import Decimal
from queue import Queue
from threading import Thread
from db.table_layouts import *
from mysql.sql import *
from util import *
from record.record import *

class Driver:
    def __init__(self, scale, host, port, user, passwd):
        self._host = host
        self._user = user
        self._passwd = passwd
        self._port = port
        self._scale = scale
        self._conn = MySQLdb.connect(host=self._host, port=self._port, user=self._user, passwd=self._passwd) #
        self._cursor = self._conn.cursor()
        self._flag = True
        self._delivery_q = Queue()
        self._delivery_t = Thread(target=self.process_delivery, args=(self._delivery_q,))
        self._delivery_t.start()
        self._delivery_stop = False
        self._cursor.execute('use tpcc;')



    def __del__(self):
        self._flag = False
        while not self._delivery_stop:
            continue

    def build(self):
        print("Build database schema...")
        #self._cursor.execute('create database tpcc;')
        self._cursor.execute('use tpcc;')
        if self._host == 'localhost':
            self._cursor.execute('set global max_allowed_packet=1024*1024*1024;')
            self._conn.commit()
        sql = open("db/schema.sql", "r").read().split(';')
        for line in sql:
            if (line):
                self._cursor.execute(line + ';')
        self._conn.commit()

    def populate(self):
        self._gen_item()
        self._gen_warehouse()
        self._gen_district()
        self._gen_stock()
        self._gen_customer()
        self._gen_order_order_line()
        self._gen_history()
        self._gen_new_order()
        print('Database has initialized.')

    def _gen_item(self):
        print("populating ITEM table...")
        g = lambda i: [i, random.randrange(population[ITEM]), rand_str(14, 25),
                       round(random.random() * (100 - 1) + 1, 2), rand_dat(26, 51)]
        rows = [g(i) for i in range(population[ITEM])]
        insert(self._cursor,ITEM, rows)
        self._conn.commit()

    def _gen_warehouse(self):
        print("populating WAREHOUSE table...")
        g = lambda i:[i, rand_str(6,11), rand_str(10,21), rand_str(10,21), rand_str(10,21), rand_str(2),
                      zip_code(),round(random.random() * 0.2, 4),300000.00]
        rows = [g(i) for i in range(self._scale)]
        insert(self._cursor, WAREHOUSE, rows)
        self._conn.commit()

    def _gen_stock(self):
        print("populating STOCK table...")
        g = lambda i,j:[j, i, random.randrange(10,101), rand_str(24), rand_str(24), rand_str(24),
                        rand_str(24), rand_str(24), rand_str(24), rand_str(24), rand_str(24),
                        rand_str(24), rand_str(24), 0, 0, 0, rand_dat(26,51)]
        rows = [g(i, j) for i in range(self._scale) for j in range(population[ITEM])]
        print('data prepared...')
        for x in range(10):
            insert(self._cursor,STOCK,rows[x*10000:x*10000+10000])
            self._conn.commit()
            print('commit')

    def _gen_district(self):
        print("populating DISTRICT table...")
        g = lambda i,j:[j,i,rand_str(6,11),rand_str(10,21),rand_str(10,21),rand_str(10,21),
                        rand_str(2),zip_code(),round(random.random()*0.2,4),30000.00,3000]# start form 0, so not 3001
        rows = [g(i, j) for i in range(self._scale) for j in range(population[DISTRICT])]
        insert(self._cursor,DISTRICT,rows)
        self._conn.commit()


    def _gen_customer(self):
        print("populating CUSTOMER table...")
        credit = lambda :'GC' if random.randrange(100) < 90 else 'BC'
        g = lambda i,j,k:[k, j, i, get_c_last(k), "OE", rand_str(8, 17), rand_str(10, 21),
                          rand_str(10, 21), rand_str(10, 21),rand_str(2), zip_code(),
                          rand_digit(16), current_time(), credit(),
                          50000.00, round(random.random() * 0.5, 4), -10, 10.00, 1, 0, rand_str(300,501)]
        rows = [g(i, j, k) for i in range(self._scale)
                           for j in range(population[DISTRICT])
                          for k in range(population[CUSTOMER])]
        insert(self._cursor,CUSTOMER,rows)
        self._conn.commit()

    def _gen_history(self):
        print("populating HISTORY table...")
        g = lambda i,j,k:[k, j, i, j, i, current_time(), 10.00, rand_str(12, 25)]
        rows = [g(i, j, k) for i in range(self._scale)
                           for j in range(population[DISTRICT])
                           for k in range(population[CUSTOMER])]
        insert(self._cursor,HISTORY,rows)
        self._conn.commit()

    def _gen_order_order_line(self):
        print("populating ORDERS table...")
        O_C_ID = rand_perm(population[CUSTOMER])
        carrier = lambda k:random.randrange(10) if k < 2100 else None # 1-10 to 0-9
        g = lambda i,j,k:[k, O_C_ID[k], j, i, current_time(), carrier(k), random.randrange(5, 16), 1]
        orders = [g(i,j,k) for i in range(self._scale) for j in range(10) for k in range(3000)]
        delivery = lambda i,j,k:orders[i * population[DISTRICT] * population[CUSTOMER] + j * population[CUSTOMER] + k][4] \
                                if k < 2100 else None
        amount = lambda k:0.00 if k < 2100 else round(random.random()*(9999.99-0.01)+0.01,2)
        g = lambda i,j,k,n:[k,j,i,n,random.randrange(population[ITEM]),i,delivery(i,j,k), 5, amount(k),rand_str(24)]
        order_lines = [g(i,j,k,n) for i in range(self._scale)
                                  for j in range(population[DISTRICT])
                                  for k in range(population[CUSTOMER])
                                  for n in range(orders[i * population[DISTRICT] * population[CUSTOMER] + j * population[CUSTOMER] + k][-2])]
        insert(self._cursor, ORDERS, orders)
        self._conn.commit() #############################################################+++++++#############
        print('populating ORDER_LINE table...')
        num_of_lines = len(order_lines)
        for x in range(10):
            insert(self._cursor, ORDER_LINE, order_lines[x*10000:min(x*10000+10000, num_of_lines)])
            self._conn.commit()

    def _gen_new_order(self):
        print("populating NEW ORDER table...")
        g = lambda i,j,k:[k,j,i]
        rows = [g(i, j, k) for i in range(self._scale) for j in range(10) for k in range(2100, 3000)]
        insert(self._cursor,NEW_ORDER,rows)
        self._conn.commit()

    #
    def do_new_order(self, w_id, d_id, c_id, ol_i_id, ol_supply_w_id, ol_quantity):
        ol_cnt = len(ol_i_id)
        ol_amount = 0
        total_amount = 0
        brand_generic = ''

        #transcation
        self._cursor.execute('START TRANSACTION;')
        print('+ New Order')
        #phase 1
        w_tax = select(cursor_= self._cursor,
                       table=WAREHOUSE,
                       where=(W_ID,eq,w_id),
                       col=W_TAX)[0][0]
        d_tax, d_next_o_id = select(cursor_=self._cursor,
                                    table=DISTRICT,
                                    col = (D_TAX,D_NEXT_O_ID),
                                    where=[(D_ID,eq,d_id),
                                           (D_W_ID,eq,w_id)])[0]
        update(cursor_=self._cursor,
               table=DISTRICT,
               row=(D_NEXT_O_ID,d_next_o_id+1),
               where=[(D_ID,eq,d_id),(D_W_ID,eq,w_id)])
        c_discount, c_last_, c_credit = select(cursor_=self._cursor,
                                               table=CUSTOMER,
                                               col = (C_DISCOUNT,C_LAST,C_CREDIT),
                                               where=[(C_ID,eq,c_id),
                                                      (C_D_ID,eq,d_id),
                                                      (C_W_ID,eq,w_id)])[0]

        #phase 2
        insert(cursor_=self._cursor,
               table=ORDERS,
               rows=(d_next_o_id,c_id,d_id,w_id,current_time(),None,ol_cnt,int(len(set(ol_supply_w_id))==1)))
        insert(cursor_=self._cursor,
               table=NEW_ORDER,
               rows=(d_next_o_id,d_id,w_id))
        #phase 3
        for i in range(ol_cnt):
            try:
                i_price, i_name, i_data = select(cursor_=self._cursor,
                                                 table=ITEM,
                                                 col=(I_PRICE,I_NAME,I_DATA),
                                                 where=(I_ID,eq,ol_i_id[i]))[0]
            except IndexError:
                self._conn.rollback()
                return False
            s_quantity, *s_dist, s_data, s_ytd, s_order_cnt, s_remote_cnt = \
            select(cursor_=self._cursor,
                   table=STOCK,
                   col=(S_QUANTITY,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,
                        S_DIST_08,S_DIST_09,S_DIST_10,S_DATA,S_YTD,S_ORDER_CNT,S_REMOTE_CNT),
                   where=[(S_I_ID,eq,ol_i_id[i]),
                          (S_W_ID,eq,ol_supply_w_id[i])])[0]

            if s_quantity - ol_quantity[i] >= 10:
                s_quantity -= ol_quantity[i]
            else:
                s_quantity = s_quantity - ol_quantity[i] + 91
            s_ytd += ol_quantity[i]
            s_order_cnt += 1
            if ol_supply_w_id[i] != w_id:
                s_remote_cnt += 1
            update(cursor_=self._cursor,
                   table=STOCK,
                   row=[(S_QUANTITY,s_quantity),
                        (S_YTD,s_ytd),
                        (S_ORDER_CNT,s_order_cnt),
                        (S_REMOTE_CNT,s_remote_cnt)],
                   where=[(S_I_ID, eq, ol_i_id[i]),
                          (S_W_ID, eq, ol_supply_w_id[i])])
            ol_amount = ol_quantity[i] * i_price
            brand_generic = 'B' if re.search('ORIGINAL',i_data) and re.search('ORIGINAL',s_data) else 'G'
            insert(cursor_=self._cursor,
                   table=ORDER_LINE,
                   rows=(d_next_o_id,d_id,w_id,i,ol_i_id[i],ol_supply_w_id[i],None,ol_quantity[i],ol_amount,s_dist[d_id]))
            total_amount += ol_amount
        total_amount *=(1-c_discount)*(1+w_tax+d_tax)
        self._conn.commit()
        print('- New Order')
        return True


    def do_payment(self, w_id, d_id, c_w_id, c_d_id, c_query, h_amount):
        self._cursor.execute('START TRANSACTION;')
        print('+ Payment')
        w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd = \
        select(cursor_=self._cursor,
               table=WAREHOUSE,
               col=(W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_YTD),
               where=(W_ID,eq,w_id))[0]
        update(cursor_=self._cursor,
               table=WAREHOUSE,
               row=(W_YTD,w_ytd+Decimal(h_amount)),
               where=(W_ID,eq,w_id))
        d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd = \
        select(cursor_=self._cursor,
               table=DISTRICT,
               col=(D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE, D_ZIP,D_YTD),
               where=(D_ID,eq,d_id))[0]
        update(cursor_=self._cursor,
               table=DISTRICT,
               row=(D_YTD,d_ytd+Decimal(h_amount)),
               where=[(D_W_ID,eq,w_id),(D_ID,eq,d_id)])
        if(type(c_query) == str):
            result = select(cursor_=self._cursor,
                            table=CUSTOMER,
                            col=(C_ID,C_FIRST,C_MIDDLE,C_LAST,C_STREET_1,C_STREET_2,C_CITY,C_STATE,
                                           C_ZIP,C_PHONE,C_SINCE,C_CREDIT,C_CREDIT_LIM,C_DISCOUNT,
                                           C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT),
                            where=[(C_LAST,eq,c_query),
                                   (C_W_ID,eq,c_w_id),
                                   (C_D_ID,eq,c_d_id)],
                            order_by=C_FIRST,
                            asc=True)
        else:
            result = select(cursor_=self._cursor,
                            table=CUSTOMER,
                            col=(C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
                                 C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
                                 C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT),
                            where=[(C_ID, eq, c_query),
                                   (C_W_ID, eq, c_w_id),
                                   (C_D_ID, eq, c_d_id)])
        c_id, c_first, c_midele, c_last, \
        c_street_1,c_street_2,c_city,c_state,\
        c_zip,c_phone,c_since,\
        c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt = result[len(result)//2]
        update(cursor_=self._cursor,
               table=CUSTOMER,
               row=[(C_BALANCE,c_balance+Decimal(h_amount)),
                    (C_YTD_PAYMENT,c_ytd_payment+1),
                    (C_PAYMENT_CNT,c_payment_cnt+1)],
               where=[(C_W_ID,eq,w_id),(C_D_ID,eq,d_id),(C_ID,eq,c_id)])
        if c_credit == 'BC':
            c_data = (''.join(map(str,[c_id,c_d_id,c_w_id,d_id,h_amount]))+select(cursor_=self._cursor,
                                                                                  table=CUSTOMER,
                                                                                  col=C_DATA,
                                                                                  where=[(C_ID,eq,c_id),
                                                                                         (C_W_ID,eq,c_w_id),
                                                                                         (C_D_ID,eq,c_d_id)])[0][0] )[0:500]
            update(cursor_=self._cursor,
                   table=CUSTOMER,
                   row=(C_DATA,c_data),
                   where=[(C_W_ID,eq,w_id),(C_D_ID,eq,d_id),(C_ID,eq,c_id)])
        #4 blank space
        h_data = w_name + '    ' + d_name
        insert(cursor_=self._cursor,
               table=HISTORY,
               rows=(c_id,c_d_id,c_w_id,d_id,w_id,current_time(),h_amount,h_data))
        self._conn.commit()
        print('- Payment')
        return True

    def do_order_status(self, w_id, d_id, c_query):
        self._cursor.execute('START TRANSACTION;')
        print('+ Order Status')
        if type(c_query) == str:
            result = select(cursor_=self._cursor,
                            table=CUSTOMER,
                            col=(C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST),
                            where=[(C_LAST, eq, c_query),
                                   (C_W_ID, eq, w_id),
                                   (C_D_ID, eq, d_id)],
                            order_by=C_FIRST,
                            asc=True)
        else:
            result = select(cursor_=self._cursor,
                            table=CUSTOMER,
                            col=(C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST),
                            where=[(C_ID, eq, c_query),
                                   (C_W_ID, eq, w_id),
                                   (C_D_ID, eq, d_id)])
        c_id, c_balance, c_first, c_middle, c_last = result[len(result)//2]
        o_id, o_entry_id, o_carrier_id = select(cursor_=self._cursor,
                                                table=ORDERS,
                                                col=(O_ID,O_ENTRY_D,O_CARRIER_ID),
                                                where=[(O_W_ID, eq, w_id),
                                                       (O_D_ID, eq, d_id),
                                                       (O_C_ID, eq, c_id)],
                                                order_by=O_ID)[0]
        result = select(cursor_=self._cursor,#ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d
                        table=ORDER_LINE,
                        col=(OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY,OL_AMOUNT,OL_DELIVERY_D),
                        where=[(OL_W_ID,eq,w_id),
                               (OL_D_ID,eq,d_id),
                               (OL_O_ID,eq,o_id)])
        self._conn.commit()
        print('- Order Status')
        return True


    def do_delivery(self, w_id, o_carrier_id):
        self._delivery_q.put({'w_id':w_id,'o_carrier_id':o_carrier_id})

    def process_delivery(self, q):
        tmp_conn = MySQLdb.Connect(host=self._host,port=self._port,user=self._user,passwd=self._passwd)
        tmp_cursor = tmp_conn.cursor()
        tmp_cursor.execute('use tpcc;')
        while self._flag:
            if not q.empty():
                t1 = time.time()
                tmp_cursor.execute('START TRANSACTION;')
                print('+ Delivery')
                dat = q.get()
                w_id = dat['w_id']
                o_carrier_id = dat['o_carrier_id']
                for d_id in range(10):
                    try:
                        o_id = select(cursor_=tmp_cursor,
                                      table=NEW_ORDER,
                                      col=NO_O_ID,
                                      where=[(NO_W_ID,eq,w_id),(NO_D_ID,eq,d_id)],
                                      order_by=NO_O_ID,
                                      asc=True)[0][0]
                        delete(cursor_=tmp_cursor,
                               table=NEW_ORDER,
                               where=[(NO_W_ID,eq,w_id),(NO_D_ID,eq,d_id),(NO_O_ID,eq,o_id)])
                    except IndexError:
                        continue
                    o_c_id = select(cursor_=tmp_cursor,
                                    table=ORDERS,
                                    col=O_C_ID,
                                    where=[(O_ID,eq,o_id),(O_W_ID,eq,w_id),(O_D_ID,eq,d_id)])[0][0]
                    update(cursor_=tmp_cursor,
                           table=ORDERS,
                           row=(O_CARRIER_ID,o_carrier_id),
                           where=[(O_ID,eq,o_id),(O_W_ID,eq,w_id),(O_D_ID,eq,d_id)])
                    order_lines = select(cursor_=tmp_cursor,
                                         table=ORDER_LINE,
                                         where=[(OL_W_ID,eq,w_id),(OL_D_ID,eq,d_id),(OL_O_ID,eq,o_id)])
                    ol_amount = select(cursor_=tmp_cursor,
                                       table=ORDER_LINE,
                                       #col='sum(' + OL_AMOUNT + ')',
                                       col = OL_AMOUNT,
                                       where=[(OL_W_ID, eq, w_id), (OL_D_ID, eq, d_id), (OL_O_ID, eq, o_id)]) # [0]
                    #print(ol_amount)
                    ol_amount = [o[0] for o in ol_amount]
                    #print(ol_amount)
                    ol_amount = sum(ol_amount)
                    for line in order_lines:
                        update(cursor_=tmp_cursor,
                               table=ORDER_LINE,
                               row=(OL_DELIVERY_D,current_time()),
                               where=[(OL_W_ID,eq,w_id),(OL_D_ID,eq,d_id),(OL_O_ID,eq,line[0])])

                    c_balance,c_delivery_cnt = select(cursor_=tmp_cursor,
                                                      table=CUSTOMER,
                                                      col=(C_BALANCE,C_DELIVERY_CNT),
                                                      where=[(C_W_ID,eq,w_id),(C_D_ID,eq,d_id),(C_ID,eq,o_c_id)])[0]
                    #print(c_balance, ol_amount, c_delivery_cnt)
                    update(cursor_=tmp_cursor,
                           table=CUSTOMER,
                           row=[(C_BALANCE,c_balance+ol_amount),(C_DELIVERY_CNT,c_delivery_cnt+1)],
                           where=[(C_W_ID,eq,w_id),(C_D_ID,eq,d_id),(C_ID,eq,o_c_id)])
                tmp_conn.commit()
                t2 = time.time()
                put_txn(Delivery,t2-t1,True)
                print('- Delivery')

        tmp_conn.close()
        self._delivery_stop = True

    def do_stock_level(self, w_id, d_id, threshold):
        self._cursor.execute('START TRANSACTION;')
        print('+ Stock Level')
        d_next_o_id = select(cursor_=self._cursor,
                             table=DISTRICT,
                             col=D_NEXT_O_ID,
                             where=[(D_W_ID,eq,w_id),(D_ID,eq,d_id)])[0][0]
        order_lines = select(cursor_=self._cursor,
                             table=ORDER_LINE,
                             where=[(OL_W_ID,eq,w_id),
                                    (OL_D_ID,eq,d_id),
                                    (OL_O_ID,beq,d_next_o_id-20),
                                    (OL_O_ID,lt,d_next_o_id)])
        items = set([order_line[4] for order_line in order_lines])
        low_stock = 0
        for item in items:
            try:
                low_stock +=  select(cursor_=self._cursor,
                                    table=STOCK,
                                    col=S_QUANTITY,
                                    where=[(S_I_ID,eq,item),
                                           (S_W_ID,eq,w_id),
                                           (S_QUANTITY,lt,threshold)])[0][0]
            except IndexError:
                pass
        self._conn.commit()
        print('- Stock Level')
        return True