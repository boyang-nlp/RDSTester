from mysql.driver import Driver
from util import *
import time
import random
import math
from record.record import *
def do_test(driver, duration):
    print(duration)
    print('Test')
    t_start = time.time()
    while True:
        txn = random.randrange(100)
        if txn >= 0 and txn < 45:
            d_id = get_d_id()
            c_id = get_c_id()
            ol_i_id = get_ol_i_id()
            ol_supply_w_id = get_ol_supply_w_id(0, 1, len(ol_i_id))
            ol_quantity = get_ol_quantity(len(ol_i_id))

            t1 = time.time()
            ret = driver.do_new_order(0, d_id, c_id, ol_i_id, ol_supply_w_id, ol_quantity)
            t2 = time.time()

            put_new_order(t2-t_start)
            put_txn(NewOrder, t2-t1, ret)

        elif txn >= 45 and txn < 88:
            d_id = get_d_id()
            c_w_id, c_d_id = get_c_w_id_d_id(0, d_id, 1)

            t1 = time.time()
            ret = driver.do_payment(0, d_id, c_w_id, c_d_id, query_cus_by(), random.random() * (5000 - 1) + 1)
            t2 = time.time()

            put_txn(Payment, t2-t1, ret)

        elif txn >= 88 and txn < 92:
            t1 = time.time()
            ret = driver.do_order_status(0, get_d_id(), query_cus_by())
            t2 = time.time()
            put_txn(OrderStatus, t2-t1, ret)

        elif txn >= 92 and txn < 96:
            t1 = time.time()
            ret = driver.do_stock_level(0, get_d_id(), random.randrange(10, 21))
            t2 = time.time()
            put_txn(StockLevel, t2-t1, ret)

        else:
            driver.do_delivery(0, get_o_carrier_id())
        t_end = time.time()
        if(t_start + duration <= t_end):
            break

