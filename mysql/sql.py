from db.table_layouts import *
#Operator
ALL = '*'
SELECT = 'select'
FROM = 'from'
WHERE = 'where'
AND = ' and '
ORDER_BY = 'order by'
DESC = 'desc'
ASC = 'asc'
INSERT = 'insert'
VALUES = 'values'
UPDATE = 'update'
DELETE = 'delete'
SET = 'set'
eq = '='
bt = '>'
lt = '<'
beq = '>='
leq = '<='


def select(cursor_, table, col = ALL, where = False, order_by = False, asc = False):
    if type(col)!=tuple:
        col = [col]
    if where and type(where)!=list:
        where = [where]
    param = [ele[-1] for ele in where]
    gen = lambda ele: str(ele[0]) + str(ele[1]) + '%s'
    where = ' '.join( [ WHERE, AND.join( [ gen(ele) for ele in where] ) ] ) if where else ''
    order_by = ' '.join([ORDER_BY,order_by,ASC if asc else DESC]) if order_by else ''
    sql = ' '.join([SELECT,','.join(col),FROM,table,where,order_by,';'])
    #print(sql, param)
    cursor_.execute(sql, param)
    return cursor_.fetchall()


def insert(cursor_, table, rows):
    values = ''.join([VALUES,'(',','.join(['%s' for i in range(num_of_cols[table])]),')'])
    sql = ' '.join([INSERT,table,values,';'])
    #print(sql)
    if type(rows[0]) != list:
        rows = [rows]
    cursor_.executemany(sql, rows)


def update(cursor_,table, row, where = False):
    if type(row)!=list:
        row = [row]
    if type(where)!=list:
        where = [where]
    param = [ele[-1] for ele in where]
    gen = lambda ele: str(ele[0]) + str(ele[1]) + '%s'
    where = ' '.join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ''
    var = [e[0]+'=%s' for e in row]
    val = [e[1] for e in row]

    sql = ' '.join([UPDATE,table,SET,','.join(var),where,';'])
    #print(sql,val+param)
    cursor_.execute(sql,val+param)

def delete(cursor_,table, where):
    if type(where)!=list:
        where = [where]
    param = [ele[-1] for ele in where]
    gen = lambda ele: str(ele[0]) + str(ele[1]) + '%s'
    where = ' '.join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ''
    sql = ' '.join([DELETE,FROM,table,where,';'])
    cursor_.execute(sql, param)
