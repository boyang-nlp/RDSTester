from mysql.driver import Driver
from tester import do_test
from record.record import analysis
from record.record import build_db
from db.conf import cnf
import numpy as np
import matplotlib.pyplot as plt
import os
"""
driver = Driver(host=host, port=port, user=user, passwd=passwd, scale=1)
build_db()
######
#driver.build()
#driver.populate()
#####
do_test(driver, 600)
print('Test Finished')
driver.__del__()
result ,new_order_result = analysis()
for r in result:
    print(r['name'],' - ','\navg time: ', r['avg'], '\ntotal: ',r['total'],'\nsuccess: ',r['success'])
X = np.array([e[1] for e in new_order_result])
Y = np.array([e[0] for e in new_order_result])
plt.plot(X,Y)
plt.show()
os.remove('rds.db')
"""
i  = 0
for host,  user, passwd, port, label, color in cnf:
    driver = Driver(host=host, port=port, user=user, passwd=passwd, scale=1)
    build_db()
    ######
    # If a database to be tested has not been initialized, uncomment following lines.
    #driver.build()
    #driver.populate()
    #####
    do_test(driver, 120)# #2 parameter is test duration time in seconds.
    print('Test Finished')
    driver.__del__()
    result ,new_order_result = analysis()
    f = open('result/'+label+'.txt', 'w')
    for r in result:
        f.write(str(r['name']+' - '+'\navg time: '+ str(r['avg'])+ '\ntotal: '+str(r['total'])+'\nsuccess: '+str(r['success'])))
    X = np.array([e[1] for e in new_order_result])
    Y = np.array([e[0] for e in new_order_result])
    np.save('result/'+label+'X', X)
    np.save('result/'+label+'Y', Y)
    plt.plot(X, Y, color=color, label=label)
    os.remove('rds.db')
    i += 1
    print('task %d finished.'%(i))

plt.legend(loc='upper left')
plt.ylabel('Number of New-Orders')
plt.xlabel('Time unit: second')
plt.show()