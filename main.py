# import cx_Oracle
#
# con = cx_Oracle.connect('oltp_user/Udit@1234/orcl')
# print(con.version)
# con.close()
import cx_Oracle

import cx_Oracle
import Config
import json

connection = None
try:
    f = open('C:\\Users\\Udit\\PycharmProjects\\DBConnection\\venv\\Migration.JSON')
    data = json.load(f)
    print(data["Migration_Info"][0])
    arr_len = len(data["Migration_Info"])
    connection = cx_Oracle.connect(
        Config.username,
        Config.password,
        Config.dsn,
        encoding=Config.encoding)

    # show the version of the Oracle Database
    print(connection.version)
    cursor = connection.cursor()
    for i in range(arr_len):
        staging_user = data["Migration_Info"][i]
        print(staging_user)
        truncate_query = 'truncate table ' + staging_user.split('|')[2] + '.' + staging_user.split('|')[3]
        insert_query = 'insert into ' + staging_user.split('|')[2] + '.' + staging_user.split('|')[3] + ' select * from ' + staging_user.split('|')[0] + '.' + staging_user.split('|')[1]
        # timestamp = 'select max'
        print(insert_query)
        cursor.execute(truncate_query)
        cursor.execute(insert_query)
        connection.commit()
except cx_Oracle.Error as error:
    print(error)
finally:
    # release the connection
    if connection:
        connection.close()
