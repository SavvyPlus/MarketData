import pymssql
import sys


def lambda_handler(event, context):

    try:
        connection = pymssql.connect(server='test-2-savvy-work.ciimkozo2x5b.ap-southeast-2.rds.amazonaws.com', user='djangounchained', password='N))$I!BJDA6y', database='EnergyAccounting')

        print("Connected...\n")

        cursor = connection.cursor()
        cursor.execute('SELECT id, account_reference FROM [EnergyAccounting].[dbo].[invoice_account]')

        print('query executed...\n')

        row = cursor.fetchone()
        while row:
            print(str(row[0]) + " " + str(row[1]))
            row = cursor.fetchone()
    except:
        print("Unexpected error: ", sys.exc_info()[0] )
