import subprocess, glob, csv, datetime
hadoopPath="Transactions/"
subprocess.call(["hadoop","fs","-get",hadoopPath])

path='Transactions/part-*'
files = glob.glob(path)

try:
        with open('cc_trans.csv','w') as f1:
                writer=csv.writer(f1, delimiter='|',lineterminator='\n',)
                #File header
                writer.writerow(['ROWNUM']+['ACCOUNTID']+['MERCHANT_NAME']+['MERCHANT_CATEGORY_CODE']+['MERCHANT_CATEGORY_DESC']+['MERCHANT_COUNTRY']+\
                                ['POST_DATE']+['TRANSACTION_DATE']+['TRANSACTION_TYPE']+['CREDIT_DEBIT']+['CREDIT_LIMIT']+['AMOUNT']+['BALANCE']+\
                                ['CREDITCARDNUMBER']+['CC_TYPE']+['USE_CASE']+['RISK_COLOR']+['RISK_NUM']+['CUST_NAME']+['NUM_CCS']+['CUST_CITY']+['CUST_STATE']+['CUST_ZIP']+['CUST_COUNTRY']+['TRANS_DETAIL'])
                for file in files:
                        with open(file) as f:
                                content = f.readlines()
                                for row in content:
                                        row=eval(row)
                                        writer.writerow(row)

        subprocess.call(["hadoop","fs","-rm","-f","-R",hadoopPath])
except:
        print('Did not merge files')
