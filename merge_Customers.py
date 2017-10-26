import subprocess, glob, csv, datetime
hadoopPath="Customers/"
subprocess.call(["hadoop","fs","-get",hadoopPath])

path='Customers/part-*'
files = glob.glob(path)

try:
        with open('uber_cust.csv','w') as f1:
                writer=csv.writer(f1, delimiter='|',lineterminator='\n',)
                #File header
                writer.writerow(['ROWNUM']+['ACCOUNTID']+['ACCT_TYPE']+['NUM_CCS']+['NAME']+['M_NAME']+['SSN']+['AUTHORIZED_NAME2']+['M_NAME2']+['SSN2']+\
                ['AUTHORIZED_NAME3']+['M_NAME3']+['SSN3']+['AUTHORIZED_NAME4']+['M_NAME4']+['SSN4']+['CREDITCARDNUMBER']+['CREDITCARDTYPE']+['EMPLOYER']+['CUSTEMAIL']+\
                ['OCCUPATION']+['CITY']+['STATE']+['ZIP']+['COUNTRY']+['PREVIOUS_CITY']+['PREVIOUS_STATE']+\
                ['PREVIOUS_ZIP']+['PREVIOUS_COUNTRY']+['DOB']+['PEP']+['SAR']+['CLOSEDACCOUNT']+['RELATED_ACCT']+['RELATED_TYPE']+['PARTY_TYPE']+['PARTY_RELATION']+['PARTY_STARTDATE']+['PARTY_ENDDATE']+\
                ['LARGE_CASH_EXEMPT']+['DEMARKET_FLAG']+['DEMARKET_DATE']+['PROB_DEFAULT_RISKR']+['OFFICIAL_LANG_PREF']+['CONSENT_SHARING']+\
                ['PREFERRED_CHANNEL']+['PRIMARY_BRANCH_NO']+['DEPENDANTS_COUNT']+['SEG_MODEL_ID']+['SEG_MODEL_TYPE']+\
                ['SEG_MODEL_NAME']+['SEG_MODEL_GROUP']+['SEG_M_GRP_DESC']+['SEG_MODEL_SCORE']+['ARMS_MANUFACTURER']+['AUCTION']+\
                ['CASHINTENSIVE_BUSINESS']+['CASINO_GAMBLING']+['CHANNEL_ONBOARDING']+['CHANNEL_ONGOING_TRANSACTIONS']+['CLIENT_NET_WORTH']+\
                ['COMPLEX_HI_VEHICLE']+['DEALER_PRECIOUS_METAL']+['DIGITAL_PM_OPERATOR']+['EMBASSY_CONSULATE']+['EXCHANGE_CURRENCY']+\
                ['FOREIGN_FINANCIAL_INSTITUTION']+['FOREIGN_GOVERNMENT']+['FOREIGN_NONBANK_FINANCIAL_INSTITUTION']+['INTERNET_GAMBLING']+\
                ['MEDICAL_MARIJUANA_DISPENSARY']+['MONEY_SERVICE_BUSINESS']+['NAICS_CODE']+['NONREGULATED_FINANCIAL_INSTITUTION']+\
                ['NOT_PROFIT']+['PRIVATELY_ATM_OPERATOR']+['PRODUCTS']+['SALES_USED_VEHICLES']+['SERVICES']+\
                ['SIC_CODE']+['STOCK_MARKET_LISTING']+['THIRD_PARTY_PAYMENT_PROCESSOR']+['TRANSACTING_PROVIDER']+['HIGH_NET_WORTH']+['HIGH_RISK']+['RISK_RATING']+['USE_CASE_SCENARIO'])
                for file in files:
                        with open(file) as f:
                                content = f.readlines()
                                for row in content:
                                        row=eval(row)
                                        writer.writerow(row)

        subprocess.call(["hadoop","fs","-rm","-f","-R",hadoopPath])
except:
        print('Did not merge files')
