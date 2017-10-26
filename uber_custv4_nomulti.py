#Developer    : Justin Suelflow
#Date         : 2017-05-02
#Program Name : Customer Data Generator
#Version#     : 6
#Description  : Code that generates customer data
#-----------------------------------------------------------------------------
# History  | ddmmyyyy  |  User     |                Changes
#          | 01192016  | Ivana D.  | Credit Card model,code, ref lists, etc...
#          | 01202016  | Jeff K.   | Comments, ref lists, etc...
#          | 01202016  | Justin S  | SSN distinct list
#          | 04102017  | Justin S  | Bug fixes, math issues
#          | 04252017  | Justin S  | Took out pyTimer to decrease memory usage on cluster
#          | 05022017  | Justin S  | PySpark, multithreading, Customer class
#-----------------------------------------------------------------------------*/
#Reference data is located on the test-bmohb console gs://newccdatav3
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Customers")
sc = SparkContext(conf = conf)

from random import randrange, choice, randint
from datetime import datetime
from barnum import gen_data
from faker import Faker
##from multiprocessing import Pool
##from functools import partial
import csv, NAICS, zips, re, geo_data
##, pyTimer

fake = Faker()
#Dictionary for type of account
Related_Type = ['Primary','Secondary','Joint']
#Dictionary for how the account was opened
Party_Type = ['Person','Non-Person']
#Dictionary for a BMO customer
Party_Relation = ['Customer','Non-Customer']
#Dictionary for random flags
Yes_No_Cust_Flag=['Yes'] + ['No'] * 2 + [''] * 392
#Closed Account flag
Clsd_flag = ['Yes'] + ['No'] * 98
#Dictionary for client whose net worth is over $500K
HighNetWorth = ['Yes'] + ['No'] * 30
#Dictionary for random Yes/No Flag
Yes_No = ['Yes'] + ['No'] * 12
#Dictionary for random Yes/No Consent
Yes_No_Consent = ['Yes'] + ['No'] * 4
#Dictionary for official language
Official_Lang = ['French']+ ['English'] * 3
#Dictionary for method of fakecommunication
Prefered_Channel = ['Direct Mail','Telemarketing','Email','SMS']
#Dictionary for random Client Onboarding flag
Channel_Onboarding=['E-mail','In Person','In person - In Branch/Bank Office','In person - Offsite/Client Location','Mail','Online','Phone','Request for Proposal (RFP)'] + ['Not Applicable'] * 10
#Dictionary for random Transaction flag
Channel_Ongoing_Transactions=['ATM','E-mail','Fax','Mail','Not Applicable','OTC Communication System','Phone'] + ['Online'] * 4 + ['In Person'] * 31
#Dictionary for random occupation
##Occupation=['11-1011 Chief Executives',\
##'11-3011 Administrative Services Managers',\
##'11-3031 Financial Managers',\
##'11-3061 Purchasing Managers',\
##'13-1011 Agents and Business Managers of Artists, Performers, and Athletes',\
##'13-1031 Claims Adjusters, Examiners, and Investigators',\
##'13-1199 Business Operations Specialists, All Other',\
##'13-2099 Financial Specialists, All Other',\
##'17-1011 Architects, Except Landscape and Naval',\
##'23-1011 Lawyers',\
##'23-1023 Judges, Magistrate Judges, and Magistrates',\
##'25-2012 Kindergarten Teachers, Except Special Education',\
##'25-2021 Elementary School Teachers, Except Special Education',\
##'29-1041 Optometrists',\
##'29-2054 Respiratory Therapy Technicians',\
##'33-2011 Firefighters',\
##'37-1012 First-Line Supervisors of Landscaping, Lawn Service, and Groundskeeping Workers',\
##'39-1011 Gaming Supervisors',\
##'39-2011 Animal Trainers',\
##'41-1011 First-Line Supervisors of Retail Sales Workers',\
##'41-1012 First-Line Supervisors of Non-Retail Sales Workers',\
##'41-2011 Cashiers',\
##'41-2031 Retail Salespersons',\
##'43-3021 Billing and Posting Clerks',\
##'45-1011 First-Line Supervisors of Farming, Fishing, and Forestry Workers',\
##'49-2011 Computer, Automated Teller, and Office Machine Repairers',\
##'53-3021 Bus Drivers, Transit and Intercity',\
##'53-4031 Railroad Conductors and Yardmasters',\
##'55-1011 Air Crew Officers',\
##'55-1012 Aircraft Launch and Recovery Officers',\
##'55-1013 Armored Assault Vehicle Officers',\
##]
#Dictionary for random products
Products=['Certificate of Deposit',\
'Checking Account',\
'Credit Card',\
'Custodial and Investment Agency - Institutional',\
'Custodial and Investment Agency - Personal',\
'Custodial/Trust Outsourcing Services (BTOS)',\
'Custody Accounts (PTIM)',\
'Custody Accounts (RSTC)',\
'DTF (BHFA)',\
'Investment Agency - Personal',\
'Investment Management Account (PTIM)',\
'Lease',\
'Loan / Letter of Credit',\
'Money Market',\
'Mortgage / Bond / Debentures',\
'None',\
'Savings Account',\
'Trust Administration - Irrevocable and Revocable (PTIM)',\
'Trust Administration - Irrevocable and Revocable Trusts (BDTC)',\
] + ['Investment Agency - Institutional'] * 5 + ['Nondeposit Investment Products'] * 14
#Dictionary for random Services
Services=['Benefit Payment Services',\
'Domestic Wires and Direct Deposit / ACH',\
'Family Office Services (FOS)',\
'Fiduciary Services',\
'International Wires and IAT',\
'Investment Advisory Services (IAS)',\
'Investment Services',\
'None',\
'Online / Mobile Banking',\
'Payroll',\
'Short Term Cash Management',\
'Trust Services',\
'Trustee Services',\
'Vault Cash Services',\
] + ['Financial Planning'] * 6 + ['Retirement Plans'] * 19
#Dictionary for random SIC_Code
SIC_Code=['6021 National Commercial Banks',\
'6211 Security Brokers Dealers and Flotation Companies',\
'6282 Investment Advice',\
'6311 Life Insurance',\
'6733 Trusts Except Educational Religious and Charitable',\
'8999 Services NEC',\
] + ['6722 Management Investment Offices Open-End'] * 12
#Dictionary for random Market Listing
Stock_Market_Listing=['Australian Stock Exchange',\
'Brussels Stock Exchange',\
'Montreal Stock Exchange',\
'Tiers 1 and 2 of the TSX Venture Exchange (also known as Tiers 1 and 2 of the Canadian Venture Exchange)',\
'Toronto Stock Exchange',\
] + ['Not Found'] * 30
#Dictionary for random Low Net Worth
LowNet=[1,2] + [0]*5
#Dictionary for Consumer vs Business
Acct_Type = ['B'] + ['C'] * 5
#Dictionary for random number of credits cards per account
Number_CC = [4] + [3]*3 + [1]*7 + [2]*11
#Dictionary for random Wolfsberg scenario
Use_Case = [40,41]*2 + [1,4,7,10,13,16,19,22,25,28,31,34,39]*4 + [2,5,8,11,14,17,20,23,26,29,32,35,38]*7 + [3,6,9,12,15,18,21,24,27,30,33,36]*65 + [37]*73
refrating=['1']*3 + ['2','4']*2 + ['3'] + ['5']*12

class Customer(object):
        def __init__(self,i,acct,liSSNMaster, acct_list):
                self.ROWNUM = i
                self.ACCOUNTID = acct
                self.SSN = liSSNMaster[i]
                self.ACCT_TYPE = choice(Acct_Type)
                self.NUM_CCS = choice(Number_CC)
                self.NAME = fake.name()
                self.CUSTEMAIL = gen_data.create_email()
                self.OCCUPATION = gen_data.create_job_title()
                self.COUNTRY = 'US'
                self.PREVIOUS_COUNTRY = 'US'
                self.DOB = gen_data.create_birthday(min_age=2, max_age=85)
                self.PARTY_ENDDATE = gen_data.create_date(past=True)
                self.CONSENT_SHARING = choice(Yes_No_Consent)
                self.LARGE_CASH_EXEMPT = choice(Yes_No)
                self.PARTY_TYPE = choice(Party_Type)
                self.PARTY_RELATION = choice(Party_Relation)
                self.PROB_DEFAULT_RISKR = randrange(0,100,1)
                self.OFFICIAL_LANG_PREF = choice(Official_Lang)
                self.DEPENDANTS_COUNT = randrange(0,5,1)
                self.USE_CASE_SCENARIO = choice(Use_Case)
                self.CLOSEDACCOUNT=choice(Clsd_flag)
                self.HIGH_NET_WORTH = choice(HighNetWorth)
                self.PARTY_STARTDATE = gen_data.create_date(past=True)
                self.ARMS_MANUFACTURER = choice(Yes_No_Cust_Flag)
                self.AUCTION = choice(Yes_No_Cust_Flag)
                self.CASHINTENSIVE_BUSINESS = choice(Yes_No_Cust_Flag)
                self.CASINO_GAMBLING = choice(Yes_No_Cust_Flag)
                self.CHANNEL_ONBOARDING = choice(Channel_Onboarding)
                self.CHANNEL_ONGOING_TRANSACTIONS = choice(Channel_Ongoing_Transactions)
                self.COMPLEX_HI_VEHICLE = choice(Yes_No_Cust_Flag)
                self.DEALER_PRECIOUS_METAL = choice(Yes_No_Cust_Flag)
                self.DIGITAL_PM_OPERATOR = choice(Yes_No_Cust_Flag)
                self.EMBASSY_CONSULATE = choice(Yes_No_Cust_Flag)
                self.EXCHANGE_CURRENCY = choice(Yes_No_Cust_Flag)
                self.FOREIGN_FINANCIAL_INSTITUTION = choice(Yes_No_Cust_Flag)
                self.FOREIGN_GOVERNMENT = choice(Yes_No_Cust_Flag)
                self.FOREIGN_NONBANK_FINANCIAL_INSTITUTION = choice(Yes_No_Cust_Flag)
                self.INTERNET_GAMBLING = choice(Yes_No_Cust_Flag)
                self.MEDICAL_MARIJUANA_DISPENSARY = choice(Yes_No_Cust_Flag)
                self.MONEY_SERVICE_BUSINESS = choice(Yes_No_Cust_Flag)
                self.NAICS_CODE = choice(NAICS.NAICS_Code)
                self.NONREGULATED_FINANCIAL_INSTITUTION = choice(Yes_No_Cust_Flag)
                self.NOT_PROFIT = choice(Yes_No_Cust_Flag)
                self.PRIVATELY_ATM_OPERATOR = choice(Yes_No_Cust_Flag)
                self.PRODUCTS = choice(Products)
                self.SALES_USED_VEHICLES = choice(Yes_No_Cust_Flag)
                self.SERVICES = choice(Services)
                self.SIC_CODE = choice(SIC_Code)
                self.STOCK_MARKET_LISTING = choice(Stock_Market_Listing)
                self.THIRD_PARTY_PAYMENT_PROCESSOR = choice(Yes_No_Cust_Flag)
                self.TRANSACTING_PROVIDER = choice(Yes_No_Cust_Flag)
                self.ZIP = choice(zips.zip)
                self.PREVIOUS_ZIP = choice(zips.zip)
                addr = geo_data.create_city_state_zip[self.ZIP]
                addr2 = geo_data.create_city_state_zip[self.PREVIOUS_ZIP]
                self.CITY = addr[0]
                self.STATE = addr[1]
                self.PREVIOUS_CITY = addr2[0]
                self.PREVIOUS_STATE = addr2[1]
                self.PRIMARY_BRANCH_NO = self.ZIP
                tmp=gen_data.create_name()
                self.M_NAME = tmp[0]
                self.EMPLOYER = gen_data.create_company_name()+' '+tmp[1]
                No_CCs = choice(Number_CC)
                #Dictionary for names list set to blank
                names=[]
                #Dictionary for Social Security Number list set to blank
                ssn=[]
                #Middle Name to reduce name dups
                mdl=[]
                #Generates Name and SSN for Credit Users
                for j in xrange(4):
                        if No_CCs > j:
                                names.insert(j,fake.name())
                                tmp2=gen_data.create_name()
                                mdl.insert(j,tmp2[0])
                                randInt = randrange(1,len(liSSNMaster),1)
                                if randInt != i:
                                        ssn.insert(j,liSSNMaster[randInt])
                                else:
                                        ssn.insert(j,liSSNMaster[randInt - 1])
                        #Name and SSN is set to blank if less than 4 customers on an account
                        else:
                                names.insert(No_CCs+j,'')
                                ssn.insert(No_CCs+j,'')
                                mdl.insert(No_CCs+j,'')
                
                self.AUTHORIZED_NAME2 = names[0]
                self.M_NAME2 = mdl[0]
                self.SSN2 = ssn[0]
                self.AUTHORIZED_NAME3 = names[1]
                self.M_NAME3 = mdl[1]
                self.SSN3 = ssn[1]
                self.AUTHORIZED_NAME4 = names[2]
                self.M_NAME4 = mdl[2]
                self.SSN4 = ssn[2]
                
                #Sets CC_NO to a random credit card number
                CC_NO = gen_data.create_cc_number()
                CC_TRANS = CC_NO[1][0]
                dt = str(datetime.now())
                clean = re.sub('\W','',dt)
                self.CREDITCARDNUMBER = str(CC_TRANS[-4:])+str(clean[-12:-3])+str(randrange(1111,9999,randrange(1,10,1)))
                self.CREDITCARDTYPE = CC_NO[0]
                
                self.RELATED_ACCT = ''
                self.RELATED_TYPE = ''
                if i > 10000:
                        rel = int(choice(acct_list))*max((randrange(0,10001,1)-9999),0)
                        if rel <> 0:
                                self.RELATED_ACCT = rel
                                self.RELATED_TYPE = choice(Related_Type)
                
                self.PREFERRED_CHANNEL = ''
                if self.CONSENT_SHARING == 'Yes':
                        self.PREFERRED_CHANNEL = choice(Prefered_Channel)
                
##              #Generates Segment ID then adds additional Segment data based on the selection to the current csv row
                Segment_ID = randrange(0,5,1)
                if Segment_ID == 0:
                        self.SEG_MODEL_ID = '01'
                        self.SEG_MODEL_TYPE = 'LOB Specific'
                        self.SEG_MODEL_NAME = 'IRRI'
                        self.SEG_MODEL_GROUP = 'Group 1'
                        self.SEG_M_GRP_DESC = 'High Risk Tier'
                        self.SEG_MODEL_SCORE = '200'
                if Segment_ID == 1:
                        self.SEG_MODEL_ID = '02'
                        self.SEG_MODEL_TYPE = 'Profitability'
                        self.SEG_MODEL_NAME = 'CRS Risk Score'
                        self.SEG_MODEL_GROUP = 'Group 1'
                        self.SEG_M_GRP_DESC = 'Mid Risk Tier'
                        self.SEG_MODEL_SCORE = '300'
                if Segment_ID == 2:
                        self.SEG_MODEL_ID = '03'
                        self.SEG_MODEL_TYPE = 'Geographical'
                        self.SEG_MODEL_NAME = 'Geo Risk'
                        self.SEG_MODEL_GROUP = 'Group 2'
                        self.SEG_M_GRP_DESC = 'Low Risk Tier'
                        self.SEG_MODEL_SCORE = '400'
                if Segment_ID == 3:
                        self.SEG_MODEL_ID = '04'
                        self.SEG_MODEL_TYPE = 'Behavioral'
                        self.SEG_MODEL_NAME = 'Financial Behavior Risk'
                        self.SEG_MODEL_GROUP = 'Group 3'
                        self.SEG_M_GRP_DESC = 'Vertical Risk'
                        self.SEG_MODEL_SCORE = '100'
                if Segment_ID == 4:
                        self.SEG_MODEL_ID = '05'
                        self.SEG_MODEL_TYPE = 'Risk Tolerance'
                        self.SEG_MODEL_NAME = 'CM Risk'
                        self.SEG_MODEL_GROUP = 'Group 4'
                        self.SEG_M_GRP_DESC = 'Geographical Risk'
                        self.SEG_MODEL_SCORE = '500'
                
                self.CLIENT_NET_WORTH = ''
                if self.HIGH_NET_WORTH == 'Yes':
                        self.CLIENT_NET_WORTH = max(max((randrange(0,101,1)-99),0)*randrange(5000000,25000000,1),randrange(1000000,5000000,1))
                else:
                        flag=choice(LowNet)
                        if flag==0:
                                self.CLIENT_NET_WORTH = randrange(-250000,600000,1)
                        else:
                                if flag==1:
                                        self.CLIENT_NET_WORTH = randrange(149000,151000,1)
                                else:
                                        self.CLIENT_NET_WORTH = randrange(40000,50000,1)
                
                #Politically Exposed Person
                self.PEP='No'
                #1% of accounts are PEP
                if (max((randrange(0,101,1)-99),0)==1):
                        self.PEP='Yes'
                
                #Customer that was demarketed by the bank
                self.DEMARKET_FLAG='No'
                self.DEMARKET_DATE=''
                #Customer with a Suspicous Activity Report
                self.SAR='No'
                #Customer with a closed account
                #generate closed acct flag
                #Condition for SARs and Demarketed Clients
                if(self.CLOSEDACCOUNT=='Yes'):
                        #1% of closed accounts are demarketed but never had a SAR filed
                        if (max((randrange(0,101,1)-99),0)==1):
                                self.DEMARKET_FLAG='Yes'
                                self.DEMARKET_DATE=gen_data.create_date(past=True)
                        if (self.DEMARKET_FLAG=='No' and max((randrange(0,11,1)-9),0)==1):
                                #10% of closed accounts have SARs
                                self.SAR='Yes'
                                #90% of closed accounts with SARs are demarketed
                                if(max((randrange(0,11,1)-9),0)==0):
                                        self.DEMARKET_FLAG='Yes'
                                        self.DEMARKET_DATE=gen_data.create_date(past=True)
                
                self.HIGH_RISK = 'No'
                self.RISK_RATING = ''
                if (self.PEP=='Yes' or self.SAR=='Yes' or self.LARGE_CASH_EXEMPT=='Yes' or self.DEMARKET_FLAG=='Yes' or self.ARMS_MANUFACTURER=='Yes'
                   or self.AUCTION=='Yes' or self.CASHINTENSIVE_BUSINESS=='Yes' or self.CASINO_GAMBLING=='Yes' or self.COMPLEX_HI_VEHICLE=='Yes' or self.DEALER_PRECIOUS_METAL=='Yes'
                   or self.DIGITAL_PM_OPERATOR=='Yes' or self.EMBASSY_CONSULATE=='Yes' or self.EXCHANGE_CURRENCY=='Yes' or self.FOREIGN_FINANCIAL_INSTITUTION=='Yes'
                   or self.FOREIGN_GOVERNMENT=='Yes' or self.FOREIGN_NONBANK_FINANCIAL_INSTITUTION=='Yes' or self.INTERNET_GAMBLING=='Yes' or self.MEDICAL_MARIJUANA_DISPENSARY=='Yes'
                   or self.MONEY_SERVICE_BUSINESS=='Yes' or self.NONREGULATED_FINANCIAL_INSTITUTION=='Yes' or self.NOT_PROFIT=='Yes' or self.PRIVATELY_ATM_OPERATOR=='Yes'
                   or self.SALES_USED_VEHICLES=='Yes' or self.THIRD_PARTY_PAYMENT_PROCESSOR=='Yes' or self.TRANSACTING_PROVIDER=='Yes' or self.HIGH_NET_WORTH=='Yes'):
                        self.HIGH_RISK='Yes'
                        self.RISK_RATING=choice(refrating)
                elif(max((randrange(0,101,1)-99),0)==1):
                        self.HIGH_RISK='Yes'
                        self.RISK_RATING=choice(refrating)

def createSSNs(liSSN, count):
        for i in xrange(int(round(count*1.05))):
                liSSN.append(''.join(str(randint(0,9)) for _ in xrange(9)))
        liSSN = list(set(liSSN))
        if len(liSSN) < count:
                createSSNs(liSSN, count)
        return liSSN

##def createCustData(liSSNMaster, ccount, remainder, lenI, acct_list, iterator):
def createCustData(liSSNMaster, acct_list, ccount):
        liCust=[]
##        if iterator == 0:
##            x = xrange(ccount)
##        else:
##            if iterator < (lenI-1):
##                x = xrange(iterator * ccount, (iterator+1) * ccount)
##            else:
##                x = xrange(iterator * ccount, ((iterator * ccount) + (ccount+remainder)))
        #Loop for number of accounts to generate
##        for i in x:
        for i in xrange(ccount):
                acct = "{0:0>8}".format(i)
                acct = str(acct) + str(randrange(1,10,1))
                acct_list.extend([acct])
                cust = Customer(i,acct,liSSNMaster,acct_list)
                row=[cust.ROWNUM]+[cust.ACCOUNTID]+[cust.ACCT_TYPE]+[cust.NUM_CCS]+[cust.NAME]+[cust.M_NAME]+[cust.SSN]+[cust.AUTHORIZED_NAME2]+[cust.M_NAME2]+[cust.SSN2]+\
                [cust.AUTHORIZED_NAME3]+[cust.M_NAME3]+[cust.SSN3]+[cust.AUTHORIZED_NAME4]+[cust.M_NAME4]+[cust.SSN4]+[cust.CREDITCARDNUMBER]+[cust.CREDITCARDTYPE]+[cust.EMPLOYER]+\
                [cust.CUSTEMAIL]+['"'+cust.OCCUPATION+'"']+[cust.CITY]+[cust.STATE]+[cust.ZIP]+[cust.COUNTRY]+[cust.PREVIOUS_CITY]+[cust.PREVIOUS_STATE]+[cust.PREVIOUS_ZIP]+[cust.PREVIOUS_COUNTRY]+\
                [cust.DOB]+[cust.PEP]+[cust.SAR]+[cust.CLOSEDACCOUNT]+[cust.RELATED_ACCT]+[cust.RELATED_TYPE]+[cust.PARTY_TYPE]+[cust.PARTY_RELATION]+[cust.PARTY_STARTDATE]+[cust.PARTY_ENDDATE]+\
                [cust.LARGE_CASH_EXEMPT]+[cust.DEMARKET_FLAG]+[cust.DEMARKET_DATE]+[cust.PROB_DEFAULT_RISKR]+[cust.OFFICIAL_LANG_PREF]+[cust.CONSENT_SHARING]+[cust.PREFERRED_CHANNEL]+\
                [cust.PRIMARY_BRANCH_NO]+[cust.DEPENDANTS_COUNT]+[cust.SEG_MODEL_ID]+[cust.SEG_MODEL_TYPE]+[cust.SEG_MODEL_NAME]+[cust.SEG_MODEL_GROUP]+[cust.SEG_M_GRP_DESC]+\
                [cust.SEG_MODEL_SCORE]+[cust.ARMS_MANUFACTURER]+[cust.AUCTION]+[cust.CASHINTENSIVE_BUSINESS]+[cust.CASINO_GAMBLING]+[cust.CHANNEL_ONBOARDING]+[cust.CHANNEL_ONGOING_TRANSACTIONS]+\
                [cust.CLIENT_NET_WORTH]+[cust.COMPLEX_HI_VEHICLE]+[cust.DEALER_PRECIOUS_METAL]+[cust.DIGITAL_PM_OPERATOR]+[cust.EMBASSY_CONSULATE]+[cust.EXCHANGE_CURRENCY]+\
                [cust.FOREIGN_FINANCIAL_INSTITUTION]+[cust.FOREIGN_GOVERNMENT]+[cust.FOREIGN_NONBANK_FINANCIAL_INSTITUTION]+[cust.INTERNET_GAMBLING]+[cust.MEDICAL_MARIJUANA_DISPENSARY]+\
                [cust.MONEY_SERVICE_BUSINESS]+[cust.NAICS_CODE]+[cust.NONREGULATED_FINANCIAL_INSTITUTION]+[cust.NOT_PROFIT]+[cust.PRIVATELY_ATM_OPERATOR]+[cust.PRODUCTS]+\
                [cust.SALES_USED_VEHICLES]+[cust.SERVICES]+[cust.SIC_CODE]+[cust.STOCK_MARKET_LISTING]+[cust.THIRD_PARTY_PAYMENT_PROCESSOR]+[cust.TRANSACTING_PROVIDER]+[cust.HIGH_NET_WORTH]+\
                [cust.HIGH_RISK]+[cust.RISK_RATING]+[cust.USE_CASE_SCENARIO]
                liCust.append(row)
        return liCust

def main():
        #####Customer Count wanted for the end file######
##        startTime=pyTimer.startTimer()
        cust_count=18500000
        liSSNMaster=[]
        liSSNMaster=createSSNs(liSSNMaster, cust_count)
##        pyTimer.endTimer(startTime,'Creating ' + str(len(liSSNMaster)) + ' SSNs for customers')
##        chk1Time=pyTimer.startTimer()
        cust_list=[]
        acct_list=[]
##        proc = 16
##        iterator = xrange(proc)
##        remainder = cust_count % proc
##        ccount = cust_count/proc
##        lenI = len(iterator)
##        func = partial(createCustData, liSSNMaster, ccount, remainder, lenI, acct_list)
##        pool = Pool(processes=proc)
##        results = pool.map(func, iterator)
##        cust_list = results[0] + results[1] + results[2] + results[3] + results[4] + results[5] + results[6] + results[7] + results[8] + results[9] + results[10] + results[11] + results[12] + results[13] + results[14] + results[15]
        cust_list = createCustData(liSSNMaster, acct_list, cust_count)
##        endLoopTime = pyTimer.startTimer()
##        avgLoopTime = round(((endLoopTime - chk1Time)/cust_count), 6)
##        avgLoopTime = ("{0:.6f}".format(avgLoopTime))
##        pyTimer.writeRuntimeLog('The average time to create a customer is: ' + str(avgLoopTime) + ' seconds\n')
##        cust_list.append(['ROWNUM']+['ACCOUNTID']+['ACCT_TYPE']+['NUM_CCS']+['NAME']+['M_NAME']+['SSN']+['AUTHORIZED_NAME2']+['M_NAME2']+['SSN2']+\
##        ['AUTHORIZED_NAME3']+['M_NAME3']+['SSN3']+['AUTHORIZED_NAME4']+['M_NAME4']+['SSN4']+['CREDITCARDNUMBER']+['CREDITCARDTYPE']+['EMPLOYER']+['CUSTEMAIL']+\
##        ['OCCUPATION']+['CITY']+['STATE']+['ZIP']+['COUNTRY']+['PREVIOUS_CITY']+['PREVIOUS_STATE']+\
##        ['PREVIOUS_ZIP']+['PREVIOUS_COUNTRY']+['DOB']+['PEP']+['SAR']+['CLOSEDACCOUNT']+['RELATED_ACCT']+['RELATED_TYPE']+['PARTY_TYPE']+['PARTY_RELATION']+['PARTY_STARTDATE']+['PARTY_ENDDATE']+\
##        ['LARGE_CASH_EXEMPT']+['DEMARKET_FLAG']+['DEMARKET_DATE']+['PROB_DEFAULT_RISKR']+['OFFICIAL_LANG_PREF']+['CONSENT_SHARING']+\
##        ['PREFERRED_CHANNEL']+['PRIMARY_BRANCH_NO']+['DEPENDANTS_COUNT']+['SEG_MODEL_ID']+['SEG_MODEL_TYPE']+\
##        ['SEG_MODEL_NAME']+['SEG_MODEL_GROUP']+['SEG_M_GRP_DESC']+['SEG_MODEL_SCORE']+['ARMS_MANUFACTURER']+['AUCTION']+\
##        ['CASHINTENSIVE_BUSINESS']+['CASINO_GAMBLING']+['CHANNEL_ONBOARDING']+['CHANNEL_ONGOING_TRANSACTIONS']+['CLIENT_NET_WORTH']+\
##        ['COMPLEX_HI_VEHICLE']+['DEALER_PRECIOUS_METAL']+['DIGITAL_PM_OPERATOR']+['EMBASSY_CONSULATE']+['EXCHANGE_CURRENCY']+\
##        ['FOREIGN_FINANCIAL_INSTITUTION']+['FOREIGN_GOVERNMENT']+['FOREIGN_NONBANK_FINANCIAL_INSTITUTION']+['INTERNET_GAMBLING']+\
##        ['MEDICAL_MARIJUANA_DISPENSARY']+['MONEY_SERVICE_BUSINESS']+['NAICS_CODE']+['NONREGULATED_FINANCIAL_INSTITUTION']+\
##        ['NOT_PROFIT']+['PRIVATELY_ATM_OPERATOR']+['PRODUCTS']+['SALES_USED_VEHICLES']+['SERVICES']+\
##        ['SIC_CODE']+['STOCK_MARKET_LISTING']+['THIRD_PARTY_PAYMENT_PROCESSOR']+['TRANSACTING_PROVIDER']+['HIGH_NET_WORTH']+['HIGH_RISK']+['RISK_RATING']+['USE_CASE_SCENARIO'])
        lines=sc.parallelize(cust_list)
        lines.saveAsTextFile("Customers")
        #Creates CSV
##        with open('customersv4.csv','w') as f1:
##                #Writer for CSV...Pipe delimited...Return for a new line
##                writer=csv.writer(f1, delimiter='|',lineterminator='\n',)
##                #Header Row
##                writer.writerow(['ROWNUM']+['ACCOUNTID']+['ACCT_TYPE']+['NUM_CCS']+['NAME']+['M_NAME']+['SSN']+['AUTHORIZED_NAME2']+['M_NAME2']+['SSN2']+\
##                ['AUTHORIZED_NAME3']+['M_NAME3']+['SSN3']+['AUTHORIZED_NAME4']+['M_NAME4']+['SSN4']+['CREDITCARDNUMBER']+['CREDITCARDTYPE']+['EMPLOYER']+['CUSTEMAIL']+\
##                ['OCCUPATION']+['CITY']+['STATE']+['ZIP']+['COUNTRY']+['PREVIOUS_CITY']+['PREVIOUS_STATE']+\
##                ['PREVIOUS_ZIP']+['PREVIOUS_COUNTRY']+['DOB']+['PEP']+['SAR']+['CLOSEDACCOUNT']+['RELATED_ACCT']+['RELATED_TYPE']+['PARTY_TYPE']+['PARTY_RELATION']+['PARTY_STARTDATE']+['PARTY_ENDDATE']+\
##                ['LARGE_CASH_EXEMPT']+['DEMARKET_FLAG']+['DEMARKET_DATE']+['PROB_DEFAULT_RISKR']+['OFFICIAL_LANG_PREF']+['CONSENT_SHARING']+\
##                ['PREFERRED_CHANNEL']+['PRIMARY_BRANCH_NO']+['DEPENDANTS_COUNT']+['SEG_MODEL_ID']+['SEG_MODEL_TYPE']+\
##                ['SEG_MODEL_NAME']+['SEG_MODEL_GROUP']+['SEG_M_GRP_DESC']+['SEG_MODEL_SCORE']+['ARMS_MANUFACTURER']+['AUCTION']+\
##                ['CASHINTENSIVE_BUSINESS']+['CASINO_GAMBLING']+['CHANNEL_ONBOARDING']+['CHANNEL_ONGOING_TRANSACTIONS']+['CLIENT_NET_WORTH']+\
##                ['COMPLEX_HI_VEHICLE']+['DEALER_PRECIOUS_METAL']+['DIGITAL_PM_OPERATOR']+['EMBASSY_CONSULATE']+['EXCHANGE_CURRENCY']+\
##                ['FOREIGN_FINANCIAL_INSTITUTION']+['FOREIGN_GOVERNMENT']+['FOREIGN_NONBANK_FINANCIAL_INSTITUTION']+['INTERNET_GAMBLING']+\
##                ['MEDICAL_MARIJUANA_DISPENSARY']+['MONEY_SERVICE_BUSINESS']+['NAICS_CODE']+['NONREGULATED_FINANCIAL_INSTITUTION']+\
##                ['NOT_PROFIT']+['PRIVATELY_ATM_OPERATOR']+['PRODUCTS']+['SALES_USED_VEHICLES']+['SERVICES']+\
##                ['SIC_CODE']+['STOCK_MARKET_LISTING']+['THIRD_PARTY_PAYMENT_PROCESSOR']+['TRANSACTING_PROVIDER']+['HIGH_NET_WORTH']+['HIGH_RISK']+['RISK_RATING']+['USE_CASE_SCENARIO'])
##                for row in cust_list:
##                        writer.writerow(row)
##        pyTimer.endTimer(startTime,str(cust_count)+' Customer creation')

if __name__ == "__main__":
    main()
