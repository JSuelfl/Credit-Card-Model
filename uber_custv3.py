#Developer    : Ivana Donevska
#Date         : 2016-01-20
#Program Name : Customer DataGenerator
#Version#     : 5
#Description  : Code that generates customer data
#-----------------------------------------------------------------------------
# History  | ddmmyyyy  |  User     |                Changes
#          | 01192016  | Ivana D.  | Credit Card model,code, ref lists, etc...
#          | 01202016  | Jeff K.   | Comments, ref lists, etc...
#          | 01202016  | Justin S  | SSN distinct list
#-----------------------------------------------------------------------------*/
#Reference data is located on the test-bmohb console gs://newccdatav3
##from pyspark import SparkConf, SparkContext
##conf = SparkConf().setAppName("Customers")
##sc = SparkContext(conf = conf)

from random import randrange, choice, randint
from datetime import datetime
from barnum import gen_data
from faker import Faker
from multiprocessing import Pool
from functools import partial
import csv, NAICS, zips, re, geo_data, pyTimer

startTime=pyTimer.startTimer()
start=10786147
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
Preffered_Channel = ['Direct Mail','Telemarketing','Email','SMS']
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

def createSSNs(liSSN, count):
        for i in xrange(int(round(count*1.05))):
                liSSN.append(''.join(str(randint(0,9)) for _ in xrange(9)))
        liSSN = list(set(liSSN))
        if len(liSSN) < count:
                createSSNs(liSSN, count)
        return liSSN

def gen_cust(liSSNMaster, acct_list, i):
        fake = Faker()
        #Initiate High Risk Flags
        #Politically Exposed Person
        PEP='No'
        #Customer with a Suspicous Activity Report
        SAR='No'
        #Customer with a closed account
        #generate closed acct flag
        Clsd=choice(Clsd_flag)
        #High risk customer flag
        high_risk='No'
        #High Risk Rating
        hr_rating=''
        #Customer that was demarketed by the bank
        demarket='No'
        dem_date=''
        #Random choice for number of credit cards per account number
        No_CCs = choice(Number_CC)
        acct=start+1+randrange(1,10,1)
        start=acct
        #Randomly generates customer name
        name = fake.name()
        tmp=gen_data.create_name()
        #Adds account number to account dictionary
        acct_list.extend([acct])
        #Creates a new row and adds data elements
##      JS - Main Account Holder SSN as current index in master SSN list
        row = [i]+[acct]+[choice(Acct_Type)]+[No_CCs]+[name]+[tmp[0]]+[liSSNMaster[i]]
        #Dictionary for names list set to blank
        names=[]
        #Dictionary for Social Security Number list set to blank
        ssn=[]
        #Generates Name and SSN for Credit Users
        #Middle Name to reduce name dups
        mdl=[]
        for j in range(No_CCs-1):
                names.insert(j,fake.name())
                tmp2=gen_data.create_name()
                mdl.insert(j,tmp2[0])
##      JS - Pull from SSN Master list
                randInt = randrange(1,len(liSSNMaster),1)
                if randInt != i:
                        ssn.insert(j,liSSNMaster[randInt])
                else:
                        ssn.insert(j,liSSNMaster[randInt - 1])

        #Name and SSN is set to blank if less than 4 customers on an account

        for k in range(4-No_CCs):
                names.insert(No_CCs+k,'')
                ssn.insert(No_CCs+k,'')
                mdl.insert(No_CCs,'')
        #Sets CC_NO to a random credit card number
        CC_NO=gen_data.create_cc_number()
        CC_TRANS=CC_NO[1][0]
        dt = str(datetime.now())
        clean=re.sub('\W','',dt)
        printCC=str(CC_TRANS[-4:])+str(clean[-12:-3])+str(randrange(1111,9999,randrange(1,10,1)))
        #Add data elements to current csv row
        row.extend([names[0],mdl[0],ssn[0],names[1],mdl[1],ssn[1],names[2],mdl[2],ssn[2],printCC,CC_NO[0],gen_data.create_company_name()+' '+tmp[1],\
        gen_data.create_email(),gen_data.create_job_title()])

        #Creates Current Address
        zip=choice(zips.zip)
        addr=geo_data.create_city_state_zip[zip]
        #Creates Previous address
        zip2=choice(zips.zip)
        addr2=geo_data.create_city_state_zip[zip2]

        #Add additional data elements to current csv row
        lrg_cash_ex=choice(Yes_No)

        #Condition for SARs and Demarketed Clients
        if(Clsd=='Yes'):
                #1% of closed accounts are demarketed but never had a SAR filed
                if (max((randrange(0,101,1)-99),0)==1 and SAR=='No'):
                        demarket='Yes'
                        dem_date=gen_data.create_date(past=True)
                if (max((randrange(0,11,1)-9),0)==1 and demarket=='No'):
                        #10% of closed accounts have SARs
                        SAR='Yes'
                        #90% of closed accounts with SARs are demarketed
                        if(max((randrange(0,11,1)-9),0)==0):
                                demarket='Yes'
                                dem_date=gen_data.create_date(past=True)
        #1% of accounts are PEP
        if (max((randrange(0,101,1)-99),0)==1):
                PEP='Yes'
        
        row.extend([addr[0],addr[1],zip,'US',addr2[0],addr2[1],zip2,'US',gen_data.create_birthday(min_age=2, max_age=85),PEP,SAR,Clsd])
        #Start Generating related accounts from account list once 10,000 accounts are generated
        if i > 10000:
                rel = int(choice(acct_list))*max((randrange(0,10001,1)-9999),0)
                if rel <> 0:
                        row.append(rel)
                        row.append(choice(Related_Type))
                else:
                        row.append('')
                        row.append('')
        else:
                row.append('')
                row.append('')
        
        #Randomly generates account start date
        party_start=gen_data.create_date(past=True)
        #Randomly selects consent option for sharing info
        Consent_Share = choice(Yes_No_Consent)
        
        #Add additional data elements to current csv row
        row.extend([choice(Party_Type),choice(Party_Relation),party_start,gen_data.create_date(past=True),\
        lrg_cash_ex,demarket,dem_date,randrange(0,100,1),choice(Official_Lang)])
        #Add data element preferred methond of contact for yes to share info...if not then blank to current row
        if Consent_Share == 'Yes':
                row.extend(['Yes',choice(Preffered_Channel)])
        else:
                row.extend(['No',''])
        #DO NOT USE CUST STATUS BELOW - NOT INTEGRATED WITH CLOSED STATUS! Add additional data elements to current csv row
        row.extend([zip,randrange(0,5,1)])

        #Generates Segment ID then adds additional Segment data based on the selection to the current csv row
        Segment_ID = randrange(0,5,1)
        
        if Segment_ID == 0:
                row.extend(['01','LOB Specific','IRRI','Group 1','High Risk Tier','200'])
        if Segment_ID == 1:
                row.extend(['02','Profitability','CRS Risk Score','Group 1','Mid Risk Tier','300'])
        if Segment_ID == 2:
                row.extend(['03','Geographical','Geo Risk','Group 2','Low Risk Tier','400'])
        if Segment_ID == 3:
                row.extend(['04','Behavioral','Financial Behavior Risk','Group 3','Vertical Risk','100'])
        if Segment_ID == 4:
                row.extend(['05','Risk Tolerance','CM Risk','Group 4','Geographical Risk','500'])
        
        #Arms Manufacturer random choice
        hr0=choice(Yes_No_Cust_Flag)
        #Auction random choice
        hr01=choice(Yes_No_Cust_Flag)
        #Cash Intensive Business random choice
        hr02=choice(Yes_No_Cust_Flag)
        #Casino Gambling random choice
        hr03=choice(Yes_No_Cust_Flag)
        #Channel Onboarding random choice
        hr04=choice(Channel_Onboarding)
        #Channel Ongoing Transactions random choice
        hr05=choice(Channel_Ongoing_Transactions)
        #Add additional data elements to current csv row
        row.extend([hr0,hr01,hr02,hr03,hr04,hr05])
        
        #Randomly select whther customer has a High Net Worth
        HighNetWorthFlag = choice(HighNetWorth)
        #Randomly Generates customer net worth based on the above flag
        if HighNetWorthFlag == 'Yes':
                row.append(max(max((randrange(0,101,1)-99),0)*randrange(5000000,25000000,1),randrange(1000000,5000000,1)))
        else:
                flag=choice(LowNet)
                if flag==0:
                        row.append(randrange(-250000,600000,1))
                else:
                        if flag==1:
                                row.append(randrange(149000,151000,1))
                        else:
                                row.append(randrange(40000,50000,1))
        #Add data elements to current csv row
        #Complex_HI_Vehicle random choice
        hr1=choice(Yes_No_Cust_Flag)
        #Dealer_Precious_Metal random choice
        hr2=choice(Yes_No_Cust_Flag)
        #Digital_PM_Operator random choice
        hr3=choice(Yes_No_Cust_Flag)
        #Embassy_Consulate random choice
        hr4=choice(Yes_No_Cust_Flag)
        #Exchange_Currency random choice
        hr5=choice(Yes_No_Cust_Flag)
        #Foreign_Financial_Institution random choice
        hr6=choice(Yes_No_Cust_Flag)
        #Foreign_Government random choice
        hr7=choice(Yes_No_Cust_Flag)
        #Foreign_NonBank_Financial_Institution random choice
        hr8=choice(Yes_No_Cust_Flag)
        #Internet_Gambling random choice
        hr9=choice(Yes_No_Cust_Flag)
        #Medical_Marijuana_Dispensary random choice
        hr10=choice(Yes_No_Cust_Flag)
        #Money_Service_Business random choice
        hr11=choice(Yes_No_Cust_Flag)
        hr12=choice(NAICS.NAICS_Code)
        #NonRegulated_Financial_Institution random choice
        hr13=choice(Yes_No_Cust_Flag)
        #Not_Profit random choice
        hr14=choice(Yes_No_Cust_Flag)
        #Occupation random choice
        #hr15=choice(Occupation)
        #Privately_ATM_Operator random choice
        hr16=choice(Yes_No_Cust_Flag)
        #Products random choice
        hr17=choice(Products)
        #Sales_Used_Vehicles random choice
        hr18=choice(Yes_No_Cust_Flag)
        #Services random choice
        hr19=choice(Services)
        #SIC_Code random choice
        hr20=choice(SIC_Code)
        #Stock_Market_Listing random choice
        hr21=choice(Stock_Market_Listing)
        #Third_Party_Payment_Processor random choice
        hr22=choice(Yes_No_Cust_Flag)
        #Transacting_Provider random choice
        hr23=choice(Yes_No_Cust_Flag)
        
        refrating=['1']*3 + ['2','4']*2 + ['3'] + ['5']*12
        if(PEP=='Yes' or SAR=='Yes' or lrg_cash_ex=='Yes' or demarket=='Yes' or hr0=='Yes'
        or hr01=='Yes' or hr02=='Yes' or hr03=='Yes' or hr1=='Yes' or hr2=='Yes' or hr3=='Yes' or hr4=='Yes' or
        hr5=='Yes' or hr6=='Yes' or hr7=='Yes' or hr8=='Yes' or hr9=='Yes' or hr10=='Yes' or hr11=='Yes' or hr13=='Yes' or hr14=='Yes' or
        hr16=='Yes' or hr17=='Yes' or hr18=='Yes' or hr22=='Yes' or hr23=='Yes' or HighNetWorthFlag=='Yes'):
                high_risk='Yes'
                hr_rating=choice(refrating)
        
        if(high_risk=='No'):
                if(max((randrange(0,101,1)-99),0)==1):
                        high_risk='Yes'
                        hr_rating=choice(refrating)

        row.extend([hr1,hr2,hr3,hr4,hr5,hr6,hr7,hr8,hr9,hr10,hr11,hr12,hr13,hr14,hr16,hr17,hr18,hr19,hr20,hr21,hr22,hr23,
        HighNetWorthFlag,high_risk,hr_rating,choice(Use_Case)])
        #End the current row
        return row

def createCustData(liSSNMaster, ccount, remainder, lenI, iterator):
        acct_list=[]
        liCust=[]
        if iterator == 0:
            x = xrange(ccount)
        else:
            if iterator < (lenI-1):
                x = xrange(iterator * ccount, (iterator+1) * ccount)
            else:
                x = xrange(iterator * ccount, ((iterator * ccount) + (ccount+remainder)))
        for i in x:
        #Loop for number of accounts to generate
                liCust.append(gen_cust(liSSNMaster, acct_list, i))
        return liCust

def main():
        #####Customer Count wanted for the end file######
        cust_count=50000
        liSSNMaster=[]
        liSSNMaster=createSSNs(liSSNMaster, cust_count)
        pyTimer.endTimer(startTime,'Creating ' + str(len(liSSNMaster)) + ' SSNs for customers')
        chk1Time=pyTimer.startTimer()
        cust_list=[]
        proc = 16
        iterator = xrange(proc)
        remainder = cust_count % proc
        ccount = cust_count/proc
        lenI = len(iterator)
        func = partial(createCustData, liSSNMaster, ccount, remainder, lenI)
        pool = Pool(processes=proc)
        results = pool.map(func, iterator)
        cust_list = results[0] + results[1] + results[2] + results[3] + results[4] + results[5] + results[6] + results[7] + results[8] + results[9] + results[10] + results[11] + results[12] + results[13] + results[14] + results[15]
        endLoopTime = pyTimer.startTimer()
        avgLoopTime = round(((endLoopTime - chk1Time)/cust_count), 6)
        avgLoopTime = ("{0:.6f}".format(avgLoopTime))
        pyTimer.writeRuntimeLog('The average time to create a customer is: ' + str(avgLoopTime) + ' seconds\n')
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
##        cust_list=createCustData(cust_count)
##        lines=sc.parallelize(cust_list)
##        lines.saveAsTextFile("Customers")
        #Creates CSV
        with open('uber_custv3.csv','w') as f1:
                #Writer for CSV...Pipe delimited...Return for a new line
                writer=csv.writer(f1, delimiter='|',lineterminator='\n',)
                #Header Row
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
                for row in cust_list:
                        writer.writerow(row)
        pyTimer.endTimer(startTime,str(cust_count)+' Customer creation')

if __name__ == "__main__":
    main()
