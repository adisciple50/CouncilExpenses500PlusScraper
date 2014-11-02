__author__ = 'Jason Crockett'

import simplejson as json
import pymysql
import urllib3


class ExpensesTable:
    TableHeaders = {'BodyName':'TEXT(80)','TransactionNumber':'INT(8)','AccountCodeDescription':'VARCHAR(3)','ExpensesType':'TEXT(50)','ServiceCode':'VARCHAR(3)','ServiceAreaCategorisation':'TEXT(100)','SupplierName':'TEXT(100)','Date':'DATE','Amount':'DECIMAL(65,2)'}

    def dbCursor(self):
        db = pymysql.connect(host='178.62.105.36',user='auditor',passwd='chocolate',db='CouncilExpenses500Plus')
        cursor = db.cursor()
        return cursor

    def dateFormatter(self,JsonDateStamp):
        JsonDateStamp = str(JsonDateStamp)
        JsonDateStamp.replace('/','-')
        JsonDateStamp = JsonDateStamp[:10]
        #JsonDateStamp = JsonDateStamp[::-1] # json returns correct Dates!
        return JsonDateStamp

    def createExpensesTable(self):
        DbCurse = self.dbCursor()
        HeaderString = ""
        for HeaderName,DataType in self.TableHeaders.items():
            HeaderString += HeaderName + " " + DataType + ", "
        CreateQuery = "CREATE TABLE Expenses (" + HeaderString + ")"
        CreateQuery = CreateQuery[:-3] + ")"
        print(CreateQuery)
        DbCurse.execute(CreateQuery)
        return True

    def getJson(self):
        conn = urllib3.connection_from_url('http://data.bathhacked.org')
        response = conn.request('GET','/resource/cimd-yfzu.json?$limit=10000')
        print('\n Dataset Fetched')
        return response.data.decode('utf-8')

    def prepareDataToListOfDicts(self):
        JsonDict = json.loads(self.getJson())
        return JsonDict

    def makeValueTuple(self,JsonRow):
        RowTup = (list(dict(JsonRow).values()))
        return RowTup

    def insertDataIntoTable(self):
        db = pymysql.connect(host='178.62.105.36',user='auditor',passwd='chocolate',db='CouncilExpenses500Plus')
        dbcurse = db.cursor()
        #Headers = str(list(TableHeaders.keys()))[1:-1] # gives the wrong order! :(
        Headers = "BodyName ,TransactionNumber ,AccountCodeDescription, ExpensesType, ServiceCode, ServiceAreaCategorisation, SupplierName, Date, Amount"
        cols = ""
        for i in range(0,len(list(self.TableHeaders.keys()))):
            cols += '%s, '
        InsertStatement = "INSERT INTO Expenses (" + str(Headers) + ")VALUES (" + cols[:-2] + ")"
        DataList = []
        for JsonRow in self.prepareDataToListOfDicts():
            print(JsonRow)
            Row = (JsonRow["body_name"] if "body_name" in JsonRow else 'Empty',JsonRow["transaction_number"] if "transaction_number" in JsonRow else 'Empty',JsonRow["account_code_description"] if "account_code_description" in JsonRow else 'Empty',JsonRow["expenses_type"] if "expenses_type" in JsonRow else 'Empty',JsonRow["service_code"] if "service_code" in JsonRow else 'Empty',JsonRow["service_area_categorisation"] if "service_area_categorisation" in JsonRow else 'Empty',JsonRow["supplier_name"] if "supplier_name" in JsonRow else 'Empty',self.dateFormatter(JsonRow["date"]) if "date" in JsonRow else 'Empty',JsonRow["amount"] if "amount" in JsonRow else 'Empty')
            DataList.append(Row)
        print(InsertStatement)
        print(DataList)
        print('\n total items stored:')
        print(str(len(DataList)))
        dbcurse.executemany(InsertStatement, DataList) # REQUIRES A COMMIT STATEMENT FROM THE CONNECTION CLASS!!! - sanity restored.
        db.commit()
        print('done')
        if dbcurse:
            dbcurse.close()
        else:
            print("dbcurse is missing in insertDataIntoTable()")
        return True

    def emptyTable(self):
        db = pymysql.connect(host='178.62.105.36',user='auditor',passwd='chocolate',db='CouncilExpenses500Plus')
        dbcurse = db.cursor()
        dbcurse.execute('TRUNCATE TABLE Expenses')
        dbcurse.close()
        return True

    def refreshTable(self):
        self.emptyTable()
        self.insertDataIntoTable()

ExpTable1 = ExpensesTable()

ExpTable1.refreshTable()