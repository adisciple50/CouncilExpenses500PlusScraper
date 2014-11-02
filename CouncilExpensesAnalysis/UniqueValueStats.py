__author__ = 'Jason Crockett'
import pymysql
import CouncilExpensesAnalysis.MathsFunctions as AMaths
import operator

def ListAllContractors():
    db = pymysql.connect(host='178.62.105.36',user='auditor',passwd='chocolate',db='CouncilExpenses500Plus')
    cursor = db.cursor()

    cursor.execute('SELECT DISTINCT SupplierName FROM Expenses')
    ContractorsList = cursor.fetchall()

    print("Unique Contractors Found:")
    print(len(ContractorsList))
    print('They Are Are As Follows:')
    #print(ContractorsList)
    ContractorsPythonList = []
    for Contractor in ContractorsList:
        #print(Contractor)
        ContractorsPythonList.append("".join(Contractor))
    print(ContractorsPythonList)
    cursor.close()
    return ContractorsPythonList

def GetTotalSpentOnContractor(ContractorName):
    conv=pymysql.converters.conversions.copy()
    conv[246]=float
    db = pymysql.connect(host='178.62.105.36',user='auditor',passwd='chocolate',db='CouncilExpenses500Plus',conv=conv)
    cursor = db.cursor()
    Query = "SELECT Amount FROM Expenses WHERE SupplierName ='" + str(ContractorName) +"';"
    print('QueryToExecute: %s' % (Query,))
    cursor.execute(Query)
    TransactionsList = cursor.fetchall()
    tuple(TransactionsList)
    print(TransactionsList)

    TransactionsListPython = []
    for Transaction in TransactionsList:
        #print(Contractor)
        TransactionsListPython.append(Transaction[0])

    print(TransactionsListPython)

    Total = sum(TransactionsListPython)
    cursor.close()
    print('Total Spent On  %s = £%s'%(ContractorName,Total))
    return {ContractorName:Total}

def getAllContractorTotals():
    AllContractors = {}
    for Contractor in ListAllContractors():
        AllContractors.update(GetTotalSpentOnContractor(Contractor))
    return AllContractors

def GetBiggestBenifactors(ListLength):
    Contractors = getAllContractorTotals()
    SortKeys = Contractors.values()
    #Reverse = list(SortKeys).sort()[::-1]
    SortedContractors = sorted(Contractors.items(),SortKeys,True)
    for i in range(ListLength):
        toPrint = str(i) + ". " + SortedContractors[i][1] ""
        print('%s. %s:Tota')



#ListAllContractors()
#GetTotalSpentOnContractor('SIRONA CARE & HEALTH C.I.C.')

print(getAllContractorTotals())