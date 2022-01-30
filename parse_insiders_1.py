# -*- coding: utf-8 -*-

import xml.etree.ElementTree as ET
import re, time, json, urllib.request, ast, httplib2, apiclient.discovery
from urllib.parse import unquote
from pandas import json_normalize
from datetime import date, timedelta
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials



# In order to parse xml with no errors, Create a new class which overrides the user-agent with Mozilla.
class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"


class InsidersDeals():
    def __init__(self):
        print('starting insiders parsing')
        # Получение списка акций  из готового листа Google Sheet для проверки (не брать то, что все равно не сможем оценить и купить!
        self.CREDENTIALS_FILE = 'stock-spreadsheets-9974a749b7e4.json'
        tickers_page = '1s6uIbhIX4IYCmFYhfWgEklFqtLX95ky7GmJNRvVexeM'
        self.ranking_page = '1C_uAagRb_GV7tu8X1fbJIM9SRtH3bAcc-n61SP8muXg'
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
        #credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
        httpAuth = credentials.authorize(httplib2.Http())  # Авторизуемся в системе
        self.service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)  # Выбираем работу с таблицами и 4 версию API

        # reading data
        results = self.service.spreadsheets().values().batchGet(spreadsheetId=tickers_page, ranges='A:R', valueRenderOption='FORMATTED_VALUE', dateTimeRenderOption='FORMATTED_STRING').execute()
        sheet_values = results['valueRanges'][0]['values']
        values = sheet_values[1:]  # текущий рабочий список (весь!)

        self.yf_working_tickers_list = []  # текущий рабочий список для работы с yfinance без излишек (если будет нужен, в скрипте не используется!)
        for i in values:
            if i[-2] == 'yfinance':
                self.yf_working_tickers_list.append(i[1])


        start = date.today()
        end = date.today() - timedelta(200)
        TOKEN = '8542ae672b4f1f3d12bf3bf51084a899955e9204cd05ea3888ef2be21b53e5ff'
        API = "https://api.sec-api.io?token=" + TOKEN
        filter = f"formType:\"4\" AND formType:(NOT \"N-4\") AND formType:(NOT \"4/A\") AND filedAt:[{str(end)} TO {str(start)}]"
        sort = [{"filedAt": {"order": "desc"}}]

        payload = {
            "query": {"query_string": {"query": filter}},
            "from": 0,
            "size": 20000,
            "sort": sort
        }

        # Format the payload to JSON bytes
        jsondata = json.dumps(payload)
        jsondataasbytes = jsondata.encode('utf-8')  # needs to be bytes

        # Instantiate the request
        req = urllib.request.Request(API)

        # Set the correct HTTP header: Content-Type = application/json
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        # Set the correct length of your request
        req.add_header('Content-Length', len(jsondataasbytes))

        # Send the request to the API
        response = urllib.request.urlopen(req, jsondataasbytes)

        # Read the response
        res_body = response.read()
        # Transform the response into JSON
        self.filingsJson = json.loads(res_body.decode("utf-8"))


    # this is for testing ONLY!!!
    def compress_filings(self, filings):
        print('in compress_filings')
        store = {}
        compressed_filings = []
        for filing in filings:
            filedAt = filing['filedAt']
            if filedAt in store: # and store[filedAt] < 5:  #  check if this is bigger - what happens!
                compressed_filings.append(filing)
                store[filedAt] += 1
            elif filedAt not in store:
                compressed_filings.append(filing)
                store[filedAt] = 1
        return compressed_filings


    # Download the XML version of the filing. If it fails wait for 5, 10, 15, ... seconds and try again.
    def download_xml(self, url):
        print(' in download_xml')
        opener = AppURLopener()
        print(f'opener is okay: {opener}\n')
        response = opener.open(unquote(url))
        print(f'response is okay: {response}\n\n')
        
        # decode the response into a string
        data = response.read().decode('utf-8', errors="replace")
        print(f'data is okay: {data}\n\n')
        # set up the regular expression extractoer in order to get the relevant part of the filing
        matcher = re.compile('<\?xml.*ownershipDocument>', flags=re.MULTILINE|re.DOTALL)
        matches = matcher.search(data)
        print(f'matcher is okay: {matcher}\n\n')
        # the first matching group is the extracted XML of interest
        xml = matches.group(0)
        print(f' xml is okay: {xml}\n\n')
        # instantiate the XML object
        root = ET.fromstring(xml)
        print(f'root is okay: {root}\n\n')
        
        return root


    def Get_Spreadsheet_Data(self):
        print('in Get_Spreadsheet_Data')
        results_rank = self.service.spreadsheets().values().batchGet(spreadsheetId=self.ranking_page, ranges='A:AE', valueRenderOption='FORMATTED_VALUE', dateTimeRenderOption='FORMATTED_STRING').execute()
        #results_rank = service.spreadsheets().values().batchGet(spreadsheetId=ranking_page, ranges='A:AE', valueRenderOption='FORMATTED_VALUE', dateTimeRenderOption='FORMATTED_STRING').execute()
        rank_sheet_values = results_rank['valueRanges'][0]['values']

        return rank_sheet_values


    # Calculate the total transaction amount in $ of a giving form 4 in XML
    def calculate_transaction_amount(self, xml):
        print('in calculate_transaction_amount')
        total = 0
        if xml is None:
            return total
        nonDerivativeTransactions = xml.findall("./nonDerivativeTable/nonDerivativeTransaction")
        for t in nonDerivativeTransactions:
            # D for disposed or A for acquired
            action = t.find('./transactionAmounts/transactionAcquiredDisposedCode/value').text
            # number of shares disposed/acquired
            shares = t.find('./transactionAmounts/transactionShares/value').text
            # price
            priceRaw = t.find('./transactionAmounts/transactionPricePerShare/value')
            price = 0 if priceRaw is None else priceRaw.text
            # set prefix to -1 if derivatives were disposed. set prefix to 1 if derivates were acquired.
            prefix = -1 if action == 'D' else 1
            # calculate transaction amount in $
            amount = prefix * float(shares) * float(price)
            total += amount
        return round(total, 2)


    # Take some other data form 4 in XML
    def find_owner(self, xml):
        print('in find_owner')
        report_owner = 'N/A'
        if xml is None:
            return report_owner
        owner_field = xml.findall("./reportingOwner/reportingOwnerId")#/rptOwnerName")
        for o in owner_field:
            report_owner = o.find('./rptOwnerName').text

        return report_owner


    # Download the XML for each filing
    # Calculate the total transaction amount per filing
    # Save the calculate transaction value to the filing dict with key 'nonDerivativeTransactions'
    def add_non_derivative_transaction_amounts(self):
        print(' in add_non_derivative_transaction_amounts')
        filings = self.compress_filings(self.filingsJson['filings'])
        print(f'filings - in add_non_derivative_transaction_amounts: {type(filings)}, {filings[0]}\n\n\n {filings[-1]}, {len(filings)} \n\n')
        for filing in filings:
            try:
                url = filing['linkToTxt']
                print(f'url: {url}')
                xml = self.download_xml(url)
                print(f'xml: {xml}')
                nonDerivativeTransactions = self.calculate_transaction_amount(xml)
                filing['nonDerivativeTransactions'] = nonDerivativeTransactions
                filing['rep_owner'] = self.find_owner(xml)
                print(f'Done for: {filings.index(filing)} out of {len(filings)}\n\n')
                print(f'upadted filing - in add_non_derivative_transaction_amounts: {type(filing)}, {filing[0]}, {filing[-1]}, {len(filing)} \n\n')
            except:
                pass
        return filings


    def ConvertBeforeSaving(self):
        print('in ConvertBeforeSaving')
        # Running the function prints the URL of each filing fetched
        returned_filings = self.add_non_derivative_transaction_amounts()
        
        filings_final = json_normalize(returned_filings) #making dataframe, clear and understood
        # headers = filings_final.columns.values.tolist()  # ['id', 'accessionNo', 'cik', 'ticker', 'companyName', 'formType', 'description', 'filedAt', 'linkToTxt', 'linkToHtml', 'linkToXbrl', 'linkToFilingDetails', 'entities', 'documentFormatFiles', 'dataFiles', 'seriesAndClassesContracts', 'Information', 'periodOfReport', 'effectivenessDate', 'nonDerivativeTransactions', 'owner']
        values = filings_final.values.tolist()

        checklist = [None, 0, '']
        final_list = []
        for i in values:
            declare_date = i[8][:10]
            period_of_report = i[-4]
            ticker = i[3]
            company_name = i[4]
            amount = i[-2]
            module_amount = amount*-1 if amount <0 else amount
            owner = i[-1]
            link_to_txt = i[9]
            if ticker not in checklist and amount not in checklist and ticker in self.yf_working_tickers_list:
                final_list.append([declare_date, period_of_report, ticker, company_name, amount, link_to_txt, owner, module_amount])
            else:
                pass

        return final_list


    def Sheet_filling(self, dataframe):
        print('in Sheet_filling')

        # working with the insiders deals page - first, reading the current data to clear them up
        insiders_deals_page = '12Ns23Wih3YMKH6hACyjPB5TV46dLcAs8LvbmYUeC3Ks'
        report_page_data = self.service.spreadsheets().values().get(spreadsheetId=insiders_deals_page, range='Update!A:I',
                                                                    valueRenderOption='FORMATTED_VALUE',
                                                                    dateTimeRenderOption='FORMATTED_STRING').execute()

        # report_page_df = pd.DataFrame(report_page_data.get("values")[1:], columns=report_page_data.get("values")[0])  # in case if the df type is needed in future
        sheet_data = report_page_data.get("values")
        headers = report_page_data.get("values")[0]

        # clear_data
        clear_up_range = []  # выбираем заполненные значения, определяем нулевую матрицу для обнуления страницы
        for _ in sheet_data:  # число строк с текущим заполнением
            clear_up_range.append([str('')] * len(headers))
        
        print(f'clear_up_range - in sub: {type(clear_up_range)}, {clear_up_range[0]}, {clear_up_range[-1]}, {len(clear_up_range)} \n\n')
        
        null_matrix = self.service.spreadsheets().values().batchUpdate(spreadsheetId=insiders_deals_page, body={
            "valueInputOption": "USER_ENTERED",
            "data": [{"range": "Update",
                      "majorDimension": "ROWS",
                      "values": clear_up_range}]
        }).execute()

        # making appropriate range from the new dataframe
        new_data = dataframe.values.tolist()
        new_d = [dataframe.columns.values.tolist()]
        for i in new_data:
            new_d.append(i)
        
        print(f'new_d - in sub: {type(new_d)}, {new_d[0]}, {new_d[-1]}, {len(new_d)} \n\n')
        
        # заполнение новыми данными
        results = self.service.spreadsheets().values().batchUpdate(spreadsheetId=insiders_deals_page, body={
            "valueInputOption": "USER_ENTERED",
            "data": [{"range": "Update",
                      "majorDimension": "ROWS",
                      "values": new_d}]
        }).execute()


    def PerformAll(self):
        print(' in PerformAll')
        list_headers = ['declare_date', 'period_of_report', 'ticker', 'company_name', 'amount', 'linkToTxt', 'report_owner', 'module_amount', 'Share_of_Total_Cap']
        final_list = self.ConvertBeforeSaving()
        print(f'final_list: {type(final_list)}, {final_list[0]}, {final_list[-1]}, {len(final_list)} \n\n')

        check_comp_values = self.Get_Spreadsheet_Data()
        tickers_capitals_dict = {}  # словарь тикер - капитал компании
        for i in check_comp_values:
            tickers_capitals_dict[i[1]] = i[5]

        for c in final_list: # adding the capitals values to each row
            cap = int(tickers_capitals_dict.get(c[2]).split(',')[0]) *1000000
            share = f'{round(int(c[4])/cap *100 ,3)}%'
            c.append(share)

        val_df_2 = pd.DataFrame(final_list, columns=list_headers).sort_values(by=['module_amount'], ascending=False).drop(columns=['module_amount'])
        print(f'val_df_2 in main part: {val_df_2} \n\n')

        self.Sheet_filling(val_df_2)
        print(f'We\'re all set!')
