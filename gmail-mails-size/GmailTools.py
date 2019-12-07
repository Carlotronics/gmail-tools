#!/usr/bin/env python3

from __future__ import print_function
import pickle, json, re, base64, sys, sqlite3, random, string
import os
from googleapiclient.discovery import build
from apiclient import errors
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from datetime import datetime, timedelta
from pathlib import Path

import threading#, httplib2

import xlsxwriter
from zipfile import ZipFile
import shutil
from time import sleep
import time

datetime_format = "%Y-%m-%d_%H-%M-%S"
datetime_format_forExcel = "%Y-%m-%d %H:%M:%S"
def genDT():
    return datetime.now().strftime(datetime_format)
base_temp_files_dir = "./temp-files"
base_attachments_dir = "./attachments"
finalDataDB = "./FinalData.db"
attachmentsDownload__threads = {}
attachmentsDownload__threads__v2 = []

dev = True

class GenericError(Exception):
    pass

# TODO - Add precise documentation for functions. Done up to _cleanFinalDB (ex).

# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
# or
# pip3 install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

# If modifying these scopes, delete the file token.pickle.
# SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
SCOPES = ['https://mail.google.com/']
# SCOPES = ['https://www.googleapis.com/auth/gmail']

class _json:
    def loads(d):
        return json.loads(d)
    def dumps(d):
        return base64.b64encode(json.dumps(d).encode("utf-8")).decode("utf-8")
        #return json.dumps(d).replace("'", "\\'")
    def b64decode(data, altchars=b'+/'):
        """Decode base64, padding being optional.

        :param data: Base64 data as an ASCII byte string
        :returns: The decoded byte string.

        """
        if type(data) == str:
            data = data.encode("utf-8")
        data = re.sub(rb'[^a-zA-Z0-9%s]+' % altchars, b'', data)  # normalize
        missing_padding = len(data) % 4
        if missing_padding:
            data += b'='* (4 - missing_padding)
        return _json.loads(base64.b64decode(data, altchars).decode("utf-8"))
class Tools:
    def FormatTime(time):
        """Format duration in seconds

        Args:
            time: Time, in seconds

        Returns:
            4-uplet: (days, hours, minutes, seconds)
        """
        m, s = divmod(time, 60)
        h, m = divmod(m, 60)
        d, h = divmod(h, 24)
        return (d, h, m, s)
    
    def HumanReadableTime(time):
        """Returns formatted time, ready to display (uses Tools.FormatTime to format accurately)

        Args:
            time: Time, in seconds

        Returns:
            string: [x day[s] ][x hour[s] ][x minute[s] ][x second[s]]
        """
        d, h, m, s = Tools.FormatTime(time)
        l = []
        if d:
            if d == 1: l.append(str(d) + " day")
            else: l.append(str(d) + " days")
        if h:
            if h == 1: l.append(str(h) + " hour")
            else: l.append(str(h) + " hours")
        if m:
            if m == 1: l.append(str(m) + " minute")
            else: l.append(str(m) + " minutes")
        if s:
            if s == 1: l.append(str(s) + " second")
            else: l.append(str(s) + " seconds")
        return " ".join(l)
    
    def RandomString(stringLength=10):
        """Generate a random string of fixed length """
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(stringLength))
    
    def parseEmailRecipients(recipientString):
        """Parses email's recipients string

        Args:
            recipientString: Email's recipients string

        Returns:
            array: List of {name, email} dicts
        """
        regex = re.compile(
            '(([\w\-\',\"\s]+)\s)?<?([^@<\s]+@[^@\s>]+)>?,?',
            re.IGNORECASE)
        recipientsArray = []

        l = re.split(regex, recipientString)
        for i in range(0, len(l)-1, 4):
            namePos = i+1
            mailPos = i+3
            if l[namePos]:
                name = l[namePos].strip()
            else:
                name = ""
            email = l[mailPos].strip()

            if name and (name[0] == '"' or name[0] == "'"):
                name = name[1:]
            if name and (name[-1] == '"' or name[-1] == "'"):
                name = name[:-1]
            if (email[0] == '"' or email[0] == "'"):
                email = email[1:]
            if (email[-1] == '"' or email[-1] == "'"):
                email = email[:-1]

            recipientsArray.append({
                "name": name,
                "email": email
            })
        return recipientsArray

    def confirm(str, default=True):
        """Used to prompt user for confirmation

        Args:
            str: String to be displayed on screen: choice to be confirmed
            default: Default choice (True / False), that will be returned if
                user simply presses Return key

        Returns:
            True / False
        """
        s = "Y/n" if default else "y/N"
        ipt = input(f"{str} [{s}] ").strip().lower()
        if ipt == "":
            return default
        return ipt == "y" or ipt == "o"

    def CreateTable_labels(dbCursor, userEmail):
        """Creates table for `labels`

        Args:
            dbCursor: sqlite3 cursor
            userEmail: Email for which table should be created
        """
        req = f"CREATE TABLE IF NOT EXISTS `{userEmail}_labels` (`id` TEXT,`name` TEXT,`type` VARCHAR(255),`mails_count` INT(11));"
        dbCursor.execute(req)

    def CreateTable_attachments(dbCursor, userEmail):
        """Creates table for `attachments`

        Args:
            dbCursor: sqlite3 cursor
            userEmail: Email for which table should be created
        """
        req = f"CREATE TABLE IF NOT EXISTS `{userEmail}_attachments` (`id` TEXT,`generatedId` VARCHAR(255),`originFilename` TEXT, `messageId` VARCHAR(255));"
        dbCursor.execute(req)

    def CreateTable_messages(dbCursor, userEmail):
        """Creates table for `messages`

        Args:
            dbCursor: sqlite3 cursor
            userEmail: Email for which table should be created
        """
        req = f"CREATE TABLE IF NOT EXISTS `{userEmail}_messages` (\
            `id` VARCHAR(255),\
            `threadId` VARCHAR(255),\
            `exp_name` TEXT(255),\
            `exp_email` TEXT,\
            `dest_names` TEXT,\
            `dest_emails` TEXT,\
            `dest_json` TEXT,\
            `labelIds` TEXT,\
            `labelNames` TEXT,\
            `to_del` INT(1) DEFAULT(0),\
            `date_ts` INT(11),\
            `date` VARCHAR(255),\
            `subject` TEXT,\
            `body` TEXT,\
            `size` INT(11),\
            `attachmentsIds` TEXT\
        );"
        dbCursor.execute(req)

    def genList(n, x=5000):
        """
        n : Total quantity
        x : Parts size
        """
        if n < x:
            return [0]
        l = []
        last = 0
        d_nx = n // x
        for i in range(d_nx):
            l.append(last)
            last += x
        if l[-1]+x < n:
            l.append(l[-1]+x)
        return l

    def FormatFilename(s):
        """Take a string and return a valid filename constructed from the string
        Uses a whitelist approach: any characters not present in valid_chars are
        removed. Also spaces are replaced with underscores.
         
        Note: this method may produce invalid filenames such as ``, `.` or `..`
        """
        valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
        filename = ''.join(c for c in s if c in valid_chars)
        filename = filename.replace(' ','_') # I don't like spaces in filenames.
        return filename

    def SnakeToCamel(s):
        """Returns the CamelCase version of a snake_case string
        """
        import re
        return ''.join(x.capitalize() or '_' for x in s.split('-'))

class GmailUtils:
    _port = None
    _creds = None
    _userId = None
    _userEmail = None
    _tokenFile = None
    _speedtests = None
    _gmailService = None
    _googleAPICredentials = None
    _userEmail_cleanForFiles = None
    _googleConsoleAuth = None
    _userInfo = None

    def __init__(
        self,
        port=0,
        tokenFile="token.pickle",
        googleAPICredentials=None,
        googleAPICredentialsFile="credentials.json",
        userId="me",
        speedtests=[],
        googleConsoleAuth=False
    ):
        """Inits object

        Args:
            port: Port on which script will listen for Google OAuth
            tokenFile: File in which Google OAuth token will be saved
            googleAPICredentials: Google JSON data from Google Developers Console, used to authenticate project
            googleAPICredentialsFile: File containing Google JSON data from Google Developers Console, used to authenticate project
            userId: Google userId (default: me)

        Returns:
            None (constructor !!)
        """
        self._port = port
        self._tokenFile = tokenFile
        self._userId = userId
        self._speedtests = speedtests
        self._googleConsoleAuth = googleConsoleAuth

        if googleAPICredentials:
            try:
                self._googleAPICredentials = json.loads(googleAPICredentials)
            except:
                self._googleAPICredentials = googleAPICredentials
        else:
            try:
                with open(googleAPICredentialsFile, "r") as creds:
                    self._googleAPICredentials = json.loads(next(creds))
            except:
                print("Invalid Google API credentials file/data.")
                exit(2)
            

    def auth(self, printUserInfo=True):
        """Authenticates Gmail user through Gmail API

        Args:
            printUserInfo: Whether or not user informations summary
                            should be printed

        Returns:
            Gmail credentials
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(self._tokenFile):
            with open(self._tokenFile, 'rb') as token:
                try:
                    creds = pickle.load(token)
                except:
                    creds = None
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_config(
                    self._googleAPICredentials, SCOPES,
                    redirect_uri='urn:ietf:wg:oauth:2.0:oob')
                if not self._googleConsoleAuth:
                    creds = flow.run_local_server(port=self._port)
                else:
                    auth_url, _ = flow.authorization_url(prompt='consent')
                    print('Please go to this URL: \n\n{}'.format(auth_url), end="\n\n")
                    code = input('Enter the authorization code: ')
                    flow.fetch_token(code=code)
                    creds = flow.credentials
                """
                creds = flow.run_console(
                    authorization_prompt_message="Please visit this URL to authorize this application: \n\n{url}",
                    authorization_code_message='Enter the authorization code: ')
                """
            # Save the credentials for the next run
            with open(self._tokenFile, 'wb') as token:
                pickle.dump(creds, token)
        self._creds = creds

        self.buildGmailService()
        # self.getUserInfo()
        self.getUserEmail()
        if printUserInfo:
            self.printUserInfo()

        return creds

    def buildGmailService(self):
        """Builds Gmail service from object credentials, and saved it as {object}_gmailService

        Args:
            None

        Returns:
            Gmail Service
        """
        self._gmailService = build('gmail', 'v1', credentials=self._creds)
        return self._gmailService

    def getUserInfo(self):
        """Retrieves Gmail Profile from Gmail API

        Args:
            None

        Returns:
            User informations ({emailAddress, messagesTotal, threadsTotal})
        """
        if not self._userInfo:
            self._userInfo = (self._gmailService.users().getProfile(userId=self._userId).execute())
        return self._userInfo

    def printUserInfo(self):
        """Prints Gmail's User Profile

        Args:
            None

        Returns:
            None
        """
        self.getUserInfo()
        print("=============== USER DATA ===============")
        print("User email : {}".format(self._userInfo.get("emailAddress", None)))
        print("Messages count : {}".format(self._userInfo.get("messagesTotal", None)))
        print("Threads count : {}".format(self._userInfo.get("threadsTotal", None)))
        print("=============== --------- ===============")

    def getUserEmail(self):
        """Retrieves user email from Google Gmail Profile

        Args:
            None

        Returns:
            Gmail user email
        """
        if not self._userEmail:
            self._userEmail = self.getUserInfo()['emailAddress']
            # self._userEmail = (self._gmailService.users().getProfile(userId=self._userId).execute())['emailAddress']
            self._userEmail_cleanForFiles = self._userEmail.replace("@", "_at_")
        return self._userEmail

    def listMessagesIDs(self, label_ids=None, includeSpamTrash=True):
        """Retrieves messages IDs list for Gmail account.
        Should be called from _listMessagesIDs to get cleaned filename

        [step]: step1.1

        v1 saves IDs into file.

        Args:
            service: Gmail API service
            label_ids: Array containing IDs of labels to filter out
            includeSpamTrash: boolean

        Returns:
            A tuple containing the filename of messages list's data,
            and the number of messages retrieved
        """
        startTime = time.time()

        file = base_temp_files_dir + "/" + genDT() + "--GmailMessagesIDs.temp"
        try:
            page_i = 0
            total_len = 0
            real_total_len = 0
            response = self._gmailService\
                            .users()\
                            .messages()\
                            .list(userId=self._userId,
                                    includeSpamTrash=includeSpamTrash,
                                    labelIds=label_ids
                            ).execute()
            messages = []
            if 'messages' in response:
                messages.extend(response['messages'])
                total_len += len(response['messages'])

            while 'nextPageToken' in response:
                page_i += 1
                print(f"[ step1.1 ] New page ({page_i})...")
                page_token = response['nextPageToken']
                response = self._gmailService\
                                .users()\
                                .messages()\
                                .list(userId=self._userId,
                                        includeSpamTrash=includeSpamTrash,
                                        labelIds=label_ids,
                                        pageToken=page_token
                                ).execute()
                messages.extend(response['messages'])
                total_len += len(response['messages'])

                if total_len >= 1000:
                    f = open(file, "a+")
                    for m in messages:
                        f.write(json.dumps(m) + "\n")
                    f.close()
                    messages = []
                    real_total_len += total_len
                    total_len = 0
                    print("Cleaned !")

            real_total_len += total_len
            f = open(file, "a+")
            for m in messages:
                f.write(json.dumps(m) + "\n")
            f.close()
            # print("End !")

            speed = time.time() - startTime
            self._speedtests.append({
                "user": userEmail,
                "step": "step1.1",
                "time": speed,
                "description": "Retrieve messages IDs list"
            })

            return (file, real_total_len)
        except errors.HttpError as error:
            print('An error occurred: %s' % error)

    def _listMessagesIDs(self):
        """Retrieves messages IDs list for Gmail account.

        [step]: step1.1

        v1 saves IDs into file.

        Args:
            None

        Returns:
            A tuple containing the filename (cleaned !) of messages IDs list's
            data, and the number of messages retrieved
        """
        messagesListFile, totalMessagesCount = self.listMessagesIDs()
        return (
            messagesListFile.split("\\")[-1].split("/")[-1],
            totalMessagesCount)

    def listMessagesIDs__v2(
        self,
        label_ids=None,
        includeSpamTrash=True,
        saveInDB=True
    ):
        """Retrieves messages IDs list for Gmail account.
        Should be called from _listMessagesIDs to get cleaned filename

        [step]: step1.1

        v2 saves IDs into sqlite3 database.

        Args:
            label_ids: Array containing IDs of labels to filter out
            includeSpamTrash: boolean
            saveInDB: boolean, used to know if IDs should be saved in DB
                        or returned

        Returns:
            A tuple containing the table name of messages list's data,
            and the number of messages retrieved,
            or the list of IDs if saveInDB=False
        """
        startTime = time.time()

        if saveInDB:
            tbl = genDT()
            file = sqlite3.connect(base_temp_files_dir+"/"+"GmailMessagesIDs.db")
            cur = file.cursor()
            cur.execute(f"CREATE TABLE `{tbl}` (\
                `id` VARCHAR(255)\
            );")
            file.commit()
            cur.close()
        try:
            page_i = 0
            total_len = 0
            real_total_len = 0
            response = self._gmailService\
                            .users()\
                            .messages()\
                            .list(userId=self._userId,
                                    includeSpamTrash=includeSpamTrash,
                                    labelIds=label_ids
                            ).execute()
            messagesIDs = []
            if 'messages' in response:
                messagesIDs.extend(response['messages'])
                total_len += len(response['messages'])

            while 'nextPageToken' in response:
                page_i += 1
                print(f"[ step1.1 ] New page ({page_i})...")
                page_token = response['nextPageToken']
                response = self._gmailService\
                                .users()\
                                .messages()\
                                .list(userId=self._userId,
                                        includeSpamTrash=includeSpamTrash,
                                        labelIds=label_ids,
                                        pageToken=page_token
                                ).execute()
                messagesIDs.extend(response['messages'])
                total_len += len(response['messages'])

                if saveInDB and total_len >= 1000:
                    cur = file.cursor()
                    for m in messagesIDs:
                        cur.execute(
                            f"INSERT INTO `{tbl}` VALUES (?)", (m["id"],))
                    cur.close()
                    file.commit()
                    messagesIDs = []
                    real_total_len += total_len
                    total_len = 0
                    print("Cleaned !")

            real_total_len += total_len
            if saveInDB:
                cur = file.cursor()
                for m in messagesIDs:
                    cur.execute(f"INSERT INTO `{tbl}` VALUES (?)", (m["id"],))
                cur.execute(f"SELECT COUNT(*) FROM `{tbl}`").fetchall()[0][0]
                cur.close()
                file.commit()
                file.close()
            # print("End !")

            speed = time.time() - startTime
            self._speedtests.append({
                "user": self._userEmail,
                "step": "step1.1",
                "time": speed,
                "description": "Retrieve messages IDs list"
            })

            if saveInDB: return (tbl, real_total_len)
            else: return messagesIDs
        except errors.HttpError as error:
            print('An error occurred: %s' % error)

    def _listMessagesIDs__v2(
        self,
        label_ids=None,
        includeSpamTrash=True,
        saveInDB=True
    ):
        """Retrieves messages IDs list for Gmail account.

        [step]: step1.1

        v2 saves IDs into sqlite3 database.

        Args:
            label_ids: Array containing IDs of labels to filter out
            includeSpamTrash: boolean
            saveInDB: boolean, used to know if IDs should be saved in DB
                        or returned

        Returns:
            A tuple containing the table name of messages IDs list's data,
            and the number of messages retrieved,
            or the list of IDs if saveInDB=False
        """
        if saveInDB:
            messagesListFile, totalMessagesCount = self.listMessagesIDs__v2(
                label_ids=label_ids,
                includeSpamTrash=includeSpamTrash,
                saveInDB=saveInDB)
            return (messagesListFile, totalMessagesCount)
        else:
            messageIDs = self.listMessagesIDs__v2(
                label_ids=label_ids,
                includeSpamTrash=includeSpamTrash,
                saveInDB=saveInDB)
            return messageIDs

    def getMailDetails(self, id):
        """Retrieves message details from message id

        v1 does not catches error and does not accept custom gmailService

        Args:
            service: Gmail API service
            id: Gmail Message ID

        Returns:
            Mail data from Gmail API
        """
        response = self._gmailService\
                        .users()\
                        .messages()\
                        .get(userId=self._userId, id=id)\
                        .execute()
        return response

    def getMailDetails__v2(self, id, gmailService=None):
        """Retrieves message details from message id

        v2 catches error and does accept custom gmailService

        Args:
            id: Gmail Message ID
            [gmailService: Gmail API service]

        Returns:
            Mail data from Gmail API
        """
        service = gmailService if gmailService else self._gmailService
        try:
            response = service.users().messages().get(userId=self._userId,
                                                        id=id).execute()
        except:
            response = None
            print(
                "[ ERROR ] [ getMailDetails__v2 ] [ messageId = " + id + " ]",
                end="")
        del service
        return response

    def listLabels(self):
        """Builds the list of all Gmail account's labels

        Args:
            None

        Returns:
            A list of labels
        """
        results = self._gmailService.users().labels().list(userId=self._userId).execute()
        labels = results.get('labels', [])
        return labels if labels else []

    def reformatMessage(self, message):
        """Build 'clean' message object from raw Google API message object

        Args:
            message: Google Message object

        Returns:
            A dict
        """
        obj = {}
        obj['id'] = message['id']
        obj['threadId'] = message['threadId'] if 'threadId' in message else None
        obj['epoch_ts'] = message['internalDate'] if 'internalDate' in message else None
        obj['labelIds'] = message['labelIds'] if 'labelIds' in message else []
        obj['body'] = message['snippet'] if 'snippet' in message else ""
        obj['sizeEstimate'] = message['sizeEstimate']
        obj['payload'] = {
            'body': message['payload']['body'] if 'body' in message['payload'] else {},
            'filename': message['payload']['filename'],
            'headers': (message['payload']['headers']) if 'headers' in message['payload'] else [],
            'mimeType': (message['payload']['mimeType']) if 'mimeType' in message['payload'] else ""
        }
        if 'parts' in message['payload']:
            obj['payload']['parts'] = (message['payload']['parts'])
        else:
            obj['payload']['parts'] = ""

        return obj

    def processMessagesIDs(self, listData, totalCount):
        """Retrieves message details from IDs list

        [step]: step1.2 (-v1)

        v1: Is ran in main process

        Args:
            listData: Iterable object, containing output of messages.list()
                        (one message per line)
            totalCount: Total messages count (only used for display)

        Returns:
            A tuple containing the name of the table containing
            messages list's data, and the number of messages retrieved
        """
        count = _sub_count = 0
        messages = []

        messagesTbl = genDT() + "--GmailMessagesData"
        messagesDB = base_temp_files_dir + "/" + "GmailMessagesData.db"
        f = sqlite3.connect(messagesDB)

        cur = f.cursor()
        cur.execute(f"CREATE TABLE `{messagesTbl}` (\
    	    `id` VARCHAR(255),\
    	    `threadId` VARCHAR(255),\
    	    `epoch_ts` INT(11),\
    	    `labelIds` TEXT,\
    	    `body` TEXT,\
    	    `sizeEstimate` INT(11),\
    	    `payload_body` TEXT,\
    	    `payload_filename` VARCHAR(255),\
    	    `payload_headers` TEXT,\
    	    `payload_parts` TEXT,\
    	    `payload_mimeType` VARCHAR(255)\
            );")
        f.commit()

        for m in listData:
            print(f"[ step1.2 ]",
                    f"[ {count} / {totalCount} ]",
                    f"Processing mail n°{count}...")
            messages.append(self.getMailDetails(json.loads(m)["id"]))
            count += 1
            _sub_count += 1
            if _sub_count >= 100:
                cur = f.cursor()
                i = 0
                for m in messages:
                    m = self.reformatMessage(m)
                    query = (f"INSERT INTO `{messagesTbl}`\
                        (id, threadId, epoch_ts, labelIds, body, \
                        sizeEstimate, payload_body, payload_filename, \
                        payload_headers, payload_parts, payload_mimeType) \
                        VALUES ('{m['id']}', '{m['threadId']}', \
                        {m['epoch_ts']}, '{_json.dumps(m['labelIds'])}', \
                        '{_json.dumps(m['body'])}', \
                        '{_json.dumps(m['sizeEstimate'])}', \
                        '{_json.dumps(m['payload']['body'])}', \
                        '{_json.dumps(m['payload']['filename'])}', \
                        '{_json.dumps(m['payload']['headers'])}', \
                        '{_json.dumps(m['payload']['parts'])}', \
                        '{_json.dumps(m['payload']['mimeType'])}')")
                    cur.execute(query)
                    i += 1
                del(messages); messages = []; _sub_count = 0
                f.commit()
                cur.close()
                print("Saved !")

        cur = f.cursor()
        for m in messages:
            m = self.reformatMessage(m)
            query = (f"INSERT INTO `{messagesTbl}` \
                (id, threadId, epoch_ts, labelIds, body, sizeEstimate, \
                payload_body, payload_filename, payload_headers, \
                payload_parts, payload_mimeType) VALUES ('{m['id']}', \
                '{m['threadId']}', {m['epoch_ts']}, \
                '{_json.dumps(m['labelIds'])}', '{_json.dumps(m['body'])}', \
                '{_json.dumps(m['sizeEstimate'])}', \
                '{_json.dumps(m['payload']['body'])}', \
                '{_json.dumps(m['payload']['filename'])}', \
                '{_json.dumps(m['payload']['headers'])}', \
                '{_json.dumps(m['payload']['parts'])}', \
                '{_json.dumps(m['payload']['mimeType'])}')")
            cur.execute(query)
        # print("End !")
        f.commit()
        cur.close()
        f.close()

        return (messagesTbl, _sub_count)

    def processMessagesIDs__v2(
        self,
        sqliteDbFile,
        IDsListTblName,
        _sql_begin,
        _sql_limit,
        totalCount,
        messagesTbl,
        subFilename
    ):
        """Retrieves message details from IDs list

        [step]: step1.2-v2

        v2: Is ran in threads

        Args:
            sqliteDbFile: Temp database, containing Gmail mails IDs
            IDsListTblName: Name of the table, in @sqliteDbFile database,
                            containing wanted IDs
            _sql_begin: Starting row
            _sql_limit: Amount of rows to process
                        (row end = @_sql_begin + @_sql_limit)
            totalCount: Number total of fetched IDs (for debugging purposes)
            messagesTbl: Name of the table,
                            in database composed from @subFilename,
                            in which downloaded messages will be saved
            subFilename: [often random string] used to identify the temp db
                            in which downloaded messages will be stored

        Returns:
            A tuple containing the name of the table containing
            messages list's data, and the number of messages retrieved
        """
        service = build('gmail', 'v1', credentials=self._creds)
        count = _sub_count = 0
        count = _sql_begin
        messages = []

        _f = sqlite3.connect(sqliteDbFile)
        listData = _f.cursor()
        listData.execute("SELECT * FROM `"
            + IDsListTblName
            + f"` LIMIT {_sql_begin},{_sql_limit};")

        # messagesTbl = genDT() + "--GmailMessagesData"
        messagesDB = base_temp_files_dir\
                    + f"/.{subFilename}.GmailMessagesData.db"
        f = sqlite3.connect(messagesDB)

        cur = f.cursor()
        cur.execute(f"CREATE TABLE `{messagesTbl}` (\
            `id` VARCHAR(255),\
            `threadId` VARCHAR(255),\
            `epoch_ts` INT(11),\
            `labelIds` TEXT,\
            `body` TEXT,\
            `sizeEstimate` INT(11),\
            `payload_body` TEXT,\
            `payload_filename` VARCHAR(255),\
            `payload_headers` TEXT,\
            `payload_parts` TEXT,\
            `payload_mimeType` VARCHAR(255)\
            );")
        f.commit()

        for m in listData:
            _id = m[0]
            print(f"[ step1.2-v2 ]",
                    f"[ {count} / {totalCount} ]",
                    f"Processing mail n°{count}...")
            messages.append(self.getMailDetails__v2(_id, gmailService=service))
            count += 1
            _sub_count += 1
            if _sub_count >= 100:
                cur = f.cursor()
                i = 0
                for m in messages:
                    if not m:
                        continue
                    m = self.reformatMessage(m)
                    query = (f"INSERT INTO `{messagesTbl}` (id, threadId, \
                        epoch_ts, labelIds, body, sizeEstimate, payload_body, \
                        payload_filename, payload_headers, payload_parts, \
                        payload_mimeType) VALUES ('{m['id']}', \
                        '{m['threadId']}', {m['epoch_ts']}, \
                        '{_json.dumps(m['labelIds'])}', \
                        '{_json.dumps(m['body'])}', \
                        '{_json.dumps(m['sizeEstimate'])}', \
                        '{_json.dumps(m['payload']['body'])}', \
                        '{_json.dumps(m['payload']['filename'])}', \
                        '{_json.dumps(m['payload']['headers'])}', \
                        '{_json.dumps(m['payload']['parts'])}', \
                        '{_json.dumps(m['payload']['mimeType'])}')")
                    cur.execute(query)
                    i += 1
                del(messages); messages = []; _sub_count = 0
                f.commit()
                cur.close()
                print("Saved !")

        cur = f.cursor()
        for m in messages:
            if not m:
                continue
            m = self.reformatMessage(m)
            query = (f"INSERT INTO `{messagesTbl}` \
                (id, threadId, epoch_ts, labelIds, body, sizeEstimate, \
                payload_body, payload_filename, payload_headers, \
                payload_parts, payload_mimeType) VALUES ('{m['id']}', \
                '{m['threadId']}', {m['epoch_ts']}, \
                '{_json.dumps(m['labelIds'])}', '{_json.dumps(m['body'])}', \
                '{_json.dumps(m['sizeEstimate'])}', \
                '{_json.dumps(m['payload']['body'])}', \
                '{_json.dumps(m['payload']['filename'])}', \
                '{_json.dumps(m['payload']['headers'])}', \
                '{_json.dumps(m['payload']['parts'])}', \
                '{_json.dumps(m['payload']['mimeType'])}')")
            cur.execute(query)
        # print("End !")
        f.commit()
        cur.close()
        f.close()

        listData.close()
        _f.close()

        return (messagesTbl, _sub_count)

    def _processMessagesFromIDsFile__v2(self, _tblName, messagesCount="Unknown", singleThread=False):
        """Opens messages IDs list from file,
            and parse them with {object}.processMessagesIDs__v2

        v2: Uses IDs DB, and starts many threads instead of one

        Args:
            filename: File name
            messagesCount: Nb of messages to parse. 
                            Used only for display purposes.

        Returns:
            A tuple containing the name of the table containing
            messages list's data, and the number of messages retrieved
        """

        startTime = time.time()

        f = sqlite3.connect(base_temp_files_dir+"/"+"GmailMessagesIDs.db")
        cur = f.cursor()

        # limit = 250
        tblName = genDT() + "--GmailMessagesData"
        rowsCount = cur.execute(f"SELECT COUNT(*) FROM `{_tblName}`;")\
                    .fetchall()[0][0]
        if singleThread:
            limit = rowsCount+1
            dispatch = [0]
        else:
            if gThreadsCount:
                limit = rowsCount // gThreadsCount
            elif rowsCount <= 2000:
                limit = rowsCount // 10
            # limit = rowsCount // 8
            else:
                limit = rowsCount // 6
            dispatch = Tools.genList(rowsCount, x=limit)
        threads = []
        filenames = []

        # Start threads
        for begin_i in range(len(dispatch)):
            filenames.append(Tools.RandomString(10))
            p = threading.Thread(
                target=self.processMessagesIDs__v2,
                args=(
                    base_temp_files_dir+"/"+"GmailMessagesIDs.db",
                    _tblName,
                    dispatch[begin_i],
                    limit,
                    rowsCount,
                    tblName,
                    filenames[begin_i]
                )
            )
            threads.append(p)
            p.start()

        # Init final-temp DB, and create table in it
        fData = sqlite3.connect(base_temp_files_dir+"/"+"GmailMessagesData.db")
        curData = fData.cursor()
        curData.execute(f"CREATE TABLE `{tblName}` (\
            `id` VARCHAR(255),\
            `threadId` VARCHAR(255),\
            `epoch_ts` INT(11),\
            `labelIds` TEXT,\
            `body` TEXT,\
            `sizeEstimate` INT(11),\
            `payload_body` TEXT,\
            `payload_filename` VARCHAR(255),\
            `payload_headers` TEXT,\
            `payload_parts` TEXT,\
            `payload_mimeType` VARCHAR(255)\
            );")
        fData.commit()
        sqlDotString = ",".join(["?" for i in range(11)])

        for thread_i in range(len(threads)):
            threads[thread_i].join()

            s = sqlite3.connect(base_temp_files_dir+f"/.{filenames[thread_i]}.GmailMessagesData.db")
            c = s.cursor()
            try:
                c.execute(f"SELECT * FROM `{tblName}`;")
            except:
                pass
            for line in c:
                curData.execute(f"INSERT INTO `{tblName}` VALUES ({sqlDotString})", line)
            c.close()
            s.close()
            os.remove(os.path.join(base_temp_files_dir+"/","."+filenames[thread_i]+".GmailMessagesData.db"))

        fData.commit()
        fData.close()

        del dispatch;
        del filenames;
        del threads;

        speed = time.time() - startTime
        self._speedtests.append({"user": self._userEmail, "step": "step1.2", "time": speed, "description": "Retrieve messages from IDs list"})

        return (tblName, rowsCount)

    def _processMessagesFromIDsFile(self, filename, messagesCount="Unknown"):
        """Opens messages IDs list from file,
            and parse them with {object}.processMessagesIDs

        v1: Starts only 1 thread

        Args:
            filename: File name
            messagesCount: Nb of messages to parse.
                            Used only for display purposes.

        Returns:
            A tuple containing the name of the table containing
            messages list's data, and the number of messages retrieved
        """
        f = open(base_temp_files_dir+"/"+filename, "r")
        messagesTbl, totalMessagesCount = self.processMessagesIDs(
                                            f,
                                            messagesCount)
        f.close()
        return (messagesTbl, totalMessagesCount)

    def RetrieveMessagesIDsThenMessagesFullList(self, ask=True):
        """First retrieves all messages IDs from Gmail API,
            then parse them to get full message data (from same API).

        [step]: step1.1 + step1.2

        Args:
            ask: Boolean used to determine if user should be asked questions

        Returns:
            Name of the table containing messages' data
        """

        userEmail = self._userEmail
        userEmail_cleanForFiles = self._userEmail_cleanForFiles

        totalMessagesCount = "Unknown"
        messagesListFile = None
        messagesTbl = None

        global base_temp_files_dir
        if not userEmail_cleanForFiles in base_temp_files_dir:
            base_temp_files_dir += "/"+userEmail_cleanForFiles

        if not Path(base_temp_files_dir).exists():
            Path(base_temp_files_dir).mkdir(parents=True, exist_ok=True)

        IDsListDB_filename = base_temp_files_dir + "/GmailMessagesIDs.db"
        IDsListDB = sqlite3.connect(IDsListDB_filename)
        IDsListDB_cur = IDsListDB.cursor()

        if not messagesTbl:
            l = [e[0] for e in 
                IDsListDB_cur\
                .execute("SELECT name FROM sqlite_master")\
                .fetchall()]
            if l and ask:
                print("Saved mail IDs already exist.",
                    "Do you want to load one of them ? ")
                for i in range(len(l)):
                    print(f"# {i+1}) {l[i].split('--')[0]}")
                print()
                toDo = input(f"Enter the backup's number, or 'c' to retrieve "\
                            + f"latest messages IDs. [c/"\
                            + '/'.join([str(i+1) for i in range(len(l))])\
                            + "] ").lower()
                if toDo == "c":
                    messagesIDsTbl, totalMessagesCount = self._listMessagesIDs__v2()
                    # messagesListFile, totalMessagesCount = self._listMessagesIDs()
                else:
                    try:
                        toDo = int(toDo)
                        print(f"You choose to use",
                                l[toDo-1],
                                "backup. Loading file...")
                    except:
                        print("Invalid input !")
                        return 1
                    messagesIDsTbl = l[toDo-1]
            else:
                messagesIDsTbl, totalMessagesCount = self._listMessagesIDs__v2()
                # messagesListFile, totalMessagesCount = self._listMessagesIDs()

            messagesTbl, totalMessagesCount = self.\
                    _processMessagesFromIDsFile__v2(
                        messagesIDsTbl,
                        messagesCount=totalMessagesCount)
            # messagesTbl, totalMessagesCount = self._processMessagesFromIDsFile(messagesListFile, messagesCount=totalMessagesCount)

        return messagesTbl

    def Run(self, cleanDB=True, cleanAttachmentFiles=True, confirm=False):
        if cleanDB:
            self._cleanFinalDB(confirm=confirm)
        if cleanAttachmentFiles:
            self._cleanAttachmentFiles(confirm=confirm)

        tempMessagesTbl = step1(gmailUtils=self, ask=confirm)

        generateCleanMessagesDataAndDownloadAttachments__v2(
            _tblName=tempMessagesTbl,
            gmailUtils=self,
            speedtests=self._speedtests)

        zipFilename = generateExcel(
            userEmail=self._userEmail,
            speedtests=self._speedtests)

        return (tempMessagesTbl, zipFilename)

    def getLabels(self):
        """Builds the list of all Gmail account's labels

        Args:
            None

        Returns:
            A list of labels
        """
        return self._gmailService.users().labels().list(userId=self._userId).execute().get('labels', [])

    def _cleanFinalDB(self, confirm=True):
        if confirm and not Tools.confirm(f"You are going to empty final database for user {self._userEmail}. Are you sure you want to continue?"):
            return False
        f = sqlite3.connect(finalDataDB)
        fCur = f.cursor()
        for tbl_suffix in ["_messages", "_labels", "_attachments"]:
            tbl = self._userEmail + tbl_suffix
            try:
                fCur.execute(f"DROP TABLE `{tbl}`");
            except:
                pass
        fCur.close()
        f.commit()
        f.close()

        return True

    def _cleanAttachmentFiles(self, confirm=True):
        if confirm and not Tools.confirm(f"You are going to delete all downloaded attachments for user {self._userEmail}. Are you sure you want to continue ?"):
            return False
        attachmentsDir = base_attachments_dir + "/" + self._userEmail_cleanForFiles;
        shutil.rmtree(attachmentsDir, ignore_errors=True)

        return True

    def Clean(self, confirm=True, _del_finalData=False, _del_zipFiles=False):
        """Cleans user's file
        Removes :
            - Temp files (./temp-files/user_email/*)
            - Attachments (./attachments/user_email/*)
            - Entries in FinalDataDB (_messages, _labels, _attachments)
            - Generated ZIP files (./*--user_email--GmailMessagesList.zip)

        Args:
            confirm: Boolean used to determine if user should be asked a confirmation

        Returns:
            A list of labels
        """
        if confirm:
            if not Tools.confirm(f"You are going to wipe all data (ZIP files, DB tables and attachments files) saved locally for user {self._userEmail}. Continue ?"):
                return False
            _del_finalData = Tools.confirm("Do you also want to delete final DB data (including attachments files) ?", _del_finalData)
            _del_zipFiles = Tools.confirm("Do you also want to delete final ZIP files ? (DANGEROUS !)", _del_zipFiles)

        # Remove temp files
        tempDir = base_temp_files_dir + "/" + self._userEmail_cleanForFiles;
        shutil.rmtree(tempDir, ignore_errors=True)

        if _del_finalData:
            # Remove attachments folder
            self._cleanAttachmentFiles(confirm=False)
            """
            attachmentsDir = base_attachments_dir + "/" + self._userEmail_cleanForFiles;
            shutil.rmtree(attachmentsDir, ignore_errors=True)
            """

            # Remove all entries from DB
            self._cleanFinalDB(confirm=False)
            """
            f = sqlite3.connect(finalDataDB)
            fCur = f.cursor()
            for tbl_suffix in ["_messages", "_labels", "_attachments"]:
                tbl = self._userEmail + tbl_suffix
                try:
                    fCur.execute(f"DROP TABLE `{tbl}`");
                except:
                    pass
            fCur.close()
            f.commit()
            f.close()
            """

        if _del_zipFiles:
            # Remove Zip files
            for p in Path(".").glob(f"*--{self._userEmail_cleanForFiles}--GmailMessagesList.zip"):
                p.unlink()

        print("Cleaning finished !")
        return True

    def FinalDataExists(self):
        f = sqlite3.connect(finalDataDB)
        fCur = f.cursor()
        for tbl_suffix in ["_messages", "_labels", "_attachments"]:
            tbl = self._userEmail + tbl_suffix
            try:
                fCur.execute(f"SELECT * FROM `{tbl}` LIMIT 1");
                if len(fCur.fetchall()[0]) == 0:
                    raise GenericError
            except:
                fCur.close()
                f.close()
                return False
        fCur.close()
        f.close()
        return True

    def GenerateLabelsMailsCount(self):
        f = sqlite3.connect(finalDataDB)
        fCur = f.cursor()
        labelsTbl = self._userEmail + "_labels"
        messagesTbl = self._userEmail + "_messages"

        fCur.execute(f"SELECT * FROM `{labelsTbl}`")

        labels = {}
        for label in fCur:
            labelId, labelName, labelType, _ = label
            labels[labelId] = [labelName, 0]

        fCur.execute(f"SELECT labelIds FROM `{messagesTbl}`")
        for msg in fCur:
            labelIds = msg[0].split(",")
            for labelId in labelIds:
                if labelId in labels:
                    labels[labelId][1] += 1

        return labels

    def GetLabelNameFromId(self, labelId):
        f = sqlite3.connect(finalDataDB)
        fCur = f.cursor()
        labelsTbl = self._userEmail + "_labels"
        fCur.execute(f"SELECT name FROM `{labelsTbl}` WHERE id = '{labelId}' LIMIT 1")
        try:
            name = fCur.fetchall()[0][0]
        except:
            return None
        return name

    def Delete_FromLabel__offline(self, labelIds, force=False):
        """Delete all mails assigned to @labelIds
        Mail must be in all labels corresponding to @labelIds to be deleted.
        """
        if not labelIds:
            return False

        print("\n" + " ".join(["/!\\"]*30))
        print("/!\\ Local saved data will be used to fetch messages' IDs. If you want latest data, first start this script with `run` action.")
        print(" ".join(["/!\\"]*30))

        columnName = "labelIds"
        strCheck = " AND ".join([(f"\"{columnName}\" LIKE '%{e}%'") for e in labelIds])
        labelsNames = []
        for id in labelIds:
            name = self.GetLabelNameFromId(id)
            if name == None:
                return False
            labelsNames.append(name)
        labelNamesStr = ",".join(labelsNames)

        f = sqlite3.connect(finalDataDB)
        fCur = f.cursor()
        # labelsTbl = self._userEmail + "_labels"
        messagesTbl = self._userEmail + "_messages"
        messagesToDel = [];

        fCur.execute(f"SELECT id,labelIds FROM `{messagesTbl}` WHERE {strCheck}")
        for msg in fCur:
            msg_labelIds = msg[1].split(",")
            if set(labelIds) <= set(msg_labelIds):
                messagesToDel.append(msg[0])

        if messagesToDel == []:
            return False

        if not force and not Tools.confirm(f"\nYou are going to permanently delete (not just trash !) {len(messagesToDel)} mails from labels [{labelNamesStr}]. Continue ?", default=False):
            print("Aborting...")
            return False

        return self.Delete_FromList(messagesToDel, force=True)

    def Delete_FromLabel(self, labelIds, force=False):
        """Delete all mails assigned to @labelIds
        Mail must be in all labels corresponding to @labelIds to be deleted.
        """
        startTime = time.time()
        breaks = []

        if not labelIds:
            return False

        columnName = "labelIds"
        labelsNames = []
        for id in labelIds:
            name = self.GetLabelNameFromId(id)
            if name == None:
                return False
            labelsNames.append(name)
        labelNamesStr = ",".join(labelsNames)

        messagesToDel = [e["id"] for e in self._listMessagesIDs__v2(saveInDB=False, label_ids=labelIds, includeSpamTrash=True) if "id" in e and e["id"]]

        if messagesToDel == []:
            print("Nothing to remove !")
            return False


        breaks.append(time.time())
        if not force and not Tools.confirm(f"\nYou are going to permanently delete (not just trash !) {len(messagesToDel)} mails from labels [{labelNamesStr}]. Continue ?", default=False):
            print("Aborting...")
            return False
        breaks[-1] = time.time() - breaks[-1]

        rt = self.Delete_FromList(list=messagesToDel, force=True, speedtest=False)

        speed = time.time() - startTime
        for brk in breaks: speed -= brk
        self._speedtests.append({
            "user": self._userEmail,
            "step": "delete-from-label",
            "time": speed,
            "description": "Delete all messages in intersection of labels"
        })

        return rt

    def _batchDelete(self, list):
        return self._gmailService.users().messages().batchDelete(
            userId=self._userId,
            body={
                "ids": list
            }
        ).execute()

    def Delete_FromList(self, list, force=False, speedtest=True):
        if list == []:
            print("Nothing to delete !")
            return False

        # if speedtest:
        startTime = time.time()
        breaks = []

        breaks.append(time.time())
        if not force and not Tools.confirm(f"\nYou are going to permanently delete (not just trash !) {len(list)}. Continue ?", default=False):
            print("Aborting...")
            return False
        breaks[-1] = time.time() - breaks[-1]

        for i in range(0, len(list), 1000):
            self._batchDelete(list[i:i+1000])

        if speedtest:
            speed = time.time() - startTime
            for brk in breaks: speed -= brk
            self._speedtests.append({
                "user": self._userEmail,
                "step": "delete-from-list",
                "time": speed,
                "description": "Delete all messages in intersection of labels"
            })

        return len(list)



def step1(gmailUtils=None, ask=True):
    """Returns string containing tbl name of table whith contains temp messages data

    Args:
        gmailUtils: Optional paramater. Used only in case of full-run, to re-use GmailUtils object.

    Returns:
        Result of GmailUtils.RetrieveMessagesIDsThenMessagesFullList()
    """
    if not gmailUtils:
        gmailUtils = GmailUtils(port=gPort, tokenFile=gTokenFile)
        gmailUtils.auth()
    return gmailUtils.RetrieveMessagesIDsThenMessagesFullList(ask=ask)

# Saves messages data in DB + download and saves attachments
# + saves label data
def generateCleanMessagesDataAndDownloadAttachments(_tblName=None, gmailUtils=None, speedtests=None):
    userId = "me"

    if not gmailUtils:
        gmailUtils = GmailUtils(port=gPort, tokenFile=gTokenFile)
        gmailUtils.auth()

    userEmail = gmailUtils._userEmail
    userEmail_cleanForFiles = gmailUtils._userEmail_cleanForFiles
    finalData = sqlite3.connect(finalDataDB)

    global base_attachments_dir
    if not userEmail_cleanForFiles in base_attachments_dir:
        base_attachments_dir += "/" + userEmail_cleanForFiles
    global base_temp_files_dir
    if not userEmail_cleanForFiles in base_temp_files_dir:
        base_temp_files_dir += "/"+userEmail_cleanForFiles

    if not Path(base_attachments_dir).exists():
        Path(base_attachments_dir).mkdir(parents=True, exist_ok=True)

    # Create labels, messages and attachments tables
    fCur = finalData.cursor()
    Tools.CreateTable_labels(fCur, userEmail)
    Tools.CreateTable_attachments(fCur, userEmail)
    Tools.CreateTable_messages(fCur, userEmail)
    fCur.close()
    finalData.commit()

    # Save labels in DB
    labels = gmailUtils.getLabels()
    fCur = finalData.cursor()
    d_labels = {}
    for label in labels:
        req = f"SELECT COUNT(*) FROM `{userEmail}_labels` WHERE id='{label['id']}'"
        fCur.execute(req)
        d_labels[label['id']] = label['name']
        r = fCur.fetchall()[0]
        if r[0] > 0:
            continue
        label['name'] = label['name'].replace('\'','')
        fCur.execute(f"INSERT INTO `{userEmail}_labels` (id, name, type) VALUES ('{label['id']}', '{label['name']}', '{label['type']}')")
    fCur.close()
    finalData.commit()
    del labels
    del label

    f = sqlite3.connect(base_temp_files_dir + "/" + "GmailMessagesData.db")
    cur = f.cursor()
    if not _tblName or not cur.execute(f"SELECT name FROM sqlite_master WHERE name = '{_tblName}' LIMIT 1").fetchall():
        # Ask user to choose a saved table for importing whole messages data
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tbls = cur.fetchall()
        print("Please choose table name :")
        choices = {}
        count = 1
        for tblName in tbls:
            tblName = tblName[0]
            print(f"# {count}) {tblName}")
            choices[count] = tblName
            count += 1
        print()
        if not choices:
            return

        # Define table name from user choice
        tbl = choices[int(input("Table index ? "))]

        # Ask user confirmation, with rows count
        cur2 = f.cursor()
        cur2.execute(f"SELECT COUNT(*) FROM `{tbl}`")
        count = cur2.fetchall()[0][0]
        if not Tools.confirm(f"\nYou are going to load {count} lines from table {tbl}. Continue ?"):
            print("Exiting...")
            exit()
        cur2.close()
    else:
        tbl = _tblName

    # Retrieve all messages from temp DB
    cur.execute(f"SELECT * FROM `{tbl}`")
    # cur.execute(f"SELECT * FROM `{tbl}` LIMIT 50")

    fCur = finalData.cursor()

    startTime = time.time()

    # Iterate over each message
    rowId = 0
    for line in cur:
        rowId += 1
        print(f"[ step2 ] Processing {rowId}...")

        messageData = {
            "id": line[0],
            "threadId": line[1],
            "epoch_ts": line[2],
            "labelIds": _json.b64decode(line[3]),
            "body": _json.b64decode(line[4]),
            "sizeEstimate": _json.b64decode(line[5]),
            "payload_body": _json.b64decode(line[6]),
            "payload_filename": _json.b64decode(line[7]),
            "payload_headers": _json.b64decode(line[8]),
            "payload_parts": _json.b64decode(line[9]),
            "payload_mimeType": _json.b64decode(line[10]),
            "attachmentsList": [],
            "From": "",
            "To": "",
            "Subject": "",
            "date": ""
        }
        messageData['date'] = datetime.fromtimestamp(messageData['epoch_ts']/1000).strftime(datetime_format_forExcel)

        fCur.execute(f"SELECT id FROM `{userEmail}_messages` WHERE id = '{messageData['id']}'")
        if fCur.fetchall():
            continue

        for header in messageData['payload_headers']:
            if header['name'] == "From":
                messageData["From"] = header["value"]
            elif header['name'] == "To":
                messageData["To"] = header["value"]
            elif header['name'] == "Subject":
                messageData["Subject"] = header["value"]

        attachmentsDirCreated = False
        attachmentsDir = "/".join([base_attachments_dir, messageData['id']])

        # Iterate over each part, possibly an attachment
        for part in messageData["payload_parts"]:
            if part["filename"] and 'body' in part and 'attachmentId' in part['body']: # If part corresponds to file
                part['filename'] = Tools.FormatFilename(part['filename'].replace("'", "").replace('"', "").replace("/", "-"))
                fileData = {
                    "attachmentId": part['body']['attachmentId'],
                    "generatedId": "",
                    "originFilename": part['filename']
                }

                fCur.execute(f"SELECT id FROM `{userEmail}_attachments` WHERE id = '{fileData['attachmentId']}'")
                if fCur.fetchall():
                    continue

                fileData['generatedId'] = str(random.randint(10000, 99999))

                for header in part.get('headers', []):
                    if header['name'] == "X-Attachment-Id":
                        fileData['generatedId'] = header['value']
                        break

                filename = "-".join([messageData['id'], fileData['generatedId'], part['filename']])
                filepath = '/'.join([attachmentsDir, filename])

                if Path(filepath).exists():
                    continue

                if not attachmentsDirCreated and not Path(attachmentsDir).exists():
                    Path(attachmentsDir).mkdir(parents=True, exist_ok=True)

                messageData['attachmentsList'].append(fileData['attachmentId'])

                if gThreadsCount and gThreadsCount > 1:
                    # threading.Thread(target=_saveAttachment, args=(gmailUtils, userId, messageData['id'], part['body']['attachmentId'], filepath)).start()
                    thread = threading.Thread(target=_saveAttachment, args=(gmailUtils, userId, messageData['id'], part['body']['attachmentId'], filepath))
                    # attachmentsDownload__threads.append(thread);
                    attachmentsDownload__threads[part["body"]["attachmentId"]] = thread;
                    thread.start()
                else:
                    _saveAttachment(gmailUtils, userId, messageData['id'], part['body']['attachmentId'], filepath)

                data = (
                    fileData['attachmentId'],
                    fileData['generatedId'],
                    fileData['originFilename'],
                    messageData["id"]
                )
                fCur.execute(f"INSERT INTO `{userEmail}_attachments` (id, generatedId, originFilename, messageId) VALUES (?,?,?,?)", data)

        messageData['To'] = Tools.parseEmailRecipients(messageData['To'])
        messageData['From'] = Tools.parseEmailRecipients(messageData['From'])
        if len(messageData['From']):
            messageData['From'] = messageData['From'][0]
        else:
            messageData['From'] = {"name": "", "email": ""}
        messageData['From']['name'] = messageData['From']['name']
        messageData['From']['email'] = messageData['From']['email']

        # If message is not already present in db, save it
        dest_names = ",".join([e['name'].replace(",","\,") for e in messageData['To']])
        dest_emails = ",".join([e['email'].replace(",","\,") for e in messageData['To']])
        labelNames = ",".join([d_labels.get(e, "") for e in messageData['labelIds']])
        attachmentsList = ",".join(messageData['attachmentsList'])
        labelIds = ",".join(messageData['labelIds'])

        data = (
            messageData['id'],
            messageData['threadId'],
            messageData['From']['name'],
            messageData['From']['email'],
            dest_names,
            dest_emails,
            json.dumps(messageData['To']),
            labelIds,
            labelNames,
            messageData['epoch_ts'],
            messageData['date'],
            messageData['Subject'],
            messageData['body'],
            messageData['sizeEstimate'],
            attachmentsList
        )
        try:
            fCur.execute(f"INSERT INTO `{userEmail}_messages` (\
                id,\
                threadId,\
                exp_name,\
                exp_email,\
                dest_names,\
                dest_emails,\
                dest_json,\
                labelIds,\
                labelNames,\
                date_ts,\
                date,\
                subject,\
                body,\
                size,\
                attachmentsIds\
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
        except:
            print(sys.exc_info(), end="\n\n")
            print(data, len(data))
            exit()

        finalData.commit()

    cur.close()
    f.close()

    fCur.close()
    finalData.close()

    while attachmentsDownload__threads:
        attachmentsDownload__threads.popitem()[1].join()

    if type(speedtests) == list:
        speed = time.time() - startTime
        speedtests.append({"user": userEmail, "step": "step2", "time": speed, "description": "Generate clean messages data and download attachments"})

    return

def generateCleanMessagesDataAndDownloadAttachments__v2(_tblName=None, gmailUtils=None, speedtests=None, singleThread=False):
    userId = "me"

    if not gmailUtils:
        gmailUtils = GmailUtils(port=gPort, tokenFile=gTokenFile)
        gmailUtils.auth()

    userEmail = gmailUtils._userEmail
    userEmail_cleanForFiles = gmailUtils._userEmail_cleanForFiles
    finalData = sqlite3.connect(finalDataDB)

    global base_attachments_dir
    if not userEmail_cleanForFiles in base_attachments_dir:
        base_attachments_dir += "/" + userEmail_cleanForFiles
    global base_temp_files_dir
    if not userEmail_cleanForFiles in base_temp_files_dir:
        base_temp_files_dir += "/"+userEmail_cleanForFiles

    if not Path(base_attachments_dir).exists():
        Path(base_attachments_dir).mkdir(parents=True, exist_ok=True)

    # Create labels, messages and attachments tables
    fCur = finalData.cursor()
    Tools.CreateTable_labels(fCur, userEmail)
    Tools.CreateTable_attachments(fCur, userEmail)
    Tools.CreateTable_messages(fCur, userEmail)
    fCur.close()
    finalData.commit()

    # Save labels in DB
    labels = gmailUtils.getLabels()
    fCur = finalData.cursor()
    d_labels = {}
    for label in labels:
        req = f"SELECT COUNT(*) FROM `{userEmail}_labels` WHERE id='{label['id']}'"
        fCur.execute(req)
        d_labels[label['id']] = label['name']
        r = fCur.fetchall()[0]
        if r[0] > 0:
            continue
        label['name'] = label['name'].replace('\'','')
        fCur.execute(f"INSERT INTO `{userEmail}_labels` (id, name, type) VALUES ('{label['id']}', '{label['name']}', '{label['type']}')")
    fCur.close()
    finalData.commit()
    del labels
    del label

    f = sqlite3.connect(base_temp_files_dir + "/" + "GmailMessagesData.db")
    cur = f.cursor()
    if not _tblName or not cur.execute(f"SELECT name FROM sqlite_master WHERE name = '{_tblName}' LIMIT 1").fetchall():
        # Ask user to choose a saved table for importing whole messages data
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tbls = cur.fetchall()
        print("Please choose table name :")
        choices = {}
        count = 1
        for tblName in tbls:
            tblName = tblName[0]
            print(f"# {count}) {tblName}")
            choices[count] = tblName
            count += 1
        print()
        if not choices:
            return

        # Define table name from user choice
        tbl = choices[int(input("Table index ? "))]

        # Ask user confirmation, with rows count
        cur2 = f.cursor()
        cur2.execute(f"SELECT COUNT(*) FROM `{tbl}`")
        count = cur2.fetchall()[0][0]
        if not Tools.confirm(f"\nYou are going to load {count} lines from table {tbl}. Continue ?"):
            print("Exiting...")
            exit()
        cur2.close()
    else:
        tbl = _tblName

    startTime = time.time()

    cur.execute(f"SELECT COUNT(*) FROM `{tbl}`")
    rowsCount = cur.fetchall()[0][0]
    if singleThread:
        limit = rowsCount+1
        dispatch = [0]
    else:
        if gThreadsCount:
            limit = rowsCount // gThreadsCount
        elif rowsCount <= 2000:
            limit = rowsCount // 10
        else:
            limit = rowsCount // 6
        dispatch = Tools.genList(rowsCount, x=limit)
    threads = []
    filenames = []

    for begin_i in range(len(dispatch)):
        filenames.append(base_temp_files_dir+f"/.{Tools.RandomString(10)}.GmailFinalMessagesData.db")
        # filenames.append(Tools.RandomString(10))
        p = threading.Thread(
            target=step2_processFinalMessageAndAttachments,
            args=(
                begin_i,
                base_temp_files_dir+"/"+"GmailMessagesData.db",
                tbl,
                dispatch[begin_i],
                limit,
                rowsCount,
                filenames[begin_i],
                d_labels,
                gmailUtils
            )
        )
        threads.append(p)
        attachmentsDownload__threads__v2.append({})
        p.start()

    fCur = finalData.cursor()
    for thread_i in range(len(threads)):
        threads[thread_i].join()

        s = sqlite3.connect(filenames[thread_i])
        c = s.cursor()

        __tbl = f"{userEmail}_messages"
        try:
            str = ",".join(["?" for i in range(len(c.execute(f"SELECT * FROM `{__tbl}` LIMIT 1").fetchall()[0]))])
            c.execute(f"SELECT * FROM `{__tbl}`;")
        except:
            pass
        for line in c:
            fCur.execute(f"INSERT INTO `{__tbl}` VALUES ({str})", line)

        __tbl = f"{userEmail}_attachments"
        try:
            str = ",".join(["?" for i in range(len(c.execute(f"SELECT * FROM `{__tbl}` LIMIT 1").fetchall()[0]))])
            c.execute(f"SELECT * FROM `{__tbl}`;")
        except:
            pass
        for line in c:
            fCur.execute(f"INSERT INTO `{__tbl}` VALUES ({str})", line)

        c.close()
        s.close()
        os.remove(filenames[thread_i])

    finalData.commit()
    fCur.close()
    finalData.close()

    del dispatch;
    del filenames;
    del threads;

    if type(speedtests) == list:
        speed = time.time() - startTime
        speedtests.append({"user": userEmail, "step": "step2-v2", "time": speed, "description": "Generate clean messages data and download attachments"})

    return

def step2_processFinalMessageAndAttachments(_threadId, sourceDB, workingTbl, _sql_begin, _sql_limit, rowsCount, tempFilename, d_labels, gmailUtils):
    # Iterate over each message
    rowId = _sql_begin
    userId = gmailUtils._userId;
    userEmail = gmailUtils._userEmail

    f = sqlite3.connect(sourceDB)
    cur = f.cursor()

    finalData = sqlite3.connect(finalDataDB)
    fCur = finalData.cursor()

    temp_dbFinal = sqlite3.connect(tempFilename)
    _fCur = temp_dbFinal.cursor()
    Tools.CreateTable_messages(_fCur, userEmail)
    Tools.CreateTable_attachments(_fCur, userEmail)
    temp_dbFinal.commit()

    cur.execute(f"SELECT * FROM `{workingTbl}` LIMIT {_sql_begin},{_sql_limit};")
    for line in cur:
        rowId += 1
        print(f"[ step2-v2 ] Processing {rowId}...")

        messageData = {
            "id": line[0],
            "threadId": line[1],
            "epoch_ts": line[2],
            "labelIds": _json.b64decode(line[3]),
            "body": _json.b64decode(line[4]),
            "sizeEstimate": _json.b64decode(line[5]),
            "payload_body": _json.b64decode(line[6]),
            "payload_filename": _json.b64decode(line[7]),
            "payload_headers": _json.b64decode(line[8]),
            "payload_parts": _json.b64decode(line[9]),
            "payload_mimeType": _json.b64decode(line[10]),
            "attachmentsList": [],
            "From": "",
            "To": "",
            "Subject": "",
            "date": ""
        }
        messageData['date'] = datetime.fromtimestamp(messageData['epoch_ts']/1000).strftime(datetime_format_forExcel)

        fCur.execute(f"SELECT id FROM `{userEmail}_messages` WHERE id = '{messageData['id']}'")
        if fCur.fetchall():
            continue

        for header in messageData['payload_headers']:
            if header['name'] == "From":
                messageData["From"] = header["value"]
            elif header['name'] == "To":
                messageData["To"] = header["value"]
            elif header['name'] == "Subject":
                messageData["Subject"] = header["value"]

        attachmentsDirCreated = False
        attachmentsDir = "/".join([base_attachments_dir, messageData['id']])

        # Iterate over each part, possibly an attachment
        for part in messageData["payload_parts"]:
            if part["filename"] and 'body' in part and 'attachmentId' in part['body']: # If part corresponds to file
                part['filename'] = Tools.FormatFilename(part['filename'].replace("'", "").replace('"', "").replace("/", "-"))
                fileData = {
                    "attachmentId": part['body']['attachmentId'],
                    "generatedId": "",
                    "originFilename": part['filename']
                }

                fCur.execute(f"SELECT id FROM `{userEmail}_attachments` WHERE id = '{fileData['attachmentId']}'")
                if fCur.fetchall():
                    continue

                fileData['generatedId'] = str(random.randint(10000, 99999))

                for header in part.get('headers', []):
                    if header['name'] == "X-Attachment-Id":
                        fileData['generatedId'] = header['value']
                        break

                filename = "-".join([messageData['id'], fileData['generatedId'], part['filename']])
                filepath = '/'.join([attachmentsDir, filename])

                if Path(filepath).exists():
                    continue

                if not attachmentsDirCreated and not Path(attachmentsDir).exists():
                    Path(attachmentsDir).mkdir(parents=True, exist_ok=True)

                messageData['attachmentsList'].append(fileData['attachmentId'])

                if gThreadsCount and gThreadsCount > 1:
                    # threading.Thread(target=_saveAttachment, args=(gmailUtils, userId, messageData['id'], part['body']['attachmentId'], filepath)).start()
                    thread = threading.Thread(target=_saveAttachment__v2, args=(_threadId, gmailUtils, userId, messageData['id'], part['body']['attachmentId'], filepath))
                    # attachmentsDownload__threads.append(thread);
                    # attachmentsDownload__threads[part["body"]["attachmentId"]] = thread;
                    attachmentsDownload__threads__v2[_threadId][part["body"]["attachmentId"]] = thread;
                    thread.start()
                else:
                    _saveAttachment(gmailUtils, userId, messageData['id'], part['body']['attachmentId'], filepath)

                data = (
                    fileData['attachmentId'],
                    fileData['generatedId'],
                    fileData['originFilename'],
                    messageData["id"]
                )
                _fCur.execute(f"INSERT INTO `{userEmail}_attachments` (id, generatedId, originFilename, messageId) VALUES (?,?,?,?)", data)

        messageData['To'] = Tools.parseEmailRecipients(messageData['To'])
        messageData['From'] = Tools.parseEmailRecipients(messageData['From'])
        if len(messageData['From']):
            messageData['From'] = messageData['From'][0]
        else:
            messageData['From'] = {"name": "", "email": ""}
        messageData['From']['name'] = messageData['From']['name']
        messageData['From']['email'] = messageData['From']['email']

        # If message is not already present in db, save it
        dest_names = ",".join([e['name'].replace(",","\,") for e in messageData['To']])
        dest_emails = ",".join([e['email'].replace(",","\,") for e in messageData['To']])
        labelNames = ",".join([d_labels.get(e, "") for e in messageData['labelIds']])
        attachmentsList = ",".join(messageData['attachmentsList'])
        labelIds = ",".join(messageData['labelIds'])

        data = (
            messageData['id'],
            messageData['threadId'],
            messageData['From']['name'],
            messageData['From']['email'],
            dest_names,
            dest_emails,
            json.dumps(messageData['To']),
            labelIds,
            labelNames,
            messageData['epoch_ts'],
            messageData['date'],
            messageData['Subject'],
            messageData['body'],
            messageData['sizeEstimate'],
            attachmentsList
        )
        try:
            _fCur.execute(f"INSERT INTO `{userEmail}_messages` (\
                id,\
                threadId,\
                exp_name,\
                exp_email,\
                dest_names,\
                dest_emails,\
                dest_json,\
                labelIds,\
                labelNames,\
                date_ts,\
                date,\
                subject,\
                body,\
                size,\
                attachmentsIds\
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
        except:
            print(sys.exc_info(), end="\n\n")
            print(data, len(data))
            exit()

        temp_dbFinal.commit()

    cur.close()
    f.close()

    _fCur.close()
    temp_dbFinal.close()

    fCur.close()
    finalData.close()

    # while attachmentsDownload__threads:
    while attachmentsDownload__threads__v2[_threadId]:
        attachmentsDownload__threads__v2[_threadId].popitem()[1].join()
        # attachmentsDownload__threads.popitem()[1].join()

    return

def _saveAttachment(gmailUtils, userId, messageId, attachmentId, filepath):
    service = build('gmail', 'v1', credentials=gmailUtils._creds)
    try:
        attachment = service\
            .users()\
            .messages()\
            .attachments()\
            .get(userId=userId, messageId=messageId, id=attachmentId).execute()
        file_data = base64.urlsafe_b64decode(attachment['data']
                                    .encode('UTF-8'))
    except:
        print("[ ERROR ] [ _saveAttachment ] ", userId, messageId, attachmentId, filepath, end="\n")
        file_data = b"not_found"
        attachment = None

    f = open(filepath, 'wb')
    f.write(file_data)
    f.close()

    del service
    del attachment
    del file_data
    try:
        del attachmentsDownload__threads[attachmentId];
    except:# Key has already been deleted by while() loop
        pass

def _saveAttachment__v2(_parent_threadId, gmailUtils, userId, messageId, attachmentId, filepath):
    service = build('gmail', 'v1', credentials=gmailUtils._creds)
    try:
        attachment = service\
            .users()\
            .messages()\
            .attachments()\
            .get(userId=userId, messageId=messageId, id=attachmentId).execute()
        file_data = base64.urlsafe_b64decode(attachment['data']
                                    .encode('UTF-8'))
    except:
        print("[ ERROR ] [ _saveAttachment ] ", userId, messageId, attachmentId, filepath, end="\n")
        file_data = b"not_found"
        attachment = None

    f = open(filepath, 'wb')
    f.write(file_data)
    f.close()

    del service
    del attachment
    del file_data
    try:
        del attachmentsDownload__threads__v2[_parent_threadId][attachmentId];
    except:# Key has already been deleted by while() loop
        pass


def generateExcel(userEmail=None, speedtests=None):
    f = sqlite3.connect(finalDataDB)
    cur = f.cursor()
    cur2 = f.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tbls = cur.fetchall()

    if not userEmail:
        userEmails = {}
        for tbl in tbls:
            userEmail = "_".join(tbl[0].split("_")[:-1])
            if not userEmail in userEmails:
                userEmails[userEmail] = None

        print("Please choose user email :")
        choices = {}
        count = 1
        for elt in userEmails.items():
            tblName = elt[0]
            print(f"# {count}) {tblName}")
            choices[count] = tblName
            count += 1
        print()
        if not choices:
            return

        userEmail = choices[int(input("Table index ? "))]
    userEmail_cleanForFiles = userEmail.replace("@", "_at_")
    
    global base_attachments_dir
    if not userEmail_cleanForFiles in base_attachments_dir:
        base_attachments_dir += "/" + userEmail_cleanForFiles

    messagesTbl = userEmail + "_messages"
    attachmentsTbl = userEmail + "_attachments"
    labelsTbl = userEmail + "_labels"
    baseFinalFilename = genDT() + "--"+userEmail_cleanForFiles+"--GmailMessagesList"
    xlsxFilename = baseFinalFilename + ".xlsx"
    zipFilename = baseFinalFilename + ".zip"

    # Init Excel workbook
    wb = xlsxwriter.Workbook(xlsxFilename)

    # Init Labels sheets
    rowId = 0
    sheet = wb.add_worksheet("Labels")
    sheet.write(rowId, 0, "labelId")
    sheet.write(rowId, 1, "labelName")

    # Get labels and parse them
    d_labels = {}
    labels = cur.execute(f"SELECT * FROM `{labelsTbl}`").fetchall()
    for label in labels:
        rowId += 1
        d_labels[label[0]] = label[1]
        sheet.write(rowId, 0, label[0])
        sheet.write(rowId, 1, label[1])

    startTime = time.time()

    # Init Excel sheet for messages
    rowId = 0
    sheet = wb.add_worksheet("Mails")
    sheet.write(rowId, 0, "id")
    sheet.write(rowId, 1, "threadId")
    sheet.write(rowId, 2, "exp_name")
    sheet.write(rowId, 3, "exp_email")
    sheet.write(rowId, 4, "dest_names")
    sheet.write(rowId, 5, "dest_emails")
    sheet.write(rowId, 6, "labelNames")
    sheet.write(rowId, 7, "date")
    sheet.write(rowId, 8, "subject")
    sheet.write(rowId, 9, "size")
    sheet.write(rowId, 10, "attachments count")
    sheet.write(rowId, 11, "attachments list (this col and next ones)")

    cur.execute(f"SELECT * FROM `{messagesTbl}`")
    for message in cur:
        print(f"[ step3.1 ] Processing line {rowId} for saving into Excel...")
        rowId += 1
        dests = json.loads(message[6])
        labels = []
        for labelId in message[7].split(","):
            labelId = labelId.strip()
            if labelId in d_labels:
                labels.append(d_labels[labelId])
        attachmentsIds = message[15].split(",")
        attachmentsFilenames = []
        for attachmentId in attachmentsIds:
            q = cur2.execute(f"SELECT * FROM `{attachmentsTbl}` WHERE id = '{attachmentId}'").fetchall()
            if len(q) <= 0:
                continue
            q = q[0]
            attachmentsFilenames.append("-".join([message[0], q[1], q[2]]))

        data = (
            message[0],# id
            message[1],# thread id
            message[2],# exp name
            message[3],# exp email
            ",".join([dest["name"] for dest in dests]),# dest names
            ",".join([dest["email"] for dest in dests]),# dest emails
            ",".join(labels),# label names
            message[11],# date
            message[12],# subject
            message[14],# email size
            len(attachmentsFilenames)# attachments count
        )
        for i in range(11):
            sheet.write(rowId, i, data[i])
        clnI = 11
        for i in range(len(attachmentsFilenames)):
            sheet.write(rowId, clnI, attachmentsFilenames[i])
            clnI += 1

    try:
        wb.close()
    except xlsxwriter.exceptions.FileCreateError:
        retry = input("File seems to be opened. Retry ? [Y/n] ").strip().lower()
        if retry == "y" or retry == "o" or retry == "":
            wb.close()

    # Make a copy of final DB
    newDb = "./._"+str(random.randint(10000, 99999))+".db"
    f_newDb = sqlite3.connect(newDb)
    c_newDb = f_newDb.cursor()

    for tbl in [messagesTbl, attachmentsTbl, labelsTbl]:
        tbl_format = cur.execute(f"SELECT * FROM sqlite_master WHERE name='{tbl}';").fetchall()[0][4]
        c_newDb.execute(tbl_format)
        insert_str = ",".join(["?" for i in range(len(cur.execute(f"SELECT * FROM `{tbl}` LIMIT 1").fetchall()[0]))])
        cur.execute(f"SELECT * FROM `{tbl}`")
        for l in cur:
            c_newDb.execute(f"INSERT INTO `{tbl}` VALUES ({insert_str})", l)
    
    c_newDb.close()
    f_newDb.commit()
    f_newDb.close()

    cur.close()
    cur2.close()
    f.close()

    # Generate ZIP file
    file_paths = []
    for root, directories, files in os.walk(base_attachments_dir):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)
    os.replace(newDb, "WholeData.db")
    with ZipFile(zipFilename,'w') as zip:
        for file in file_paths:
            zip.write(file)
        zip.write(os.path.join("./", xlsxFilename))
        zip.write(os.path.join("./", "WholeData.db"))
    os.remove(os.path.join("./", xlsxFilename))
    os.remove(os.path.join("./", "WholeData.db"))

    if type(speedtests) == list:
        speed = time.time() - startTime
        speedtests.append({"user": userEmail, "step": "step3", "time": speed, "description": "Generate excel, then Zip file containing all data"})

    return zipFilename

gPort = 0
gTokenFile = "token.pickle"
gThreadsCount = None
gGoogleApiCredentials = None
gGoogleConsoleAuth = False
actions = [
    "step1",
    "step2",
    "step3",
    "auth",
    "run",
    "clean",
    "count-mails-in-label",
    "delete-from-label--offline",
    "delete-from-label",
    "delete-from-list",
    "labels-usage-count"]

def genIncorrectAction(action):
    print("Incorrect action. Please use")
    print(" python3 GmailTools.py <action>")
    print("\nAvailable actions :", ",".join(actions))
    print("\nSteps are :")
    print(" - step1: Download messages IDs, then message data, and save them into temp DB.")
    print(" - step2: For each message retrieved from step1, download attachments and save message data + attachments path in Final Database")
    print(" - step3: Load data saved in Final Databse and add it Excel file. Finally, compress whole data in ZIP file.")
    print("\n`run` option runs all three steps mentionned above.\n")
    print("\n`auth` option authenticates user (generates token file) and displays informations on Gmail account.\n")
    print("\n`clean` option removed all files for logged user.\n")
    print("\nOptional parameters :")
    print(" - p (int) : Port on which script will listen for authenticating user on Google OAuth")
    print(" - y : If specified, no prompt will be displayed [still implementing........]")
    print(" - tokenFile : File in which user token will be stored")
    print(" - threads (int) : Max threads count")
    print()
def genCustomHelp():
    s = []
    def print(*args):
        ss = []
        for arg in args:
            ss.append(arg)
        s.append(ss)
    print("\nAvailable actions :", ",".join(actions))
    print("\nSteps are :")
    print(" - step1: Download messages IDs, then message data, and save them into temp DB.")
    print(" - step2: For each message retrieved from step1, download attachments and save message data + attachments path in Final Database")
    print(" - step3: Load data saved in Final Databse and add it Excel file. Finally, compress whole data in ZIP file.")
    print("\n`run` option runs all three steps mentionned above.")
    print("\n`auth` option authenticates user (generates token file) and displays informations on Gmail account.")
    print("\n`clean` option removes all files for logged user.")
    print("\n`labels-usage-count` option displays a table showing number of mails per label.")
    print("\n`count-mails-in-label` option returns number of mails in specified labels (intersection).")
    print("\n`delete-from-label--offline` option permanently deletes all mails within specified labels (--labels-ids) (intersection) using previous exported data.")
    print("\n`delete-from-label` option permanently deletes all mails within specified labels (--labels-ids) (intersection), using latest Gmail data..")
    print("\tExample : --labels-ids=Label1,Label2 will delete all mails that are both in labels 1 and 2.")
    print("\n`delete-from-list` option permanently deletes all mails from IDs list (--messages-ids).")
    print("\n")

    return "\n".join([" ".join(e) for e in s])

def genParametersList(parms, beginPos=2):
    """Retrieves message details from id

    Args:
        service: Gmail API service
        id: Gmail Message ID

    Returns:
        Mail data from Gmail API
    """
    pass_next = False
    p_next = None
    for i in range(beginPos, len(sys.argv)):
        p = sys.argv[i].strip()
        if not pass_next:
            if "=" in p:
                if p[0] == "-":
                    if len(p) <= 1:
                        continue
                    p = p[1:]
                p = p.split("=")
                if p[0] == "consoleAuth":
                    parms["consoleAuth"] = True
                else:
                    parms[p[0]] = p[1]
            elif p[0] == "-" and len(p) > 1:
                if p == "-y" or p == "-consoleAuth":
                    parms[p[1:]] = True
                else:
                    pass_next = True
                    p_next = p[1:]
        else:
            parms[p_next] = p
            p_next = None
            pass_next = False

def genParametersList__v2(parms):
    import argparse
    import sys as _sys
    def print_help(self, file=None):
        if file is None:
            file = _sys.stdout
        self._print_message(self.format_help(), file)
        self._print_message(genCustomHelp(), file)
    argparse.ArgumentParser.print_help = print_help
    parser = argparse.ArgumentParser(add_help=True)

    parser.add_argument("-a", '--action', \
        help=f'Action to run', \
        required=True, \
        choices=actions)

    parser.add_argument('-p',  "--port", \
        type=int, \
        dest="port", \
        required=False, \
        help='Port on which program should listen for Gmail auth')

    parser.add_argument('-tf',  "--token-file", \
        dest="tokenFile", \
        type=str, \
        required=False, \
        help='Name of file in which Gmail token is stored')
    parser.add_argument('-gac',  "--google-api-credentials", \
        dest="googleApiCredentials", \
        type=str, \
        required=False, \
        # default="credentials.json", \
        help='Gmail Developer credentials (JSON string)')

    parser.add_argument('-mids',  "--messages-ids", \
        dest="messagesIds", \
        type=str, \
        required=False, \
        default="",
        help='List of messages IDs')

    parser.add_argument('-lids',  "--labels-ids", \
        dest="labelsIds", \
        type=str, \
        required=False, \
        default="",
        help='List of labels IDs')

    parser.add_argument('-th',  "--threads", \
        dest="threads", \
        type=int, \
        required=False, \
        help='Number of threads to use for processing data')

    parser.add_argument("-y", "--yes", dest="yes", action='store_true', help="Don't prompt confirmation")
    parser.add_argument("-ca", "--console-auth", dest="consoleAuth", action='store_true', help="Do Gmail auth in console mode")

    args = parser.parse_args()
    args.labelsIds = [e for e in args.labelsIds.split(",") if e]
    args.messagesIds = [e for e in args.messagesIds.split(",") if e]

    # Check values
    if args.action == "delete-from-label" or args.action == "delete-from-label--offline":
        if not args.labelsIds:
            # print("You must specify label(s) ID(s) with --labels-ids.")
            parser.error("--action=delete-from-label[--offline] requires --labels-ids")
            exit(-1)
    elif args.action == "delete-from-list":
        if not args.messagesIds:
            # print("You must specify message(s) ID(s) with --messages-ids.")
            parser.error("--action=delete-from-list requires --messages-ids")
            exit(-1)

    for arg in vars(args):
        argVal = getattr(args, arg)
        if argVal != None:
            parms[arg] = argVal

    print(parms)

def main():
    parms = {}
    genParametersList__v2(parms)

    if "port" in parms:
        global gPort; gPort = int(parms["port"])
    if "tokenFile" in parms:
        global gTokenFile; gTokenFile = parms["tokenFile"]
    if "threads" in parms:
        global gThreadsCount
        try: gThreadsCount = int(parms["threads"])
        except: print("Threads count must be valid int"); exit()
    if "googleApiCredentials" in parms or "google-api-credentials" in parms:
        global gGoogleApiCredentials;
        if "googleApiCredentials" in parms:
            gGoogleApiCredentials = parms["googleApiCredentials"]
        else:
            gGoogleApiCredentials = parms["google-api-credentials"]
    if "consoleAuth" in parms and parms["consoleAuth"]:
        global gGoogleConsoleAuth
        gGoogleConsoleAuth = True

    speedtests = []

    # p = sys.argv[1].lower()
    p = parms["action"]
    if p == "run" \
        or p == "step1" \
        or p == "step2" \
        or p == "step3" \
        or p == "auth" \
        or p == "clean" \
        or p == "labels-usage-count" \
        or p == "count-mails-in-label" \
        or p == "delete-from-label--offline" \
        or p == "delete-from-label" \
        or p == "delete-from-list":
        userActions = UserActions(
            parms=parms,
            authUser=True,
            speedtests=speedtests,
            printUserInfo=True)
        action = Tools.SnakeToCamel(p)
        func = getattr(userActions, action)
        func()

    else:
        exit()

    if speedtests:
        # print(speedtests)
        print()
        total = 0
        for speedtest in speedtests:
            total += speedtest['time']
            print(f"[ runtime ] [ user = {speedtest['user']} ] [ time = ({speedtest['time']}s) {Tools.HumanReadableTime(speedtest['time'])} ] [ {speedtest['step']} ] {speedtest['description']}")
        print(f"[ runtime ] [ TOTAL ] [ time = ({total}s) {Tools.HumanReadableTime(total)} ]")

class UserActions:
    def __init__(self, parms, authUser=True, speedtests=[], printUserInfo=True, verbose=True):
        self.speedtests = speedtests;
        self.parms = parms
        self.verbose = verbose

        self.gmailUtils = GmailUtils(
            port=gPort,
            tokenFile=gTokenFile,
            speedtests=self.speedtests,
            googleAPICredentials=gGoogleApiCredentials,
            googleConsoleAuth=gGoogleConsoleAuth)
        if authUser:
            self.gmailUtils.auth(printUserInfo=printUserInfo)

    def Auth(self):
        pass

    def Run(self):
        if (not "yes" in self.parms or not self.parms["yes"]) \
            and not Tools.confirm(f"You are going to load all emails from {self.gmailUtils._userEmail}. Continue ?"):
            print("Exiting...")
            exit(4)

        # zipFilename = quickRun(gmailUtils, speedtests)[1]
        zipFilename = self.gmailUtils.Run(cleanDB=True, cleanAttachmentFiles=True, confirm=False)[1]

        print(f"\nFinished ! You can find your archive under the name \"{zipFilename}\".")

    def Step1(self):
        print("==== Loading mails IDs & data... ====", end="\n\n")
        step1(gmailUtils=self.gmailUtils)

    def Step2(self):
        print("==== Saving clean data in final database... ====", end="\n\n")
        generateCleanMessagesDataAndDownloadAttachments__v2(gmailUtils=self.gmailUtils, speedtests=self.speedtests)

    def Step3(self):
        print("==== Generating Excel & Zip files ... ====", end="\n\n")
        generateExcel(speedtests=self.speedtests)

    def Clean(self):
        return self.gmailUtils.Clean(confirm=True)

    def LabelsUsageCount(self):
        if not self.gmailUtils.FinalDataExists() or (not self.parms["yes"] and Tools.confirm("You are going to use existing mails data. Do you want to retrieve latest data ?")):
            self.gmailUtils.Run(cleanDB=True, cleanAttachmentFiles=True, confirm=False)

        labelsCounts = self.gmailUtils.GenerateLabelsMailsCount()
        for label in labelsCounts:
            labelName, count = labelsCounts[label]
            print(label, labelName, count, sep="\t\t\t\t")

    def DeleteFromLabel_Offline(self):
        if not self.gmailUtils.FinalDataExists():
            self.gmailUtils.Run(cleanDB=True, cleanAttachmentFiles=True, confirm=False)
        
        is_del = self.gmailUtils.Delete_FromLabel__offline(self.parms["labelsIds"], force=False)
        if is_del:
            print(f"{is_del} messages have been succesfully deleted !")
        else:
            print("No message was deleted !")

    def DeleteFromList(self):
        is_del = self.gmailUtils.Delete_FromList(self.parms["messagesIds"], force=False)
        if is_del:
            print(f"{is_del} messages have been succesfully deleted !")
        else:
            print("No message has been deleted !")

    def CountMailsInLabel(self):
        cnt = len(self.gmailUtils._listMessagesIDs__v2(saveInDB=False, label_ids=self.parms["labelsIds"], includeSpamTrash=True))

        print(cnt, f"mails in [{' ∩ '.join(labels)}]")

    def DeleteFromLabel(self):
        self.gmailUtils.Delete_FromLabel(labelIds=self.parms["labelsIds"], force=False)

if __name__ == '__main__':
    main()
