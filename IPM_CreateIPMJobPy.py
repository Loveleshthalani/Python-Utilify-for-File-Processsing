#Import libraries
try:
    import logging
    import shutil
    from pathlib import Path
    import pathlib
    import os
    import datetime
    import configparser
    import pyodbc # type: ignore
    import os.path
    import sys
    import inspect
    import shutil
    import ctypes
    from logging.handlers import TimedRotatingFileHandler
    # package should be install, if not [Command: pip install package name]
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)
# ******************************************************************************************

##Change Command Window Title
ctypes.windll.kernel32.SetConsoleTitleW("MasterCard IPM Validation Script")

# Initialize variables
global InputDir, OutputDir, ErrorDir, FileCount, DoneFileCount, NewInputDir, IPMFileSource, ErrorReason, LogDir, InputFileName

DB_Server_NAME = DBName_CI = log_file_name = FileStatus = ErrorReason = file_name = FileName = InputFileName = file_list = ""
bStopProcessing = bFinalMessage = False
IPMFileSource = "MASTERCARDIPM"
FileCount = MultiPODEnabled = 0
objCon = None

######################################################################################################################################################

def GetObjectName():
    # Purpose: This function returns calling object name
    return inspect.stack()[1][3]

######################################################################################################################################################

def get_file_handler(LOG_FILE):
   file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
   file_handler.setFormatter(FORMATTER)
   return file_handler

######################################################################################################################################################

def get_logger(logger_name):
   logger = logging.getLogger(logger_name)
   logger.setLevel(logging.DEBUG)
   # Imp do not remove below commented code
   #logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler(logger_name))
   logger.propagate = False
   return logger

######################################################################################################################################################

def udfCreateDatabaseConnection(cdc_server_name, cdc_db_name):
    MessageLogger.info(f"CALL: Function - {GetObjectName()}")
    # Purpose: This returns connection object to connect SQL Server
    try:
        DBConnectionStr = 'Driver={'+str(SqlOdbcDriver)+'};Server='+cdc_server_name+';Database='+cdc_db_name+';Trusted_Connection=yes;MultiSubnetFailover=Yes'
        MessageLogger.info(f"DBConnectionStr = {DBConnectionStr}")
        cdc_conn = pyodbc.connect(DBConnectionStr)
        return cdc_conn
    
    except pyodbc.Error as err_cdc:
        print(f"ERROR: {err_cdc}")

# ******************************************************************************************

def is_eof(f):
    MessageLogger.info(f"CALL: Function - {GetObjectName()}")
    cur = f.tell()    # save current position,  tell() method can be used to get the position of File Handle
    f.seek(0, os.SEEK_END)
    end = f.tell()    # find the size of file
    f.seek(cur, os.SEEK_SET), 
    #seek() function is used to change the position of the File Handle to a given specific position.
    #os.SEEK_SET or 0 to set the reference point to the beginning of the file
    #os.SEEK_CUR or 1 to set the reference point to the current position
    #os.SEEK_END or 2 to set the reference point to the end of the file.
    return cur == end

# ******************************************************************************************

# Select Queries For Different purpose Return different values according to i/p prameter
def SQLSelectQueries(Sel_qry, Arg_Var1 = '', Arg_Var2 = ''):
    
    MessageLogger.info(f"CALL: Function - {GetObjectName()}")
    
    if Sel_qry == 0:
        qry = f""
    
    elif Sel_qry == 1:
        qry = f"SELECT COUNT(1) FROM ClearingFiles WITH(NOLOCK) WHERE FileStatus = 'DONE' AND FileExtension = '{Arg_Var1}' AND FileSource = 'MASTERCARDIPM' AND FileId LIKE '%{Arg_Var2}%'"
        
    elif Sel_qry == 2:
        qry = "SELECT ISNULL(MAX(JobId),0) FROM ClearingFiles WITH(NOLOCK)"
    
    elif Sel_qry == 3:
        qry = f"SELECT CONVERT(DATE,(MIN(ProcDayEnd))) FROM ARSystemAccounts WITH(NOLOCK)"
    
    elif Sel_qry == 4:
        qry = "SELECT CONVERT(DATE,(GETDATE()))"
    
    elif Sel_qry == 5:
        qry = f"SELECT COUNT(1) FROM ClearingFiles WITH(NOLOCK) WHERE FileStatus = 'READY' AND FileId LIKE '%{Arg_Var1}' AND FileSource = 'MASTERCARDIPM'"
    
    elif Sel_qry == 6:
        qry = f"SELECT JobId FROM ClearingFiles WITH(NOLOCK) WHERE FileStatus = 'READY' AND FileId LIKE '%{Arg_Var1}' AND FileSource = 'MASTERCARDIPM'"
    
    else:
        MessageLogger.debug("Incorrect Parameter Supplied for function SQLSelectQueries")
        sys.exit("Incorrect Parameter Supplied for function SQLSelectQueries")
            
    MessageLogger.debug(f"Sql Query  : {qry}")
    try:
        objCon = udfCreateDatabaseConnection(DB_Server_NAME, DBName_CI)
        objCursor = objCon.cursor()
        objCursor.execute(qry)
        qry_result = objCursor.fetchall()
        objCursor.close()
        
    except Exception as e:
            MessageLogger.error(f"Error Raised from Function SQLSelectQueries : {e}")
            sys.exit()
    
    if qry_result is None or not qry_result :
        MessageLogger.debug("QueryResultError : Above Query Doesnot Return Expected Result")
        print("QueryResultError : Above Query Doesnot Return Expected Result")
        sys.exit()
        
    else:
        try:
            if Sel_qry in [0,1,2,5,6]:
                SingleRes = qry_result[-1][-1]
                MessageLogger.info(f"SingleRes = {SingleRes}")
                return SingleRes

            elif Sel_qry in [3,4]:
                    SQLDateTime = str(qry_result[-1][-1]).strip()
                    MessageLogger.info(f"SQLDateTime = {SQLDateTime}")
                    return SQLDateTime
            else:
                pass
                
        except Exception as e:
            MessageLogger.error(f"Error Raised from Function SQLSelectQueries : {e}")
            sys.exit()

######################################################################################################################################################

#SUBROUTINE FOR WRITTING THE TRIGGER RECORD FOR IPM Recon file
def InsertIntoClearingFiles_Table ():
    MessageLogger.info(f"CALL: Function - {GetObjectName()}")
    global DoneFileCount, NewInputDir, FileCount

    iJobId = MaxJobIdFromDB = 0

    MessageLogger.debug (f"Input Filename is : {InputFileName}")

    FileNameId = InputFileName

    FileNameId = f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{FileNameId}"
    MessageLogger.debug (f"File Name after = {FileNameId}")
        
    FilePath = OutputDir + fl_Name
    
    IncomingFileExtension = os.path.splitext(FileNameId)[1][1:]
    MessageLogger.debug (f"IncomingFileExtension = {IncomingFileExtension}")

    CurrentTime = datetime.datetime.now().strftime("%H:%M:%S.%f")[:12]
    Date_Received = f"{SQLSelectQueries(3)} {CurrentTime}"
    MessageLogger.info(f"Date_Received For INSERT INTO ClearingFiles = {Date_Received}")
    
    if IsSystemDateTimeEnabled_IPM == 1:
        SystemLastDateTime = f"{SQLSelectQueries(4)} {CurrentTime}"
        MessageLogger.info(f"SystemLastDateTime For INSERT INTO ClearingFiles = {SystemLastDateTime}")
	
    FileDateFromFile = ""
    
    try:
        if len(fl_Name) >= 43:
            DatePart = fl_Name[24:30]
            TimePart = fl_Name[32:38]
            FileDateFromFile = datetime.datetime.strptime(DatePart+TimePart,'%y%m%d%H%M%S')
            MessageLogger.debug(f"FileDateFromFile = {FileDateFromFile}")
        else:
            MessageLogger.debug("File does not have Standard Name, FileDate would be assigned as NULL into DB")
    
    except Exception as e:
        MessageLogger.debug(f"Invalid DateTime Formart at position 25-30 and/or 33-38 {e}")
        sys.exit()
        
    MessageLogger.debug(f"Length of FileDateFromFile variable = {len(str(FileDateFromFile))}")
    
    #Checking Whether IPM record is already inserted with FileStatus = 'READY'
    ReadyRecCnt = SQLSelectQueries(5,InputFileName)

    if ReadyRecCnt > 0:
        JobIdReadyRec = SQLSelectQueries(6,InputFileName)

        if len(str(FileDateFromFile)) > 0:
            if IsSystemDateTimeEnabled_IPM == 0:
                SQLQuery_ClrFile = f"UPDATE ClearingFiles SET FileStatus = 'InQueue', ErrorReason = '{ErrorReason}', FileDate = '{FileDateFromFile}', Path_FileName = '{FilePath}' WHERE JobId = {JobIdReadyRec}"
            else:
                SQLQuery_ClrFile = f"UPDATE ClearingFiles SET FileStatus = 'InQueue', ErrorReason = '{ErrorReason}', FileDate = '{FileDateFromFile}', Path_FileName = '{FilePath}', SystemLastDateTime = '{SystemLastDateTime}' WHERE JobId = {JobIdReadyRec}"
        else:
            if IsSystemDateTimeEnabled_IPM == 0:
                SQLQuery_ClrFile = f"UPDATE ClearingFiles SET FileStatus = 'InQueue', ErrorReason = '{ErrorReason}', Path_FileName = '{FilePath}' WHERE JobId = {JobIdReadyRec}"
            else:
                SQLQuery_ClrFile = f"UPDATE ClearingFiles SET FileStatus = 'InQueue', ErrorReason = '{ErrorReason}', Path_FileName = '{FilePath}', SystemLastDateTime = '{SystemLastDateTime}' WHERE JobId = {JobIdReadyRec}"
                
        iJobId = JobIdReadyRec
    
    else:
        MessageLogger.debug("About to Insert Record into ClearingFiles")
        FileIdColumnData = f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{fl_Name}"
        MessageLogger.debug(f"FileId : {FileIdColumnData}")
        
        MaxJobIdFromDB = SQLSelectQueries(2)
        MessageLogger.info(f"MaxJobIdFromDB From ClearingFiles Table = {MaxJobIdFromDB}")

        if MaxJobIdFromDB == 0:
            MessageLogger.debug ("MaxJobIdFromDB Is NULL In ClearingFiles table")
            iJobId = 100 
        else:
            iJobId = int(MaxJobIdFromDB) + 1
        
        MessageLogger.debug(f"Finally iJobId = {iJobId}")

        
        if len(str(FileDateFromFile)) > 0:
            if IsSystemDateTimeEnabled_IPM == 0:
                SQLQuery_ClrFile = "INSERT INTO ClearingFiles(FileId, Path_FileName, FileStatus, Date_Received, FileSource, ErrorReason, FileDate, Jobid, QMsgCount, Retry, FileExtension, ProcessSleepIntrvl)"
                SQLQuery_ClrFile = f"{SQLQuery_ClrFile} VALUES ( '{FileIdColumnData}', '{FilePath}', 'InQueue', '{Date_Received}', '{IPMFileSource}', '{ErrorReason}', '{FileDateFromFile}', {iJobId}, 0, 0, '{IncomingFileExtension}', 30)"
            else:
                SQLQuery_ClrFile = "INSERT INTO ClearingFiles(FileId, Path_FileName, FileStatus, Date_Received, FileSource, ErrorReason, FileDate, Jobid, QMsgCount, Retry, FileExtension, ProcessSleepIntrvl, SystemLastDateTime)"
                SQLQuery_ClrFile = f"{SQLQuery_ClrFile} VALUES ( '{FileIdColumnData}', '{FilePath}', 'InQueue', '{Date_Received}', '{IPMFileSource}', '{ErrorReason}', '{FileDateFromFile}', {iJobId}, 0, 0, '{IncomingFileExtension}', 30,'{SystemLastDateTime}')"	
        else:
            if IsSystemDateTimeEnabled_IPM == 0:
                SQLQuery_ClrFile = "INSERT INTO ClearingFiles(FileId, Path_FileName, FileStatus, Date_Received, FileSource, ErrorReason, Jobid, QMsgCount, Retry, FileExtension, ProcessSleepIntrvl)"
                SQLQuery_ClrFile = f"{SQLQuery_ClrFile} VALUES ( '{FileIdColumnData}', '{FilePath}', 'InQueue', '{Date_Received}', '{IPMFileSource}', '{ErrorReason}', {iJobId}, 0, 0, '{IncomingFileExtension}', 30)"
            else:
                SQLQuery_ClrFile = "INSERT INTO ClearingFiles(FileId, Path_FileName, FileStatus, Date_Received, FileSource, ErrorReason, Jobid, QMsgCount, Retry, FileExtension, ProcessSleepIntrvl,SystemLastDateTime)"
                SQLQuery_ClrFile = f"{SQLQuery_ClrFile} VALUES ( '{FileIdColumnData}', '{FilePath}', 'InQueue', '{Date_Received}', '{IPMFileSource}', '{ErrorReason}', {iJobId}, 0, 0, '{IncomingFileExtension}', 30,'{SystemLastDateTime}')"
                
    MessageLogger.debug(f"SQL Query Update or Insert In ClearingFiles = {SQLQuery_ClrFile}")
    
    try:
        objCon = udfCreateDatabaseConnection(DB_Server_NAME, DBName_CI)
        objCursor = objCon.cursor()
        objCursor.execute(SQLQuery_ClrFile)
        objCursor.commit()
        objCursor.close()
        
    except Exception as e:
        MessageLogger.debug(f"Insert or Update In ClearingFiles Failed : {e}")
        print(f"Insert or Update In ClearingFiles Failed : {e}")
        sys.exit()
    
    MessageLogger.debug ("Clearing File Job Inserted/Modified")

    MessageLogger.debug (f"Before Delete TEMP Folder Inside FileIn Folder = {NewInputDir}")
    DoneFileCount = DoneFileCount + 1
    MessageLogger.debug(f"DoneFileCount = {DoneFileCount}, FileCount = {FileCount}")

    if (DoneFileCount == FileCount):
        MessageLogger.debug(f"Going to Delete temporary Folder {NewInputDir}")
        DeleteFolder(NewInputDir)

######################################################################################################################################################

def DeleteFolder(strFolderPath):
    MessageLogger.info(f"CALL: Function - {GetObjectName()}")
    FolderLocation = pathlib.Path(os.path.split(strFolderPath)[0])
    if FolderLocation.exists():
        MessageLogger.debug (f"About to delete file and folder : {FolderLocation}")
        shutil.rmtree(strFolderPath)
    else:
        MessageLogger.debug (f"{FolderLocation} Folder does not exist, Proceed Further")
        
######################################################################################################################################################

def fctHandleAtTheRateChar(MainInputString):
    MessageLogger.info(f"CALL: Function - {GetObjectName()}")
    InputString = MainInputString
    InputString1 = MainInputString
    StringLength = len(InputString)
    InputString1 = InputString1.replace(b'\x40', b'')

    if len(InputString1) > 0:
        LastChar = InputString[(StringLength-1):StringLength]
        if LastChar == b'\x40':
            while StringLength > 0:
                InputString = InputString[0:(StringLength-1)]
                StringLength = len(InputString)
                LastChar = InputString[(StringLength-1):StringLength]
                if LastChar != b'\x40':
                    if (StringLength-1)<=0:
                        MessageLogger.debug (f"1 StringLength = {StringLength}")

                    if (StringLength-1) > 0:
                        InputString = InputString[0:(StringLength-1)]
                    else: 
                        InputString = b''
                    
                    if (StringLength-4)<=0:
                        MessageLogger.debug (f"2 StringLength = {StringLength}")

                    if (StringLength-4) > 0:
                        InputString = InputString[0:(StringLength-4)]
                    else: 
                        InputString = b''

                    ResultString = InputString
                    break
        else:
            ResultString = InputString

        fctHandleAtTheRateChar = ResultString
    else:
        fctHandleAtTheRateChar = InputString1
    return fctHandleAtTheRateChar
######################################################################################################################################################

# Setting Config File, Required Eenvironment Variables should present into config file
config_file_name = "SetupCIPy.ini"

config_file_name_path = os.path.abspath((os.path.join(os.getcwd(), config_file_name)))
# check config file
if not os.path.exists(config_file_name):
    print(f"ERROR : {config_file_name} not found")
    bStopProcessing = True
    print('os.path.exists(config_file_name) = ',os.path.exists(config_file_name))

config = configparser.ConfigParser()
config.read(config_file_name_path)

# Fetch and Assign Eenvironment Variables from Config File
if not bStopProcessing:
    
    DB_Server_NAME                  = (config.get('DEFAULT', "DB_Server_NAME", fallback=-1))
    DBName_CI                       = (config.get('DEFAULT', "DBName_CI", fallback=-1))
    SqlOdbcDriver                   = (config.get('DEFAULT', "SqlOdbcDriver", fallback=-1))
    InputDir                        = (config.get('DEFAULT', "IPMFileIN", fallback=-1))
    OutputDir                       = (config.get('DEFAULT', "IPMFileOUT", fallback=-1))
    ErrorDir                        = (config.get('DEFAULT', "IPM_ERROR", fallback=-1))
    LogDir                          = (config.get('DEFAULT', "IPM_LOG", fallback=-1))
    MultiPODEnabled                 = (config.get('DEFAULT', "MultiPODEnabled_IPM", fallback=-1))
    IsSystemDateTimeEnabled_IPM     = (config.get('DEFAULT', "IsSystemDateTimeEnabled_IPM", fallback=-1))

    MultiPODEnabled             = int(MultiPODEnabled)
    IsSystemDateTimeEnabled_IPM = int(IsSystemDateTimeEnabled_IPM)

    LogDir = LogDir.strip()

    # Creating Log File Path and Name
    FORMATTER = logging.Formatter("%(asctime)s — %(thread)s — %(levelname)s — %(message)s", datefmt='%m/%d/%Y %H:%M:%S')
    log_file_name = f"{LogDir}/IPM_{datetime.date.today().strftime('%m%d%Y')}.log"
    MessageLogger = get_logger(log_file_name)

    MessageLogger.debug ("*************************** IPM File Processing Starts ***************************")
    print("*************************** IPM File Processing Starts ***************************")
    
    InputDir = InputDir.strip() + "\\"
    OutputDir = OutputDir.strip() + "\\"
    ErrorDir = ErrorDir.strip() + "\\"

    Check_EnvVar = ['InputDir', 'OutputDir', 'ErrorDir']
    
    for EnvVar in Check_EnvVar:
        if globals()[EnvVar] in ["","\\"]:
            MessageLogger.debug(f"Environment variable {EnvVar} is not set. Aborting ...")
            sys.exit()
        
    NewInputDir = CurrentDateTime = ""
    DoneFileCount = 0
    MessageLogger.debug ("Initially Input Folder = " + InputDir)

    CurrentDateTime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    NewInputDir = InputDir + CurrentDateTime
    MessageLogger.debug ("New Input Folder Name which is to be created = " + NewInputDir)
        
    path = Path(NewInputDir)
    path.mkdir(parents=True, exist_ok=True)
    NewInputDir = NewInputDir + "\\"
    MessageLogger.debug ("A new Interim folder has been created at: " + NewInputDir)
    
    file_list = [name for name in os.listdir(InputDir) if os.path.isfile(os.path.join(InputDir, name))]
    file_list.sort(key=lambda s: os.path.getmtime(os.path.join(InputDir, s)))
    Temp_FileList = file_list
    file_list = [os.path.join(InputDir, Temp_File) for Temp_File in Temp_FileList if os.path.splitext(Temp_File)[1].upper() in ['.IPM','.A001','.A002','.A003','.A004','.A005','.A006']]
    
    for file_name in file_list:
        shutil.move(file_name, NewInputDir)
        
    InputDir = NewInputDir
    MessageLogger.debug ("New Input Directory = " + InputDir)
    
    FileCount = len(file_list)
    MessageLogger.debug(f"FileCount = {len(file_list)}")

    for filename in file_list:
        TotalLength = 0
        fl_Name = os.path.basename(filename)
        print(f"Processing File Name = {fl_Name}")
        MessageLogger.debug (f"Current File Name = {filename}")
        FilePath = f"{OutputDir}{fl_Name}"
        MessageLogger.debug (f"Expected OUT FilePath With File Name = {FilePath}")

        #Putting OutFile Name and its path in temporary variables, OutFileName_Temp and OutFilePath_Temp
        OutFilePath_Temp = f'{OutputDir}{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
        MessageLogger.debug (f"OutFileName_Temp = {OutFilePath_Temp}")
        #Putting OutFile Name and its path in temporary variables, OutFileName_Temp and OutFilePath_Temp

        inputFilePath = InputDir + fl_Name
        InputFileName = fl_Name
        
        OutFilePath = OutputDir + fl_Name
        MessageLogger.debug(f"OutFilePath = {OutFilePath}")

        if pathlib.Path(OutFilePath).exists():
            MessageLogger.debug (f"{fl_Name} File already exist at Out Folder")
            print (f"{fl_Name} File already exist at Out Folder")
            sys.exit()
            
        if len(os.listdir(ErrorDir)) > 0:
            Errordir_FileName = os.listdir(ErrorDir)
            MessageLogger.debug(f"{Errordir_FileName} : File Is Present In ErrorDir Please Resolve It Before Further Processing")
            print(f"{Errordir_FileName} : File Is Present In ErrorDir Please Resolve It Before Further Processing")
            sys.exit()
            
        RecCount = SQLSelectQueries(1,os.path.splitext(fl_Name)[1][1:],fl_Name)
        if RecCount > 0:
            MessageLogger.debug (f"{fl_Name} File already Processed CanNot Process Again")
            print (f"{fl_Name} File already Processed CanNot Process Again")
            sys.exit()

        MessageLogger.debug (f"inputFilePath = {inputFilePath}")
        f = open(inputFilePath, "rb")
        outputFile = open(OutFilePath_Temp, "ab")
        seq = True
        MsgNumber = i = 0
        str_0 = str_1 = b''
        MessageLogger.debug("Removing @ character received inside file after 1012 characters, also at the end of file")

        while not is_eof(f):
            if seq :
                str_0 = f.read(1012)
                MsgNumber = MsgNumber + 1
                
                if bFinalMessage :
                    str_3 = str_0.replace(b'\x40', b'')
                    
                    if len(str_3) > 0:
                        outputFile.write(str_2)
                        bFinalMessage = False
                    else:
                        outputFile.write(str_1)
                        break

                    str_1 = str_2 = str_3 = b''
                    
                i = -1
                i = str_0.find(b'\x40')
                J = 0
                if (i > 4):
                    J = 4
                else:
                    i = -1
                    
                if i >= 0:
                    str_1 = str_0[0:i-J] if i >= 4 else str_0
                    str_2 = str_0
                    bFinalMessage = True
                
                if not bFinalMessage:
                    ResultString = fctHandleAtTheRateChar(str_0)
                    
                    if len(ResultString) > 0:
                        outputFile.write(str_0)
                        str_0 = b''
                        
                    ResultString = b''
                    
                seq = False
                
            else:
                str4 = f.read(2)
                seq = True

        if len(str_1) > 0:
            ResultString = fctHandleAtTheRateChar(str_0)
            if (len(ResultString) > 0):
                outputFile.write(ResultString)

        ResultString = str_0 = b''
        MessageLogger.debug ("After loop, Just before closing temp file")
        f.close()

        outputFile.close()
        os.rename(OutFilePath_Temp, FilePath)
        if pathlib.Path(inputFilePath).exists():
            os.remove(inputFilePath)
            
        MessageLogger.debug ("File moves to the Out folder:") 
        InsertIntoClearingFiles_Table()

        ResultString = ""
        str_0 = str_1 = str_2 = str_3 = str4 = b''

MessageLogger.debug ("*************************** IPM File Processing End ***************************")
print("*************************** IPM File Processing End ***************************")