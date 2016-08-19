# RecreateDistributionAreaPolygon.py

# Create DistributionArea polygon feature class
# Copy it into the production database.

# This program is run monthly on Arctic as a scheduled task
#
# This program can only run successfully when
# a connection can be created to the production database
#
# The DistributionArea feature class does not reside in a dataset for two reasons:
# (1) Since it does not have to be versioned for editing, it does not have to
#     reside in a versioned feature class.
# (2) Since it is updated programmatically, it must not reside in a dataset that
#     is locked when services are running.

# 2013-04-05 BLG:
#   - Only include mains where Subsystem = 'Distribution'
#   - Increase buffer zone distance from 100 feet to 500 feet
#   - Do not eliminate holes from polygons

import arcgisscripting, datetime, os, smtplib, sys

gp = arcgisscripting.create()

# Production database - read, update.
N                    = r'\\Arctic\GIS_Data'
SDE                  = N + r'\GIS_Private\DataResources\DBA\Datasets\Export\Interfaces\Connections\Conway_sdeVector_sdeDataOwner.sde'
SDE_wPressurizedMain = SDE + r'\sdeVector.sdeDataOwner.WaterUtility\sdeVector.sdeDataOwner.wPressurizedMain'
SDE_DistributionArea = SDE + r'\sdeVector.sdeDataOwner.DistributionArea'

# Temporary database - create, update, delete.
GDB                  = r'C:\GIS_Development\DataResources\DBA\Datasets\Export\Data\Databases'
GDB_DatabaseName     = r'DistributionArea.gdb'
GDB_Database         = GDB + r'\\' + GDB_DatabaseName
GDB_DistributionMain = GDB_Database + r'\DistributionMain'
GDB_DistributionArea = GDB_Database + r'\DistributionArea'

# Status report email.
MailServer = 'nnww-smtp.nnww.nnva.gov'
MailSender = '%s <%s@nnva.gov>' % (os.environ['USERNAME'], os.environ['USERNAME'])  # 'GisAdmin <GisAdmin@nnva.gov>'
MailRecipientsIfSuccess = ['Marietta Washington <mvwashington@nnva.gov>',
                           'Brian Kingery <bkingery@nnva.gov>',
                           'Barbara Gates <bgates@nnva.gov>']
MailRecipientsIfError = ['Marietta Washington <mvwashington@nnva.gov>',
                         'Brian Kingery <bkingery@nnva.gov>',
                         'Barbara Gates <bgates@nnva.gov>']

################################################################################

def RecreateDistributionAreaPolygon():
    startTime = datetime.datetime.now()
    errorMsgs = []
    try:
        CreateTemporaryDatabase()
        CreateFeatureClass()
        CopyToProductionDatabase()
        DeleteTemporaryDatabase()
    except BaseException, e:
        errorMsgs.append(e)
    finally:
        SendStatusMail(mailServer = MailServer,
                       sender = MailSender,
                       recipientsIfSuccess = MailRecipientsIfSuccess,
                       recipientsIfError = MailRecipientsIfError,
                       startDateTime = startTime,
                       errorMessages = errorMsgs)
    
################################################################################

def CreateTemporaryDatabase():
    print 'CREATING TEMPORARY DATABASE ...'
    if not gp.Exists(GDB):
        print 'Creating directory ' + GDB + ' ...'
        CreateDirectory(GDB)
    if gp.Exists(GDB_Database):
        print 'Deleting old temporary database ' + GDB_Database + ' ...'
        gp.Delete(GDB_Database)
    print 'Creating temporary database ' + GDB_Database + ' ...'
    gp.CreateFileGDB(GDB, GDB_DatabaseName)
    print 'Copying distribution mains from ' + SDE_wPressurizedMain + ' to temporary database ' + GDB_DistributionMain + ' ...'
    whereClause = 'Subsystem = 50'  # Distribution
    gp.Select_analysis(SDE_wPressurizedMain, GDB_DistributionMain, whereClause)
    
def CreateFeatureClass():
    print 'CREATING DISTRIBUTION AREA FEATURE CLASS ...'
    print 'Buffering mains to create polygon feature class ' + GDB_DistributionArea + ' ...'
    bufferZoneDistance = '500 FEET' # Create buffer zone of this distance around input features
    lineSide = 'FULL'               # Generate buffer on both sides of each line
    lineEndType = 'ROUND'           # Generate buffer in the shape of a half circle at the end of each line
    dissolveOption = 'ALL'          # Dissolve all buffers together into a single feature & remove overlap
    dissolveField = ''              # Ignore field values when dissolving buffers
    gp.Buffer_analysis(GDB_DistributionMain, GDB_DistributionArea, bufferZoneDistance, lineSide, lineEndType, dissolveOption, dissolveField)
    print 'Adding field FeatureCreationDate to ' + GDB_DistributionArea + ' ...'
    gp.AddField(GDB_DistributionArea, 'FeatureCreationDate', 'DATE')
    print 'Updating field: Setting FeatureCreationDate = today''s date ...'
    gp.CalculateField(GDB_DistributionArea, 'FeatureCreationDate', 'Date()', 'VB')

def CopyToProductionDatabase():
    # This procedure repopulates the DistributionArea feature class in the production database.
    # (It does not replace the entire feature class.  Deleting the old feature class requires an
    # exclusive schama lock, which may not be available if services are running or basemap network is open.)
    print 'COPYING DISTRIBUTION AREA FEATURES TO PRODUCTION DATABASE ...'
    print 'Deleting old features from ' + SDE_DistributionArea + ' ...'
    gp.deletefeatures(SDE_DistributionArea)
    print 'Copying new features from ' + GDB_DistributionArea + ' to ' + SDE_DistributionArea + ' ...'
    gp.Append_management(GDB_DistributionArea, SDE_DistributionArea)
   
def DeleteTemporaryDatabase():
    print 'DELETING TEMPORARY DATABASE ...'
    print 'Deleting temporary database ' + GDB_Database + ' ...'
    gp.Delete(GDB_Database)

################################################################################

def CreateDirectory(fullPath):
    '''Create directory if it does not exist'''
    # Split fullPath into two parts:  the drive or UNC prefix, and the remainder
    (prefix, remainder) = os.path.splitdrive(fullPath)
    if prefix == '':
        (prefix, remainder) = os.path.splitunc(fullPath)
    if remainder[0] == '\\':
        remainder = remainder[1:]
    # Split remainder into folder names
    folders = remainder.split('\\')
    # Create each subdirectory in fullPath that does not already exist
    path = prefix
    for folder in folders:
        pathAndFolder = path + '\\' + folder
        if not gp.Exists(pathAndFolder):
            print '  Creating directory ' + pathAndFolder + ' ...'
            gp.CreateFolder(path, folder)
        path = pathAndFolder

################################################################################

def SendStatusMail(mailServer=MailServer,
                   sender=MailSender,
                   recipientsIfSuccess=[],
                   recipientsIfError=[],
                   startDateTime=datetime.datetime.now(),
                   errorMessages=[],
                   warningMessages=[],
                   informationalMessages=[],
                   logFiles=[]):
    print 'Sending status email ...'
    endDateTime = datetime.datetime.now()
    elapsedTime = endDateTime - startDateTime
    server = os.environ['COMPUTERNAME'].lower()
    program = os.path.basename(sys.argv[0])
    args = ' '.join(sys.argv[1:])
    task = '%s %s' % (program, args)
    if errorMessages:
        status = 'Error'
        recipients = recipientsIfError
    elif logFiles:
        status = 'Termination status unknown'
        recipients = Union(recipientsIfSuccess, recipientsIfError)
    else:
        status = 'Success'
        recipients = recipientsIfSuccess
    subject = 'Scheduled Task - %s - %s' % (task, status)
    body  = 'Task:   %s\r\n\r\n' % task
    body += 'Server:  %s\r\n\r\n' % server
    body += 'Started:   %s\r\n\r\n' % startDateTime.strftime('%A, %B %d, %Y %I:%M:%S %p')
    body += 'Terminated:   %s\r\n\r\n' % endDateTime.strftime('%A, %B %d, %Y %I:%M:%S %p')
    body += 'Elapsed Time:  %s\r\n\r\n' % str(elapsedTime).split('.')[0]
    body += 'Status:   %s\r\n\r\n' % status
    if errorMessages:
        body = body + '-------- %s --------\r\n\r\n' % 'Error Messages'
        for m in errorMessages:
            body += '%s\r\n' % str(m)
        body += '\r\n'
    if warningMessages:
        body = body + '-------- %s --------\r\n\r\n' % 'Warning Messages'
        for m in warningMessages:
            body += '%s\r\n' % str(m)
        body += '\r\n'
    if informationalMessages:
        body = body + '-------- %s --------\r\n\r\n' % 'Informational Messages'
        for m in informationalMessages:
            body += '%s\r\n' % str(m)
        body += '\r\n'
    for logFile in logFiles:
        (logPath, logFileName) = os.path.split(logFile)
        body += '-------- %s --------\r\n\r\n' % logFileName
        f = open(logFile, 'r')
        body += ''.join(f.readlines())
        f.close()
        body += '\r\n'
    body += '\r\n'
    body += 'This is an automatically generated message.  Please do not reply.\r\n'
    SendMail(mailServer, sender, recipients, subject, body)

def SendMail(mailServer=MailServer,
             sender=MailSender,
             recipients=[],
             subject='',
             body=''):
    if recipients:
        message = 'From: %s\r\n' % sender
        message += 'To: %s\r\n' % ', '.join(recipients)
        message += 'Subject: %s\r\n\r\n' % subject
        message += '%s\r\n' % body
        server = smtplib.SMTP(mailServer)
        server.sendmail(sender, recipients, message)
        server.quit()

def Union(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    setUnion = set1.union(set2)
    union = list(setUnion)
    return union
    
################################################################################

RecreateDistributionAreaPolygon()
