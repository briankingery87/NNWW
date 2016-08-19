# UnlockFiles.py

# OVERVIEW
#
#   This program disconnects network users from the following files on Arctic:
#
#       MainBrks.shp
#       cadindex.shp
#       sdeVectorFrequent.mdb
#       sdeVectorInfrequent.mdb
#
# HOW TO RUN
#
#   UnlockFiles.py
#
# WHEN TO RUN
#
#   Run this program before running ExportWeekday.py and ExportWeekend.py.
#   This enables them to acquire exclusive schema locks on the above files.
#
# WARNING
#
#   This program must be run in 64-bit Python (the default version on the servers).


import datetime, os, re, smtplib, subprocess, sys

MainBrks_shp = r'\\Arctic\Desktop_GIS\ArcData\MainBreaks\MainBrks.shp'
cadindex_shp = r'\\Arctic\Desktop_GIS\GIS\OperationalSystems\Locations\Waterworks BaseMap\Data\Shapefiles\Drawings\cadindex.shp'
sdeVectorInfrequent_mdb = r'\\Arctic\Desktop_GIS\GIS\OperationalSystems\Locations\Waterworks BaseMap\Data\Databases\sdeVectorInfrequent.mdb'
sdeVectorFrequent_mdb = r'\\Arctic\Desktop_GIS\GIS\OperationalSystems\Locations\Waterworks BaseMap\Data\Databases\sdeVectorFrequent.mdb'

################################################################################

def UnlockFiles():
    try:
        Initialize()
        CloseFile(MainBrks_shp)
        CloseFile(cadindex_shp)
        CloseFile(sdeVectorInfrequent_mdb)
        CloseFile(sdeVectorFrequent_mdb)
    finally:
        SendStatusMail()

#
# Initialize
#
#   - Initialize global variables.
#

def Initialize():
    global ErrorMsgs, ExecutionStartTime, MailSender, MailServer
    global StatusMailRecipientsIfError, StatusMailRecipientsIfSuccess
    ErrorMsgs = []
    ExecutionStartTime = datetime.datetime.now()
    MailSender = '%s <%s@nnva.gov>' % (os.environ['USERNAME'], os.environ['USERNAME'])
    MailServer = 'nnww-smtp.nnww.nnva.gov'
    StatusMailRecipientsIfError = ['Marietta Washington <mvwashington@nnva.gov>',
                                   'Brian Kingery <bkingery@nnva.gov>',
                                   'Barbara Gates <bgates@nnva.gov>']
    StatusMailRecipientsIfSuccess = ['Marietta Washington <mvwashington@nnva.gov>',
                                     'Brian Kingery <bkingery@nnva.gov>',
                                     'Barbara Gates <bgates@nnva.gov>']

#
# Close file
#
#   - Disconnect all network users from the given file.
#

def CloseFile(fileToClose):
    print 'Disconnecting all network users from', fileToClose, '...'
    try:
        #
        # Run the "openfiles /query" command.
        # It lists all files currently open by network users on this server.
        #
        cmd = 'openfiles /query /s Arctic /fo csv /v'
        output = subprocess.check_output(cmd)
        lines = output.splitlines()
        #
        # Example command output:
        #   [
        #   '',
        #   "INFO: The system global flag 'maintain objects list' needs",
        #   '      to be enabled to see local opened files.',
        #   '      See Openfiles /? for more information.',
        #   '',
        #   '',
        #   'Files opened remotely via local share points:',
        #   '---------------------------------------------',
        #   '"Hostname","ID","Accessed By","Type","#Locks","Open Mode","Open File (Path\executable)"',
        #   '"ARCTIC","1342178304","johndoe","Windows","0","Read","E:\\desktop_gis\\"',
        #   '"ARCTIC","2013276160","janesmith","Windows","0","No Access.","E:\\desktop_gis\\"',
        #   '"ARCTIC","1208065025","bgates","Windows","0","Write + Read","E:\\desktop_gis\\TEMP\\TEMP.shp.GISDEV1.1484.5036.sr.lock"',
        #   ...
        #   ]
        #
        # Create a pattern-matching pattern to parse the command output.
        # The seven groups match: server, file ID, username, os type, lock count, open mode, open file.
        #
        linePattern = '\A"(.*)","(.*)","(.*)","(.*)","(.*)","(.*)","(.*)"\Z'
        #
        # Create a pattern-matching pattern for the path of the file to be closed.
        # Enable it to match the format of the paths of open files listed by the "openfiles /query" command.
        #   Example:  For the path  \\Arctic\Desktop_GIS\ArcData\MainBreaks\MainBrks.shp
        #   create the pattern      e:\\desktop_gis\\arcdata\\mainbreaks\\mainbrks.shp.*
        #
        filePattern = fileToClose.lower().replace(r'\\arctic', 'e:').replace('\\', '\\\\') + '.*'
        #
        # Run the "openfiles /disconnect" command.
        # It disconnects all connected network users from the file to be closed.
        #
        for line in lines:
            m = re.match(linePattern, line)
            if m:
                fileID = m.group(2)
                openFile = m.group(7).lower()
                if re.match(filePattern, openFile):
                    # Disconnect network user from file.
                    cmd = 'openfiles /disconnect /s Arctic /id %s' % fileID
                    subprocess.check_output(cmd)
    except BaseException, e:
        ErrorMsgs.append(e)
      
# openfiles /query /s SERVER /fo TABLE_or_LIST_or_CSV /v
#
#   Lists all open files.
#
#       /s SERVER - Connect to this remote server.
#       /fo TABLE_or_LIST_or_CSV - Format output as a table, list, or comma-separated value form.
#       /v - Display detailed information, including full paths to open files.

# openfiles /disconnect /s SERVER /id FILE_ID
#
#   Disconnects network user from file.
#
#       /s SERVER - Connect to this remote server.
#       /id FILE_ID - Disconnect the open file with this file ID.


#
# Send status email
#
#   - Send email to administrative users which states whether the task terminated
#     successfully.  Include error messages, if any.
#

def SendStatusMail():
    global ErrorMsgs, ExecutionStartTime, MailSender, MailServer
    global StatusMailRecipientsIfError, StatusMailRecipientsIfSuccess
    print 'Sending status email ...'
    program = os.path.basename(sys.argv[0])
    args = ' '.join(sys.argv[1:])
    taskName = '%s %s' % (program, args)
    thisServer = os.environ['COMPUTERNAME'].lower()
    status = 'Success'
    recipients = StatusMailRecipientsIfSuccess
    if ErrorMsgs <> []:
        status = 'Error'
        recipients = StatusMailRecipientsIfError
    executionEndTime = datetime.datetime.now()
    elapsedTime = executionEndTime - ExecutionStartTime
    subject = 'Scheduled Task - %s - %s' % (taskName, status)
    body  = 'Task:   %s\r\n\r\n' % taskName
    body += 'Server:  %s\r\n\r\n' % thisServer
    body += 'Started:   %s\r\n\r\n' % ExecutionStartTime.strftime('%A, %B %d, %Y %I:%M:%S %p')
    body += 'Ended:   %s\r\n\r\n' % executionEndTime.strftime('%A, %B %d, %Y %I:%M:%S %p')
    body += 'Elapsed Time:  %s\r\n\r\n' % str(elapsedTime).split('.')[0]
    body += 'Status:   %s\r\n\r\n' % status
    for msg in ErrorMsgs:
        body += '%s\r\n\r\n' % msg
    body += '\r\n'
    body += 'This message was sent by an automated process.  Please do not reply.\r\n'
    SendMail(MailServer, MailSender, recipients, subject, body)

def SendMail(mailServer, sender, recipients, subject, body):
    if recipients:
        message = 'From: %s\r\n' % sender
        message += 'To: %s\r\n' % ', '.join(recipients)
        message += 'Subject: %s\r\n\r\n' % subject
        message += '%s\r\n' % body
        server = smtplib.SMTP(mailServer)
        server.sendmail(sender, recipients, message)
        server.quit()

################################################################################

UnlockFiles()
