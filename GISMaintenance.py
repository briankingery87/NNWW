# GISMaintenance.py
# B. Gates
# 9/17/15


# Usage:
#
#   GISMaintenance.py [/AppServer applicationServer] [/DBServer databaseServer] \
#                     [/Versions] [/Compress] [/Import] [/Indexes]


# This program performs the specified GIS maintenance tasks in the sdeVector
# database.  If no maintenance tasks are specified, it does nothing.
#
# If an application server is specified, this program must be run on the
# specified server.  Otherwise, it may be run on any server.
#
# If a database server is specified, this program will work in the sdeVector
# database on the specified database server.  Otherwise, it will work in the
# sdeVector database on Conway.
#
# This program performs the specified maintenance tasks:
#
#   First, it disables scheduled tasks, services, and database connections:
#
#       - Disables scheduled GIS tasks.
#       - Stops GIS services.
#       - Stops the database from accepting new connections.
#       - Determines which Windows users are connected to the database,
#         sends email asking them to disconnect, and
#         waits 15 minutes for them to disconnect.
#       - Disconnects all users from the database.
#
#   Next, it performs the specified maintenance tasks:
#
#       /Versions
#
#           - Reconciles and posts all versions.
#           - Deletes all versions.
#           - Creates new versions for the SAP-GIS and AssetWorks-GIS
#             interfaces.
#
#       /Compress
#
#           - Compresses the database.
#
#       /Import
#
#           - Imports data from other systems.
#
#       /Indexes
#
#           - Rebuilds indexes.
#           - Updates statistics.
#
#   Finally, it re-enables database connections, services, and scheduled tasks:
#
#       - Enables the database to accept new connections.
#       - Restarts GIS services.
#       - Re-enables scheduled GIS tasks.
#       - Sends email notifying users who were connected to the database
#         prior to maintenance that all GIS services are available.
#       - Sends execution status report to administrative users.  Includes
#         termination status, the contents of generated log files, and error
#         messages, if any.


# Schedule this program to be run in the production database during the night or
# on weekends, when users do not need to be connected to the database.
# Schedule it to be run daily and weekly, respectively, as follows:
#
#       GISMaintenance.py /AppServer Arctic /DBServer Conway /Versions /Compress
#       GISMaintenance.py /AppServer Arctic /DBServer Conway /Versions /Compress /Import /Indexes


################################################################################

import arcpy, datetime, logging, os, re, shutil, smtplib, subprocess, sys, time

#
# Main program
#

def PerformMaintenance():
    global Versions, Compress, Import, Indexes
    try:
        Initialize()
        #
        # Disable scheduled tasks, services, and database connections.
        #
        if Versions or Compress or Indexes:
            DisableTasks()
            StopServices()
        if Versions or Indexes:
            StopAcceptingConnections()
            SendWarningMail()
            DisconnectUsers()
        #
        # Perform maintenance tasks that can only be done when services are
        # stopped and users are disconnected from the database.
        #
        if Versions:
            ReconcilePostDeleteVersions()
        if Compress:
            CompressDatabase()
        if Versions:
            CreateVersions()
        if Import:
            ImportDataFromOtherSystems()
        if Indexes:
            RebuildIndexes()
            UpdateStatistics()
    finally:
        try:
            #
            # Enable database connections, services, and scheduled tasks.
            #
            if Versions or Indexes:
                AcceptConnections()
            if Versions or Compress or Indexes:
                StartServices()
                EnableTasks()
            if Versions or Indexes:
                SendNotificationMail()
        finally:
            #
            # Send execution status report to administrative users.
            #
            SendStatusMail()

################################################################################

#
# Initialize
#
#   - Initialize global variables.
#   - Process command line arguments.
#   - Verify this program is running on the required server.
#

def Initialize():
    global ExecutionStartTime, ExecutionSuccessful
    global ApplicationServer, DatabaseServer
    global Versions, Compress, Import, Indexes
    global Database, DatabaseServer_Database_sde, DatabaseServer_Database_sdeAdmin, DatabaseUsers
    global TaskServer_Tasks
    global Services, ServiceServers, ServiceServer_Services
    global LogDirectory, LogFile, VersionsLogFile
    global MailServer, MailSender, NotificationMailRecipients, StatusMailRecipientsIfSuccess, StatusMailRecipientsIfError
    try:
        #
        # Execution status
        #
        ExecutionSuccessful = True
        ExecutionStartTime = datetime.datetime.now()
        date = '%04i%02i%02i' % (ExecutionStartTime.year, ExecutionStartTime.month, ExecutionStartTime.day)
        time = '%02i%02i%02i' % (ExecutionStartTime.hour, ExecutionStartTime.minute, ExecutionStartTime.second)
        thisServer = os.environ['COMPUTERNAME'].lower()
        thisUser = os.environ['USERNAME']
        #
        # Log files
        #   Write progress and error messages to the log file.
        #   Track events at and above the debug level - i.e., debug, info, warning, error, and critical.
        #
        LogDirectory = r'\\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Maintenance\LogFiles'
        LogFile         = '%s\%s_%s_%s.log'    % (LogDirectory, date, time, thisServer)
        VersionsLogFile = '%s\%s_%s_%s_%s.log' % (LogDirectory, date, time, thisServer, 'Versions')
        logging.basicConfig(filename=LogFile, filemode='w', format='[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG)
        LoggingInfo('Start time: %s' % ExecutionStartTime)
        DeleteOldLogFiles()
        #
        # Servers & maintenance tasks
        #   Initialize to defaults.
        #   Override with user-specified command line args.
        #
        ApplicationServer = thisServer  # Server on which this program must be run.  Default: Any.
        DatabaseServer    = 'conway'    # Server on which the database resides.  Default: Conway.
        Versions = False    # If True, reconcile, post, delete, and create versions.
        Compress = False    # If True, compress the database.
        Import   = False    # If True, import data from other systems.
        Indexes  = False    # If true, rebuild indexes and update statistics.
        ProcessCommandLineArgs()
        VerifyApplicationServer()
        #
        # Database
        #
        Database = 'sdeVector'
        DatabaseUsers = ['arcgiscontainer', 'dbo', 'sa', 'sde', 'sdeadmin', 'sdedataowner', 'sdeviewer']    # Users to whom email cannot be sent.
        DatabaseServer_Database_sde      = r'\\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Maintenance\Interfaces\%s_%s_%s.sde' % (DatabaseServer, Database, 'sde')
        DatabaseServer_Database_sdeAdmin = r'\\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Maintenance\Interfaces\%s_%s_%s.sde' % (DatabaseServer, Database, 'sdeAdmin')
        assert os.path.exists(DatabaseServer_Database_sde), 'Database connection file "%s" does not exist.' % DatabaseServer_Database_sde
        assert os.path.exists(DatabaseServer_Database_sdeAdmin), 'Database connection file "%s" does not exist.' % DatabaseServer_Database_sdeAdmin
        #
        # Scheduled tasks
        #   If working in production database (Conway sdeVector), temporarily
        #   stop scheduled tasks on production application server (Arctic).
        #   If working in development database (DQSQL sdeVector), temporarily
        #   stop scheduled tasks on development application server (DevArctic).
        #   Otherwise, temporarily stop scheduled tasks on all application servers.
        #
        if DatabaseServer == 'conway':
            TaskServer_Tasks = {'arctic': ['\NNWW\ImportGisScadaReadings', '\NNWW\RefreshMapServices']}
        elif DatabaseServer == 'dqsql':
            TaskServer_Tasks = {}
        else:
            TaskServer_Tasks = {'arctic': ['\NNWW\ImportGisScadaReadings', '\NNWW\RefreshMapServices']}
        #
        # Services
        #   If working in production database (Conway sdeVector), temporarily
        #   stop services on production application server (Arctic).
        #   If working in development database (DQSQL sdeVector), temporarily
        #   stop services on development application server (DevArctic).
        #   Otherwise, temporarily stop scheduled tasks on all application servers.
        #
        if DatabaseServer == 'conway':
            ServiceServers = ['arctic']
        elif DatabaseServer == 'dqsql':
            ServiceServers = ['devarctic']
        else:
            ServiceServers = ['arctic', 'devarctic']
        Services = ['ArcGIS Server', 'AW_GIS_Interface']
        ServiceServer_Services = {}
        #
        # Email
        #
        MailServer = 'nnww-smtp.nnww.nnva.gov'
        MailSender = '%s <%s@nnva.gov>' % (thisUser, thisUser)
        NotificationMailRecipients = []
        StatusMailRecipientsIfSuccess = ['Marietta Washington <mvwashington@nnva.gov>',
                                         'Brian Kingery <bkingery@nnva.gov>',
                                         'Barbara Gates <bgates@nnva.gov>']
        StatusMailRecipientsIfError   = ['Marietta Washington <mvwashington@nnva.gov>',
                                         'Brian Kingery <bkingery@nnva.gov>',
                                         'Barbara Gates <bgates@nnva.gov>']
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

#
# Process command line arguments
#
#   - Update default values for global variables with values passed in on the command line.
#

def ProcessCommandLineArgs():
    global ApplicationServer, DatabaseServer
    global Versions, Compress, Import, Indexes
    cmd = ' '.join(sys.argv)
    LoggingInfo('Command: "%s"' % cmd)
    usage = 'Usage: GISMaintenance.py [/AppServer server] [/DBServer server] [/Versions] [/Compress] [/Import] [/Indexes]'
    args = [a.lower()
               for a in sys.argv[1:]]
    while args != []:
        arg = args.pop(0)
        if arg == '/appserver':
            assert args != [], '"%s" is an invalid command.\n%s' % (cmd, usage)
            ApplicationServer = args.pop(0)
            assert ApplicationServer[0] != '/', '"%s" is an invalid command.\n%s' % (cmd, usage)
        elif arg == '/dbserver':
            assert args != [], '"%s" is an invalid command.\n%s' % (cmd, usage)
            DatabaseServer = args.pop(0)
            assert DatabaseServer[0] != '/', '"%s" is an invalid command.\n%s' % (cmd, usage)
        elif arg == '/versions':
            Versions = True
        elif arg == '/compress':
            Compress = True
        elif arg == '/import':
            Import = True
        elif arg == '/indexes':
            Indexes = True
        else:
            assert False, '"%s" is an invalid command.\n"%s" is not a valid command line argument\n%s' % (cmd, arg, usage)

#
# Verify this program is running on the required application server
#
#   - Why?  When one server is reimaged from another, scheduled tasks are
#     copied, causing them to be scheduled to run on both servers
#     simultaneously.  To prevent multiple instances of this program from being
#     run on different servers, we only allow it to be run on one server.
#

def VerifyApplicationServer():
    global ApplicationServer
    LoggingInfo('Application Server: %s' % ApplicationServer)
    thisServer = os.environ['COMPUTERNAME'].lower()
    cmd = ' '.join(sys.argv)
    assert ApplicationServer == thisServer, \
           'Tried to run "%s" on %s. It must be run on %s.' % (cmd, thisServer, ApplicationServer)

#
# Delete old log files
#
#   - Delete log files more than 30 days old.
#

def DeleteOldLogFiles():
    global LogDirectory
    LoggingInfo('    Deleting old log files ...')
    try:
        fileNamePattern = '^(\d\d\d\d)(\d\d)(\d\d)_(\d\d)(\d\d)(\d\d).*\.log$'
        for fileName in os.listdir(LogDirectory):
            m = re.match(fileNamePattern, fileName)
            if m:
                year = int(m.group(1))
                month = int(m.group(2))
                day = int(m.group(3))
                logFileDate = datetime.datetime(year, month, day)
                if datetime.datetime.today() - logFileDate > datetime.timedelta(days=30):
                    logFile = os.path.join(LogDirectory, fileName)
                    LoggingInfo('    Deleting %s' % logFile)
                    os.remove(logFile)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingError(e)

################################################################################

#
# Disable tasks
#
#   - Disable the Refresh Map Services task on Arctic.
#     It restarts the ArcGIS Server Object Manager service on Arctic,
#     which causes maintenance tasks to hang.
#

def DisableTasks():
    global ExecutionSuccessful, TaskServer_Tasks
    LoggingInfo('Disabling scheduled tasks ...')
    try:
        for (server, tasks) in TaskServer_Tasks.items():
            for task in tasks:
                success = DisableTask(task, server)
                assert success, 'Unable to disable scheduled task %s on server %s' % (task, server)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

def DisableTask(taskname, server=os.environ['COMPUTERNAME']):
    LoggingInfo('    Disabling task %s on server %s' % (taskname, server))
    cmd = r'schtasks /Change /S %s /TN "%s" /Disable' % (server, taskname)
    output = RunCommand(cmd)
    success = ParseSchTasksCommandOutput(output)
    return success

def ParseSchTasksCommandOutput(lines):
    #
    # Example input:
    #
    #   SUCCESS: The parameters of scheduled task "RefreshMapservices" have been changed.
    #
    # Example output:
    #
    #   True
    #
    for line in lines:
        m = re.match('^SUCCESS.*$', line)
        if m:
            return True
    return False

#
# The Windows SchTasks command manages scheduled tasks on a local or remote computer.
#
# Examples:
#
# - Display each scheduled task on server Arctic along with its next run time and status.
#   schtasks /Query /S Arctic
#
# - Disable scheduled task "RefreshMapservices" on server Arctic:
#   schtasks /Change /S Arctic /TN "RefreshMapservices" /Disable
#
# - Enable scheduled task "RefreshMapServices" on server Arctic:
#   schtasks /Change /S Arctic /TN "RefreshMapservices" /Enable
#

################################################################################

#
# Stop services
#
#   - Stop GIS services on the GIS servers.
#
#     (Map services on the test server may point at data on the production
#     server.)
#

def StopServices():
    global ExecutionSuccessful, ServiceServer_Services
    LoggingInfo('Stopping services ...')
    try:
        IdentifyRunningServices()
        for (server, services) in ServiceServer_Services.items():
            services.sort(reverse=True)
            for service in services:
                stopDateTime = datetime.datetime.now() + datetime.timedelta(minutes=10)
                state = StopService(service, server)
                while state != 'STOPPED':
                    assert datetime.datetime.now() < stopDateTime, 'Unable to stop service %s on server %s' % (service, server)
                    Pause(seconds=10)
                    state = QueryService(service, server)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

def IdentifyRunningServices():
    '''Determine which GIS services are currently running on each remote server.
       Save lists in ServiceServer_Services.'''
    global Services, ServiceServer_Services
    LoggingInfo('    Identifying running services ...')
    for server in ServiceServers:
        service_state = QueryServices(server)
        services = []
        for (service, state) in service_state.items():
            if service in Services and state in ['RUNNING', 'START_PENDING']:
                services.append(service)
        ServiceServer_Services[server] = services
    for (serviceServer, services) in ServiceServer_Services.items():
        LoggingInfo('        %s: %s' % (serviceServer, services))

def QueryServices(server=os.environ['COMPUTERNAME']):
    '''What is the state of each service on the given server?
       (A state is STOPPED, START_PENDING, STOP_PENDING, or RUNNING.)'''
    LoggingInfo('    Querying services on server %s ...' % server)
    # The Windows SC command communicates with the Service Controller and
    # installed services on a remote machine.
    cmd = r'sc \\%s query state= all' % server
    output = RunCommand(cmd)
    service_state = ParseSCCommandOutput(output)
    return service_state

def QueryService(service, server=os.environ['COMPUTERNAME']):
    '''What is the state of the given service on the given server?
       (A state is STOPPED, START_PENDING, STOP_PENDING, or RUNNING.)'''
    LoggingInfo('    Querying service %s on server %s ...' % (service, server))
    # The Windows SC command communicates with the Service Controller and
    # installed services on a remote machine.
    cmd = r'sc \\%s query "%s"' % (server, service)
    output = RunCommand(cmd)
    service_state = ParseSCCommandOutput(output)
    state = service_state[service]
    LoggingInfo(state)
    return state

def StopService(service, server=os.environ['COMPUTERNAME']):
    '''Stop the given service on the given server.'''
    LoggingInfo('    Stopping service %s on server %s ...' % (service, server))
    # The Windows SC command communicates with the Service Controller and
    # installed services on a remote machine.
    cmd = r'sc \\%s stop "%s"' % (server, service)
    output = RunCommand(cmd)
    service_state = ParseSCCommandOutput(output)
    state = service_state[service]
    LoggingInfo(state)
    return state

def ParseSCCommandOutput(lines):
    '''Parse output from the Windows SC command.
       Return a dictionary which maps service names to states
       (STOPPED, START_PENDING, STOP_PENDING, or RUNNING).'''
    #
    # Example input:
    #
    #   SERVICE_NAME: ArcServerObjectManager
    #   DISPLAY_NAME: ArcGIS Server Object Manager
    #       TYPE                : 10 WIN32_OWN_PROCESS
    #       STATE               : 4 RUNNING
    #                             (STOPPABLE, NOT_PAUSABLE, IGNORES_SHUTDOWN))
    #       WIN32_EXIT_CODE     : 0  (0x0)
    #       SERVICE_EXIT_CODE   : 0  (0x0)
    #       CHECKPOINT          : 0x0
    #       WAIT_HINT           : 0x0
    #
    # Example output:
    #
    #   {'ArcServerObjectManager': 'RUNNING'}
    #
    service_state = {}
    for line in lines:
        m = re.match('^\s*SERVICE_NAME:\s+(\S.*\S)\s*$', line)
        if m:
            service = m.group(1)
        m = re.match('^\s*STATE\s*:\s*\d+\s+(\S+)\s*$', line)
        if m:
            state = m.group(1)
            service_state[service] = state
            service = None
    return service_state

#
# The Windows SC command communicates with the Service Controller
# and installed services on a remote machine.
#
# Examples:
#
# - Start service "ArcServerObjectManager" on server "Arctic":
#   sc \\Arctic start "ArcServerObjectManager"
#
# - Stop service "ArcServerObjectManager" on server "Arctic":
#   sc \\Arctic stop "ArcServerObjectManager"
#
# - Look up state (stopped, start pending, running, or stop pending)
#   of service "ArcServerObjectManager" on server "Arctic":
#   sc \\Arctic query "ArcServerObjectManager"
#
# - Look up service name, display name, and state (stopped, start pending,
#   running, or stop pending) of all services on server "Arctic":
#   sc \\Arctic query state= all
#
# - Look up name of service with display name "ArcGIS Server Object Manager" on
#   server "Arctic":
#   sc \\Arctic getkeyname "ArcGIS Server Object Manager"
#

################################################################################

#
# Prevent database from accepting new connections
#
#   (Any connection can lock items in the database, which may stop a task from
#   running or completing.)
#

def StopAcceptingConnections():
    global ExecutionSuccessful, DatabaseServer_Database_sde
    LoggingInfo('Preventing database from accepting new connections ...')
    try:
        arcpy.AcceptConnections(DatabaseServer_Database_sde, False)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

################################################################################

#
# Send warning mail
#
#   - Determine which Windows users are currently connected to the database, and
#     save their email addresses in the NotificationMailRecipients list.
#   - Send mail warning them that they will be disconnected from the database in
#     15 minutes to enable automated maintenance work to proceed.
#   - Wait 15 minutes for users to finish their work before disconnecting them.
#

def SendWarningMail():
    global DatabaseServer_Database_sde, DatabaseUsers, MailSender, MailServer, NotificationMailRecipients
    LoggingInfo('Sending warning email ...')
    NotificationMailRecipients = ['%s@nnva.gov' % u
                                     for u in ConnectedWindowsUsers()]
    if NotificationMailRecipients != []:
        LoggingInfo('    Sending email to %s ...' % repr(NotificationMailRecipients))
        subject = 'Please Disconnect from the GIS Database'
        body =  'Please save your edits, stop editing, and disconnect from the GIS database.\n\r'
        body += 'Automated GIS maintenance will begin in 15 minutes.\n\r\n\r'
        body += 'This message was sent by an automated process.  Please do not reply.\n\r'
        SendMail(MailServer, MailSender, NotificationMailRecipients, subject, body)
        # Wait until 15 minutes have passed or all Windows users have disconnected
        # from the database, whichever comes first.
        deadline = datetime.datetime.now() + datetime.timedelta(minutes=15)
        while datetime.datetime.now() < deadline and \
              ConnectedWindowsUsers() != []:
            Pause(minutes=1)

def ConnectedWindowsUsers():
    return [u
               for u in ConnectedUsers()
                   if u not in DatabaseUsers]

def ConnectedUsers():
    users = [u.Name.lower()
                for u in arcpy.ListUsers(DatabaseServer_Database_sde)]
    distinctUsers = list(set(users))
    return distinctUsers
        
################################################################################

#
# Disconnect users
#

# Note: arcpy.DisconnectUser cannot disconnect users from a 9.3 GDB.
def DisconnectUsers():
    global ExecutionSuccessful, DatabaseServer_Database_sde
    LoggingInfo('Disconnecting users ...')
    try:
        usersToDisconnect = 'ALL'
        arcpy.DisconnectUser(DatabaseServer_Database_sde, usersToDisconnect)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

################################################################################

#
# Reconcile, post, and delete versions
#
#   - For each version, except the default version:
#
#       - Reconcile version - bring edits from an ancestor (the target version)
#         into the edit version.
#
#         If conflicts are detected, abort the reconcile.  This may cause some,
#         but not all, versions to be deleted.  When this happens, conflicts
#         should be resolved manually - the administrator should decide whether
#         to keep edits from the target version or edits from the edit version.
#
#       - Post version - merge all edits made in the edit version into the
#         target version, making both versions identical.
#
#       - Delete version.

def ReconcilePostDeleteVersions():
    global DatabaseServer_Database_sde, ExecutionSuccessful, VersionsLogFile
    LoggingInfo('Reconciling, posting and deleting versions ...')
    try:
        #
        # ALL_VERSIONS = Reconcile edit versions with the target version.
        # BLOCKING_VERSIONS = Reconcile versions that are blocking the target version
        #     from compressing.  Use the recommended reconcile order.
        #
        reconcileMode = 'ALL_VERSIONS'
        #
        # The target version is a version in the direct ancestry of an edit version,
        # such as the parent version or the default version.  It typically contains
        # edits from other versions that the user performing the reconcile would like
        # to pull into their edit version.
        #
        targetVersion = 'sde.DEFAULT'
        #
        # Reconcile these edit versions with the target version.
        #
        editVersionList = arcpy.ListVersions(DatabaseServer_Database_sde)
        #
        # LOCK_ACQUIRED = Acquire locks during the reconcile process.  Use this when
        #     edits will be posted.  It ensures that the target version is not
        #     modified in the time between the reconcile and post operations.
        # NO_LOCK_ACQUIRED = Do not acquire locks during the reconcile process.  This
        #     allows multiple users to reconcile in parallel.  Use this when the
        #     edit version will not be posted to the target version because the
        #     target version might be modified in the time between the reconcile and
        #     post operations.
        #
        acquireLocks = 'LOCK_ACQUIRED'
        #
        # BY_OBJECT = During reconcile, treat as a conflict changes to the same
        #     feature (record) in the parent and child versions.
        # BY_ATTRIBUTE = During reconcile, treat as a conflict changes to the same
        #     attribute (field) of the same feature (record) in the parent and child
        #     versions.
        #
        conflictDefinition = 'BY_ATTRIBUTE'
        #
        # ABORT_CONFLICTS = Abort the reconcile if conflicts are found between the
        #     target version and the edit version.
        # NO_ABORT = Do not abort the reconcile if conflicts are found between the
        #     target version and the edit version.
        #
        abortIfConflicts = 'ABORT_CONFLICTS'
        #
        # FAVOR_TARGET_VERSION = Resolve conflicts in favor of the target version.
        # FAVOR_EDIT_VERSION = Resolve conflicts in favor of the edit version.
        #
        conflictResolution = 'FAVOR_EDIT_VERSION'
        #
        # POST = After the reconcile, post the current edit version to the target
        #     version.
        # NO_POST = After the reconcile, do not post the current edit version to the
        #     target version.
        #
        withPost = 'POST'
        #
        # DELETE_VERSION = Delete the current edit version after it is reconciled
        #     and posted to the target version.
        # KEEP_VERSION = Do not delete the current edit version after it is
        #     reconciled and posted to the target version.
        #
        withDelete = 'DELETE_VERSION'
        #
        # Write geoprocessing messages to an ASCII log file.
        #
        arcpy.ReconcileVersions_management(DatabaseServer_Database_sde, reconcileMode, targetVersion, editVersionList,
                                           acquireLocks, abortIfConflicts, conflictDefinition, conflictResolution,
                                           withPost, withDelete, VersionsLogFile)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

################################################################################

#
# Compress database
#
#   - Reduce the amount of data the database must search through for each
#     version query.
#

def CompressDatabase():
    global DatabaseServer_Database_sde, ExecutionSuccessful
    LoggingInfo('Compressing database ...')
    try:
        arcpy.Compress_management(DatabaseServer_Database_sde)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

################################################################################

#
# Create versions
#
#   Create versions for the SAP-GIS and AssetWorks-GIS Interfaces.
#

def CreateVersions():
    global ExecutionSuccessful
    LoggingInfo('Creating versions ...')
    #
    # Create versions for SAP-GIS Interface.
    #
    # Create SAP_GIS_Interface as child of Default with public access.
    try:
        versionName = 'SAP_GIS_Interface'
        parentVersion = 'sde.DEFAULT'
        accessLevel = 'PUBLIC'
        CreateVersion(versionName, parentVersion, accessLevel)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingError(e)
    # Create ArcGISContainer as child of SAP_GIS_Interface with public access.
    try:
        versionName = 'ArcGISContainer'
        parentVersion = 'DBO.SAP_GIS_Interface'
        accessLevel = 'PUBLIC'
        CreateVersion(versionName, parentVersion, accessLevel)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingError(e)
    #
    # Create version for AssetWorks-GIS Interface.
    #
    # Create AW_GIS_Interface as child of Default with public access.
    try:
        versionName = 'AW_GIS_Interface'
        parentVersion = 'sde.DEFAULT'
        accessLevel = 'PUBLIC'
        CreateVersion(versionName, parentVersion, accessLevel)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingError(e)

def CreateVersion(versionName, parentVersion, accessLevel):
    global DatabaseServer_Database_sdeAdmin
    LoggingInfo('    Creating version %s with parent %s and access level %s...' % (versionName, parentVersion, accessLevel))
    arcpy.CreateVersion_management(DatabaseServer_Database_sdeAdmin, parentVersion, versionName, accessLevel)

################################################################################

#
# Import data from other systems
#
#   - Run SSIS (SQL Server Integration Services) packages to import data into
#     GIS from other systems.
#   - Note:  SSIS packages must be run in Visual Studio 2008.
#

# To do: Implement ImportDataFromOtherSystems().
# Future to do: Reconfigure SSIS packages to import data into Conway sdeVector.
def ImportDataFromOtherSystems():
    global ExecutionSuccessful
    LoggingInfo('Importing data from other systems ...')
    try:
        assert False, 'Unable to import data from other systems - "/Import" option has not been implemented.'
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingError(e)

################################################################################

#
# Rebuild indexes
#
#   - Boost database performance on indexes that became fragmented during
#     compression.
#

def RebuildIndexes():
    global DatabaseServer_Database_sde, ExecutionSuccessful
    LoggingInfo('Rebuilding indexes ...')
    try:
        #
        # SYSTEM = Rebuild indexes on the state and state lineage tables.
        # NO_SYSTEM = Do not rebuild indexes on the state and state lineage
        #     tables.
        #
        includeSystem = 'SYSTEM'
        #
        # List the rasters, tables and feature classes owned by sdeDataOwner.
        #
        dataList = ListData(DatabaseServer_Database_sde)
        #
        # ALL = Rebuild indexes on all tables for the selected datasets.  This
        #     includes spatial indexes, user-created attribute indexes, and
        #     geodatabase-maintained indexes for the dataset.
        # ONLY_DELTAS = Only rebuild indexes for the delta tables of the
        #     selected datasets.  Use this option if the business tables for the
        #     selected datasets are not updated often and there is a high volume
        #     of edits in the delta tables.
        #
        deltaOnly = 'ALL'
        arcpy.RebuildIndexes_management(DatabaseServer_Database_sde, includeSystem, dataList, deltaOnly)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

def ListData(workspace):
    # Return list of feature classes, tables, and rasters owned by sdeDataOwner.
    arcpy.env.workspace = workspace
    pattern='*.sdeDataOwner.*'
    data = arcpy.ListRasters(pattern)
    data += arcpy.ListTables(pattern)
    data += arcpy.ListFeatureClasses(pattern)
    data += [f for d in arcpy.ListDatasets(pattern)
                   for f in arcpy.ListFeatureClasses(pattern, 'ALL', d)]
    return data

################################################################################

#
# Update database statistics
#
#   - Facilitate optimal performance of SQL Server's query optimizer.
#
#   - This is an input/output-intensive operation.  Do this when database
#     traffic is at its lightest.
#

def UpdateStatistics():
    global DatabaseServer_Database_sde, ExecutionSuccessful
    LoggingInfo('Updating statistics ...')
    try:
        #
        # SYSTEM = Gather statistics on the state and state lineage tables.
        # NO_SYSTEM = Do not gather statistics on the state and state lineage
        #     tables.
        #
        includeSystem = 'SYSTEM'
        #
        # List the rasters, tables and feature classes owned by sdeDataOwner.
        #
        dataList = ListData(DatabaseServer_Database_sde)
        #
        # ANALYZE_BASE = Gather statistics on the base tables for the selected
        #     datasets.
        # NO_ANALYZE_BASE = Do not gather statistics for the base tables for the
        #     selected datasets.
        #
        analyzeBase = 'ANALYZE_BASE'
        #
        # ANALYZE_DELTA = Gather statistics on the delta tables for the selected
        #     datasets.
        # NO_ANALYZE_DELTA = Do not gather statistics on the delta tables for
        #     the selected datasets.
        #
        analyzeDelta = 'ANALYZE_DELTA'
        #
        # ANALYZE_ARCHIVE = Gather statistics on the archive tables for the
        #     selected datasets.
        # NO_ANALYZE_ARCHIVE = Do not gather statistics on the archive tables
        #     for the selected datasets.
        #
        analyzeArchive = 'ANALYZE_ARCHIVE'
        arcpy.AnalyzeDatasets_management(DatabaseServer_Database_sde, includeSystem, dataList,
                                         analyzeBase, analyzeDelta, analyzeArchive)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

################################################################################

#
# Allow database to accept new connections
#

def AcceptConnections():
    global DatabaseServer_Database_sde, ExecutionSuccessful
    LoggingInfo('Enabling the database to accept new connections ...')
    try:
        arcpy.AcceptConnections(DatabaseServer_Database_sde, True)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingError(e)

################################################################################

#
# Start services
#
#   - Start the GIS services that were stopped on Arctic and DevArctic.
#

def StartServices():
    global ServiceServer_Services, ExecutionSuccessful
    LoggingInfo('Starting services ...')
    #
    # On each server, start each service that was previously stopped.
    # If unable to restart all services, start as many as possible and catch exceptions.
    # If any exceptions are caught, re-raise one of them.
    #
    exception = None
    for (server, services) in ServiceServer_Services.items():
        services.sort(reverse=True)
        while services != []:
            service = services.pop()
            try:
                #
                # Query the service for its current state --
                # STOP_PENDING, STOPPED, START_PENDING, or RUNNING.
                #
                state = QueryService(service, server)
                #
                # If state is STOP_PENDING, wait until state is STOPPED.
                # (If after 10 minutes it still is not STOPPED, raise an exception.)
                #
                if state == 'STOP_PENDING':
                    deadline = datetime.datetime.now() + datetime.timedelta(minutes=10)
                    while state != 'STOPPED':
                        assert datetime.datetime.now() < deadline, \
                               'Unable to stop service %s on server %s after 10 minutes' % (service, server)
                        Pause(seconds=10)
                        state = QueryService(service, server)
                #
                # If the state is STOPPED, start it.
                #
                if state == 'STOPPED':
                    state = StartService(service, server)
                #
                # If state is START_PENDING, wait until state is RUNNING.
                # (If after 10 minutes it still is not RUNNING, raise an exception.)
                #
                if state == 'START_PENDING':
                    deadline = datetime.datetime.now() + datetime.timedelta(minutes=10)
                    while state != 'RUNNING':
                        assert datetime.datetime.now() < deadline, \
                               'Unable to start service %s on server %s after 10 minutes' % (service, server)
                        Pause(seconds=10)
                        state = QueryService(service, server)
                #
                # The service should now be RUNNING.
                # (If it is not, raise an exception.)
                #
                assert state == 'RUNNING', \
                       'Unable to start service %s on server %s' % (service, server)
            except BaseException, e:
                #
                # Caught an exception.
                # Save it, and continue.
                #
                ExecutionSuccessful = False
                LoggingCritical(e)
                exception = e
    #
    # If exceptions were caught, re-raise one of them.
    #
    if exception:
        raise exception

def StartService(service, server=os.environ['COMPUTERNAME']):
    '''Start the given service on the given server.'''
    LoggingInfo('    Starting service %s on server %s ...' % (service, server))
    # The Windows SC command communicates with the Service Controller and
    # installed services on a remote machine.
    cmd = r'sc \\%s start "%s"' % (server, service)
    output = RunCommand(cmd)
    service_state = ParseSCCommandOutput(output)
    state = service_state[service]
    LoggingInfo(state)
    return state

################################################################################

#
# Enable tasks
#
#   - Enable the Refresh Map Services task on Arctic.
#

def EnableTasks():
    global ExecutionSuccessful, TaskServer_Tasks
    LoggingInfo('Enabling scheduled tasks ...')
    try:
        for (server, tasks) in TaskServer_Tasks.items():
            for task in tasks:
                success = EnableTask(task, server)
                assert success, 'Unable to enable scheduled task %s on server %s' % (task, server)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingCritical(e)
        raise e

def EnableTask(taskname, server=os.environ['COMPUTERNAME']):
    LoggingInfo('    Enabling task %s on server %s' % (taskname, server))
    cmd = r'schtasks /Change /S %s /TN "%s" /Enable' % (server, taskname)
    output = RunCommand(cmd)
    success = ParseSchTasksCommandOutput(output)
    return success

################################################################################

#
# Send notification email
#
#   - Notify users who were programmatically disconnected before maintenance
#     that all GIS services are now available.
#

def SendNotificationMail():
    global MailSender, MailServer, NotificationMailRecipients
    LoggingInfo('Sending notification email ...')
    try:
        subject = 'All GIS services are now available'
        body  = 'All GIS services are now available.\n\r\n\r'
        body += 'This message was sent by an automated process.  Please do not reply.\n\r'
        SendMail(MailServer, MailSender, NotificationMailRecipients, subject, body)
    except BaseException, e:
        ExecutionSuccessful = False
        LoggingError(e)

################################################################################

#
# Send status email
#
#   - Send email to administrative users which states whether the task terminated
#     successfully.  Include error messages, if any.  Include contents of log file, if any.
#

def SendStatusMail():
    global ExecutionSuccessful, ExecutionStartTime, MailSender, MailServer
    global StatusMailRecipientsIfError, StatusMailRecipientsIfSuccess
    LoggingInfo('Sending status email ...')
    program = os.path.basename(sys.argv[0])
    args = ' '.join(sys.argv[1:])
    taskName = '%s %s' % (program, args)
    thisServer = os.environ['COMPUTERNAME'].lower()
    status = 'Success'
    recipients = StatusMailRecipientsIfSuccess
    if not ExecutionSuccessful:
        status = 'Error'
        recipients = StatusMailRecipientsIfError
    executionEndTime = datetime.datetime.now()
    elapsedTime = executionEndTime - ExecutionStartTime
    subject = 'Scheduled Task - %s - %s' % (taskName, status)
    body  = 'Task:   %s\r\n\r\n' % taskName
    body += 'Application Server:  %s\r\n\r\n' % thisServer
    body += 'Database Server:  %s\r\n\r\n' % DatabaseServer
    body += 'Started:   %s\r\n\r\n' % ExecutionStartTime.strftime('%A, %B %d, %Y %I:%M:%S %p')
    body += 'Ended:   %s\r\n\r\n' % executionEndTime.strftime('%A, %B %d, %Y %I:%M:%S %p')
    body += 'Elapsed Time:  %s\r\n\r\n' % str(elapsedTime).split('.')[0]
    body += 'Status:   %s\r\n\r\n' % status
    for logFile in [LogFile, VersionsLogFile]:
        if os.path.exists(logFile):
            (logPath, logFileName) = os.path.split(logFile)
            body += '-------- %s --------\r\n\r\n' % logFileName
            f = open(logFile, 'r')
            body += ''.join(f.readlines())
            f.close()
            body += '\r\n'
    body += '\r\n'
    body += 'This message was sent by an automated process.  Please do not reply.\r\n'
    SendMail(MailServer, MailSender, recipients, subject, body)

################################################################################

def RunCommand(cmd):
    '''Run the given Windows command. Return its output as a list of strings.'''
    LoggingInfo('    Running Windows command "%s"' % cmd)
    # subprocess.check_output runs the given Windows command and returns its output as a byte string.
    # If return code is non-zero, raises a CalledProcessError, in which case subprocess.CalledProcessError
    # has the return code in the returncode attribute and any output in the output attribute.
    output = subprocess.check_output(cmd)   # Note: Must run on app server with Python 2.7.3 (Arctic, DevArctic)
    lines = output.splitlines()
    return lines

def Pause(minutes=0, seconds=0):
    seconds = 60 * minutes + seconds
    LoggingInfo('    Pausing %s seconds ...' % seconds)
    time.sleep(seconds)

def SendMail(mailServer, sender, recipients, subject, body):
    if recipients:
        message = 'From: %s\r\n' % sender
        message += 'To: %s\r\n' % ', '.join(recipients)
        message += 'Subject: %s\r\n\r\n' % subject
        message += '%s\r\n' % body
        server = smtplib.SMTP(mailServer)
        server.sendmail(sender, recipients, message)
        server.quit()

def LoggingInfo(msg):
    print msg
    logging.info(msg)

def LoggingError(msg):
    print msg
    logging.error(msg)

def LoggingCritical(msg):
    print msg
    logging.critical(msg)

def DisplayConnections():
    # Print all database connections.
    # This procedure is not called, but it is useful when testing code interactively.
    global DatabaseServer_Database_sde
    print "%-21s  %-12s  %-14s  %-8s  %s" % ('Connection Date Time', 'Client', 'User', 'Trans ID', 'Connection Type')
    print "%-21s  %-12s  %-14s  %-8s  %s" % ('--------------------', '------', '----', '--------', '---------------')
    for u in arcpy.ListUsers(DatabaseServer_Database_sde):
        connectionTime = u.ConnectionTime.strftime('%Y-%m-%d  %H:%M:%S')
        if u.IsDirectConnection:
            connectionType = 'Direct'
        else:
            connectionType = 'Application Server'
        print "%-21s  %-12s  %-14s  %-8s  %s" % (connectionTime, u.ClientName, u.Name, u.ID, connectionType)

################################################################################

#
# If maintenance tasks were specified on the commmand line, run the main program.
#

args = set([a.lower() for a in sys.argv])
args.intersection_update(['/versions', '/compress', '/import', '/indexes'])
if args:
    PerformMaintenance()
