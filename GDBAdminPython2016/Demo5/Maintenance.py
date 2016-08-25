# Name: Maintenance.py
# Description: This script will reconcile, post and compress an
#              enterprise geodatabase. It will then rebuild indexes
#              and gather statistics on the data in the geodatabase and 
#              send an email report.

# Author: Esri

# Import the required modules
import arcpy, smtplib, sys

try:
    # Set an gdb admin connection variable.
    adminConn = "C:/presentations/DevSummit2016/Demos/Demo5/Admin.sde"
    print("Connecting to the geodatabase as the gdb admin user (sde)")
    
    # Set a few environment variables
    arcpy.env.workspace = adminConn
    arcpy.env.overwriteOutput = True

    # For demo purposes we will block connections to the geodatabase during schema rec/post/compress.
    print("The database is no longer accepting connections")
    arcpy.AcceptConnections(adminConn, False)

    # Disconnect any connected users.
    print("Disconnecting all users")
    arcpy.DisconnectUser(adminConn, 'ALL')

    # Get a list of versions to pass into the ReconcileVersions tool.
    # Only reconcile versions that are children of Default
    print("Compiling a list of versions to reconcile")
    verList = arcpy.da.ListVersions(adminConn)
    versionList = [ver.name for ver in verList if ver.parentVersionName == 'sde.DEFAULT']

    # Execute the ReconcileVersions tool.
    try:
        print("Reconciling all versions")
        arcpy.ReconcileVersions_management(adminConn, "ALL_VERSIONS", "sde.DEFAULT",
                                           versionList,"LOCK_ACQUIRED", "NO_ABORT",
                                           "BY_OBJECT", "FAVOR_TARGET_VERSION","POST",
                                           "KEEP_VERSION", sys.path[0] + "/reclog.txt")
        recMsg = 'Reconcile and post executed successfully.\n\r'
        recMsg += 'Reconcile Log is below.\n' #warning this can be very long.
        recMsg += open(sys.path[0] + "/reclog.txt", 'r').read()
    except:
        recMsg = 'Reconcile & post failed. Error message below.\n\r' + arcpy.GetMessages()

    # Run the compress tool.
    try:
        print("Running compress")
        arcpy.Compress_management(adminConn)
        #if the compress is successful add a message.
        compressMsg = '\nCompress was successful.\n\r'
    except:
        #If the compress failed, add a message.
        compressMsg = '\nCompress failed: error message below.\n\r' + arcpy.GetMessages()


    #Update statistics and idexes for the system tables
    # Note: to use the "SYSTEM" option the user must be an geodatabase or database administrator.
    try:
        print("Rebuilding indexes on the system tables")
        arcpy.RebuildIndexes_management(adminConn, "SYSTEM")
        rebuildSystemMsg = 'Rebuilding of system table indexes successful.\n\r'
    except:
        rebuildSystemMsg = 'Rebuild indexes on system tables fail: error message below.\n\r' + arcpy.GetMessages()

    try:
        print("Updating statistics on the system tables")
        arcpy.AnalyzeDatasets_management(adminConn, "SYSTEM")
        analyzeSystemMsg = 'Analyzing of system tables successful.\n\r'
    except:
        analyzeSystemMsg = 'Analyze system tables failed: error message below.\n\r' + arcpy.GetMessages()


    # Allow connections again.
    print("Allow users to connect to the database again")
    arcpy.AcceptConnections(adminConn, True)
    print("Finshed gdb admin user (sde) tasks \n")

    #Get a list of datasets owned by the gdb user
    print("Connecting to the geodatabase as the data owner (gdb)")
    
    # Get the user name for the workspace
    # this assumes you are using database authentication.
    # OS authentication connection files do not have a 'user' property.
    ownerConn = 'C:/presentations/DevSummit2016/Demos/Demo5/Owner.sde'
    print("Using Describe function to get the connected user name property from the connection file")
    desc = arcpy.Describe(ownerConn)
    connProps = desc.connectionProperties
    userName = connProps.user
    print("Connected as user {0}".format(userName))

    # Get a list of all the datasets the user has access to.
    # First, get all the stand alone tables and feature classes.
    print("Compiling a list of data owned by the {0} user".format(userName))
    dataList = arcpy.ListTables('*.' + userName + '.*') \
             + arcpy.ListFeatureClasses('*.' + userName + '.*')

    # Next, for feature datasets get all of the featureclasses
    # from the list and add them to the master list.
    for dataset in arcpy.ListDatasets('*.' + userName + '.*'):
        dataList += arcpy.ListFeatureClasses(feature_dataset=dataset)

    # Pass in the list of datasets owned by the data owner to the rebuild indexes and
    # analyze datasets tools.
    try:
        print("Rebuilding indexes on the data, as the data owner")
        arcpy.RebuildIndexes_management(ownerConn, "NO_SYSTEM", dataList, "ALL")
        rebuildUserMsg = 'Rebuilding of user data indexes successful.\n\r'
    except:
        rebuildUserMsg = 'Rebuild user data indexes failed: error message below.\n\r' + arcpy.GetMessages()

    try:
        print("Updating statistics on the data, as the data owner")
        arcpy.AnalyzeDatasets_management(ownerConn, "NO_SYSTEM", dataList,"ANALYZE_BASE",
                                         "ANALYZE_DELTA", "ANALYZE_ARCHIVE")
        analyzeUserMsg = 'Analyzing of user datasets successful.\n\r'
    except:
        analyzeUserMsg = 'Analyze user datasets failed: error message below.\n\r' + arcpy.GetMessages()
    
    print("Finished data owner (gdb) tasks \n")
    #Set a flag to indicate that the script has finished executing its required tasks.
    scriptSuccess = True

except:
    import traceback
    scriptSuccess = False
    failMsg = '\n**SCRIPT FAILURE**\n'
    failMsg += 'Most recent GP messages below.\n'
    failMsg += arcpy.GetMessages() +'\n'
    failMsg += '\nTraceback messages below.\n'
    failMsg += traceback.format_exc().splitlines()[-1]

# Email function for sending emails.
def sendEmail(subject, emailmessage):
        # take the email list and use it to send an email to connected users.
        SERVER = "your smtp server"
        FROM = "Python Admin <you@yourdomain.com>"
        TO = 'you@yourdomain.com'
        SUBJECT = subject
        MSG = emailmessage

        # Prepare actual message
        MESSAGE = """\
From: %s
To: %s
Subject: %s

%s
        """ % (FROM, TO, SUBJECT, MSG)
        #Connect to the server
        server = smtplib.SMTP(SERVER)
        # Send the mail
        server.sendmail(FROM, TO, MESSAGE)
        #Disconnect from the server.
        server.quit()
        #multiple examples for sending emails.
        #http://docs.python.org/library/email-examples.html#email-examples

#Send a summary using the send email function and the messages that have been created.
if scriptSuccess == True:
    subject = 'Geodatabase maintenance script summary.'
    msg = recMsg + compressMsg + rebuildSystemMsg + analyzeSystemMsg +rebuildUserMsg + analyzeUserMsg
else:
    subject = 'Geodatabase maintenance script failed.'
    msg = failMsg

print("Sending email report")
sendEmail(subject, msg)

print("Done.")
