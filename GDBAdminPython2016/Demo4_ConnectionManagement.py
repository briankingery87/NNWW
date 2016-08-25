# Demo 3: Connection Management

# If running outside of the Python window, uncomment the import
#import arcpy

# Block new connections to the geodatabase
arcpy.AcceptConnections(r'C:\presentations\DevSummit2016\Demos\Demo4\sde.sde', False)

# Get a list of users that are connected to the geodatabase
userList = arcpy.ListUsers(r'C:\presentations\DevSummit2016\Demos\Demo4\sde.sde')
print(userList)

# Clean up the output of the userList
for user in userList:
    print user.Name, user.ID

# Disconnect a single user
arcpy.DisconnectUser(r'C:\presentations\DevSummit2016\Demos\Demo4\sde.sde', 87)

# Regenerate list
userList = arcpy.ListUsers(r'C:\presentations\DevSummit2016\Demos\Demo4\sde.sde')
for user in userList:
    print user.Name, user.ID

# Create a list to filter sde and featureservice users from the list to disconnect
idList = [user.ID for user in userList if user.Name not in ("sde", "featureservice")]

# Disconnect all other users
arcpy.DisconnectUser(r'C:\presentations\DevSummit2016\Demos\Demo4\sde.sde', idList)

# Regenerate list
userList = arcpy.ListUsers(r'C:\presentations\DevSummit2016\Demos\Demo4\sde.sde')
for user in userList:
    print user.Name, user.ID
