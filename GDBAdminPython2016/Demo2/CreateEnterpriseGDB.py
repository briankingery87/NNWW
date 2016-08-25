# Demo 2: Create Enterprise Geodatabase, Users, and Roles

# If running outside of the Python window, uncomment the import
#import arcpy

# arcpy.Describe example of is_geodatabase
arcpy.Describe(r'C:\presentations\DevSummit2016\Demos\Demo2\Demo2.sde').connectionProperties.is_geodatabase

# Create the geodatabase
arcpy.CreateEnterpriseGeodatabase_management("POSTGRESQL", 'jill', 'demo2', "DATABASE_AUTH", 'postgres', 'postgres', "SDE_SCHEMA", 'sde', 'sde', '', r'C:\presentations\DevSummit2016\Demos\Demo2\Server_Ent_Adv.ecp')

# Create an editor role
arcpy.CreateRole_management(r'C:\presentations\DevSummit2016\Demos\Demo2\Demo2.sde', 'editor')

# Create list of users
userList = ['matt', 'tom', 'colin']

# Create users and assign to editor role
for user in userList:
    arcpy.CreateDatabaseUser_management(r'C:\presentations\DevSummit2016\Demos\Demo2\Demo2.sde', "DATABASE_USER", user, user, 'editor')
        
