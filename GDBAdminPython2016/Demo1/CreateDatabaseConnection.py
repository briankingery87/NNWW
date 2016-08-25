# Demo 1: Create Database Connection File

# If running outside of the Python window, uncomment the import
#import arcpy

#Create the connection file
arcpy.CreateDatabaseConnection_management(r'C:\presentations\DevSummit2016\Demos\Demo1',
                                          'Demo1',
                                          "POSTGRESQL",
                                          'jill',
                                          "DATABASE_AUTH",
                                          'postgres', 'postgres',
                                          "SAVE_USERNAME",
                                          "demo1")
