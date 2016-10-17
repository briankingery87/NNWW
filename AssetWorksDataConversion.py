## AssetWorksDataConversion.py
## Brian Kingery
##
## Reprojects data from Conway_sdeVector_sdeViewer.sde to GIS10Test_AssetWorksGIS_sdeDataOwner.sde from
## NAD_1983_StatePlane_Virginia_South_FIPS_4502_Feet to WGS_1984 and thenperforms additional tasks on the
## pressurized mains followed by changing the privileges on each feature class in the dataset.
##

############################################################################
############################################################################

##----------------------> If Ran Successfully <-----------------------------
##Python Shell Work Log   

##>>>
##Started: Tuesday, December 15, 2015 02:00:23 PM
##Processing
##Old DataConversion Dataset deleted
##DataConversion dataset created
##SystemValve converted
##Hydrant converted
##wServiceLocation converted
##wPressurizedMain converted
##Dissolve complete of wPressurizedMains
##Feature Vertices To Points complete
##Lat/Long fields added
##Lat/Long fields calculated
##Lat/Long Converted
##Latitude Field Calculation Complete
##Longitude Field Calculation Complete
##Deleted fields that were created from Coordinate Notation
##Deleted wPM_Dissolved_Points in dataset
##Copied wPM_Points_Converted into dataset as wPM_Dissolved_Vertices
##Deleted wPM_Points_Converted
##Deleted field - NetworkSegmentID
##Added field - SegmentID
##SegmentID Field Calculation Complete
##Deleted field - ORIG_FID
##Added field - VertexID
##wPM_Dissolved_Vertices sorted
##Cursor complete
##Deleted original wPM_Dissolved_Vertices
##Copied sorted wPM_Dissolved_Vertices
##Deleted sorted wPM_Dissolved_Vertices
##Privilege changes successful
##Ended: Tuesday, December 15, 2015 04:56:20 PM
##Elapsed Time: 2:55:56

############################################################################
############################################################################

import arcpy, time, datetime, string
from arcpy import env

## Start
ExecutionStartTime = datetime.datetime.now()
print "Started: %s" % ExecutionStartTime.strftime('%A, %B %d, %Y %I:%M:%S %p')
print "Processing"

## Target Location
env.workspace = r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde"
env.overwriteOutput = True
env.outputMFlag = "Disabled"
env.outputZFlag = "Disabled"

## Original sde files
valves      =  r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\Conway_sdeVector_sdeViewer.sde\sdeVector.SDEDATAOWNER.WaterUtility\sdeVector.SDEDATAOWNER.SystemValve"
hydrants    =  r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\Conway_sdeVector_sdeViewer.sde\sdeVector.SDEDATAOWNER.WaterUtility\sdeVector.SDEDATAOWNER.Hydrant"
meters      =  r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\Conway_sdeVector_sdeViewer.sde\sdeVector.SDEDATAOWNER.WaterUtility\sdeVector.SDEDATAOWNER.wServiceLocation"
mains       =  r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\Conway_sdeVector_sdeViewer.sde\sdeVector.SDEDATAOWNER.WaterUtility\sdeVector.SDEDATAOWNER.wPressurizedMain"

## Delete DataConversion Dataset if it exists
if arcpy.Exists(r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde\DataConversion"):
    arcpy.Delete_management(r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde\DataConversion")
    print "Old DataConversion Dataset deleted"

## Create Dataset with desired spatial reference - WGS
## http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/Projected_coordinate_systems/02r3000000vt000000/
sr = arcpy.SpatialReference(4326)
##sr = arcpy.SpatialReference(r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Data\Shapefiles\CoordinateFile.prj")
arcpy.CreateFeatureDataset_management(r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde", "DataConversion", sr)
print "DataConversion dataset created"

## Make Copy of original sde files to DQSQL_AssetWorksGIS_sdeDataOwner.sde\DataConversion
outLocation =       r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde\DataConversion"

copied_valves   =   "SystemValve"
copied_hydrants =   "Hydrant"
copied_meters   =   "wServiceLocation"
copied_mains    =   "wPressurizedMain"

config_keyword = "GEOGRAPHY"

arcpy.FeatureClassToFeatureClass_conversion(valves, outLocation, copied_valves, "", "", config_keyword)
print "SystemValve converted"
arcpy.FeatureClassToFeatureClass_conversion(hydrants, outLocation, copied_hydrants, "", "", config_keyword)
print "Hydrant converted"
arcpy.FeatureClassToFeatureClass_conversion(meters, outLocation, copied_meters, "", "", config_keyword)
print "wServiceLocation converted"
arcpy.FeatureClassToFeatureClass_conversion(mains, outLocation, copied_mains, "", "", config_keyword)
print "wPressurizedMain converted"

del env.workspace
env.workspace = r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde\DataConversion"

## Dissolve Tool on the wPressurizedMain feature class using NetworkSegmentID - single part not multipart
in_feature_class =  "wPressurizedMain"
out_feature_class = "wPressurizedMain_Dissolved"
dissolve_field =    "NetworkSegmentID"                                      ################## Was "District"...Running a test using this fields..."NetworkSegmentID" will be the actual field used ##################
multi_part =        "SINGLE_PART"

arcpy.Dissolve_management(in_feature_class, out_feature_class, dissolve_field, "", multi_part)
print "Dissolve complete of wPressurizedMains"

## Feature Vertices to Points
inFeatures =        "wPressurizedMain_Dissolved"
outFeatureClass =   "wPM_Dissolved_Points"
arcpy.FeatureVerticesToPoints_management(inFeatures, outFeatureClass, "ALL")
print "Feature Vertices To Points complete"

## Add Lat/Long Fields
inFeature =         "wPM_Dissolved_Points"
fieldName1 =        "Latitude"
fieldName2 =        "Longitude"
fieldType =         "DOUBLE"
arcpy.AddField_management(inFeature, fieldName1, fieldType)
arcpy.AddField_management(inFeature, fieldName2, fieldType)
print "Lat/Long fields added"

## Execute CalculateField of Lat/Long Fields
##      Example
##      lat = 37.016514     # !shape.extent.YMax!
##      lon x Coord = -76.42133 or -8522343.5726    # !shape.extent.XMax!

expression1 = "{0}".format("!SHAPE.extent.YMax!")
expression2 = "{0}".format("!SHAPE.extent.XMax!")

arcpy.CalculateField_management(inFeature, fieldName1, expression1, "PYTHON_9.3")
arcpy.CalculateField_management(inFeature, fieldName2, expression2, "PYTHON_9.3")

print "Lat/Long fields calculated"

del env.workspace
env.workspace = r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde"

## Convert Coordinate Notation with SHAPE as input field.
input_table = "wPM_Dissolved_Points"
output_points = "wPM_Points_Converted"
x_field = "Latitude"
y_field = "Longitude"
input_format = "SHAPE" 
output_format = "DD_2" 
id_field = ""
spatial_ref = arcpy.SpatialReference(4326)

arcpy.ConvertCoordinateNotation_management(input_table, output_points, x_field, y_field, input_format, output_format, id_field, spatial_ref)
print "Lat/Long Converted"

## Field Calculate
input_FeatureClass = "wPM_Points_Converted"
expression3 = "{0}".format("!DDLat![:-1]") 
expression4 = "{0}".format("!DDLon![1:-1]")
expression5 = "{0}".format("- !Longitude!")

arcpy.CalculateField_management(input_FeatureClass, "Latitude", expression3, "PYTHON_9.3")
print "Latitude Field Calculation Complete"
arcpy.CalculateField_management(input_FeatureClass, "Longitude", expression4, "PYTHON_9.3")
arcpy.CalculateField_management(input_FeatureClass, "Longitude", expression5, "PYTHON_9.3")
print "Longitude Field Calculation Complete"

## Delete fields that were created from Coordinate Notation
arcpy.DeleteField_management(input_FeatureClass,  ["DDLat", "DDLon"])
print "Deleted fields that were created from Coordinate Notation"

## Delete wPM_Dissolved_Points in dataset, Copy file to dataset, deleted file outside of dataset
deleteFC_1 =    r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde\AssetWorksGIS.SDEDATAOWNER.DataConversion\AssetWorksGIS.SDEDATAOWNER.wPM_Dissolved_Points"
copyFC_input =  "AssetWorksGIS.SDEDATAOWNER.wPM_Points_Converted"
wPM_Dissolved_Vertices = r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde\AssetWorksGIS.SDEDATAOWNER.DataConversion\AssetWorksGIS.SDEDATAOWNER.wPM_Dissolved_Vertices"

arcpy.Delete_management(deleteFC_1)
print "Deleted wPM_Dissolved_Points in dataset"
arcpy.Copy_management(copyFC_input, wPM_Dissolved_Vertices)
print "Copied wPM_Points_Converted into dataset as wPM_Dissolved_Vertices"
arcpy.Delete_management(copyFC_input)
print "Deleted wPM_Points_Converted"

## Delete field NetworkSegmentID from wPM_Dissolved_Vertices
arcpy.DeleteField_management(wPM_Dissolved_Vertices, "NetworkSegmentID")
print "Deleted field - NetworkSegmentID"

## Add field SegmentID (Long)
arcpy.AddField_management(wPM_Dissolved_Vertices, "SegmentID", "LONG")
print "Added field - SegmentID"

## Field Calculate SegmentID
expression = "{0}".format("!ORIG_FID!")
arcpy.CalculateField_management(wPM_Dissolved_Vertices, "SegmentID", expression, "PYTHON_9.3")
print "SegmentID Field Calculation Complete"
arcpy.DeleteField_management(wPM_Dissolved_Vertices, "ORIG_FID")
print "Deleted field - ORIG_FID"

## Add field VertexID (Long)
arcpy.AddField_management(wPM_Dissolved_Vertices, "VertexID", "LONG")
print "Added field - VertexID"

## Sort Ascending creating a new file
in_dataset = wPM_Dissolved_Vertices
out_dataset = r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde\AssetWorksGIS.SDEDATAOWNER.DataConversion\AssetWorksGIS.SDEDATAOWNER.wPM_Dissolved_Vertices_Sorted"
# Order features first by location (Shape) and then by WELL_YIELD
sort_fields = [["SegmentID", "ASCENDING"]]
# execute the function
arcpy.Sort_management(in_dataset, out_dataset, sort_fields)
print "wPM_Dissolved_Vertices sorted"

featureclass = out_dataset
fields = ['SegmentID', 'VertexID']
# Create update cursor for feature class 
with arcpy.da.UpdateCursor(featureclass, fields) as cursor:
    # For each row, evaluate the SegmentID value (index position of 0) and update VertexID (index position of 1)
    x = 1
    y = 1
    for row in cursor:
        if row[0] == x:
            row[1] = y
            y += 1
        else:
            x += 1
            y = 1
            if row[0] == x:
                row[1] = y
                y += 1

        # Update the cursor with the updated list
        cursor.updateRow(row)

del cursor
print "Cursor complete"

# Delete original wPM_Dissolved_Vertices
arcpy.Delete_management(wPM_Dissolved_Vertices)
print "Deleted original wPM_Dissolved_Vertices"
# Copy the sorted wPM_Dissolved_Vertices and then delete the original
fc = out_dataset
outfc = r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde\AssetWorksGIS.SDEDATAOWNER.DataConversion\AssetWorksGIS.SDEDATAOWNER.wPM_Dissolved_Vertices"
arcpy.CopyFeatures_management(fc, outfc)
print "Copied sorted wPM_Dissolved_Vertices"
arcpy.Delete_management(fc)
print "Deleted sorted wPM_Dissolved_Vertices"

## Execute ChangePrivileges
dataset =       r"\\arctic\gis_data\GIS_Private\DataResources\AssetworksDataConversion\Interfaces\DQSQL_AssetWorksGIS_sdeDataOwner.sde\DataConversion"
user_admin =    "gis_administrator"
user_viewer =   "gis_viewer"
view =          "GRANT"
edit1 =         "GRANT"
edit2 =         "REVOKE"

fclist = arcpy.ListFeatureClasses("","",dataset)

for fc in fclist:
    arcpy.ChangePrivileges_management(fc, user_admin, view, edit1)
    arcpy.ChangePrivileges_management(fc, user_viewer, view, edit2)

print "Privilege changes successful"

## Done
ExecutionEndTime = datetime.datetime.now()
ElapsedTime = ExecutionEndTime - ExecutionStartTime
print "Ended: %s" % ExecutionEndTime.strftime('%A, %B %d, %Y %I:%M:%S %p')
print "Elapsed Time: %s" % str(ElapsedTime).split('.')[0]
