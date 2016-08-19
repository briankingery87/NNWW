# ExportWeekday.py
# Barbara Gates
# November 10, 2015


# OVERVIEW
#
# This program exports data from the production ArcSDE geodatabase to a personal
# geodatabase and shapefiles for Basemap users.
#
# After this program exports data, Local Basemap users run "Update Local GIS
# Features" to copy the exported data to their machines.  They can then display
# the data in Local Basemap on their machines without a network connection.


# DETAILS
#
# This program does the following:
#
#   1.  Create a temporary personal geodatabase.
#
#   2.  Create empty datasets in the temporary geodatabase.
#
#           - If available, obtain spatial reference from datasets in production
#             database.
#           - Otherwise, obtain from datasets in existing (stale) basemap
#             database.
#
#   3.  For each feature class to be exported:
#
#           - Export the feature class from the production database to the
#             temporary personal geodatabase.
#           - If unsuccessful, export the feature class from the existing
#             (stale) basemap personal geodatabase to the temporary personal
#             geodatabase, instead.
#
#   4.  Replace the existing (stale) basemap personal geodatabase with the
#       temporary personal geodatabase.
#
#   5.  For each feature class to be exported to a shapefile:
#
#           - Export the feature class to a temporary shapefile.
#           - If successful, replace existing (stale) shapefile with the
#             temporary shapefile, create a spatial index, and create an
#             attribute index.
#
#   6.  Send email to administrative users which states whether the task
#       completed successfully.  Include error messages, if any.


# HOW TO RUN
#
# The task scheduler on Arctic runs this program every weekday morning.
#
# To run this program manually:
#
#   - In the task scheduler on Arctic, run it manually.
#
# or
#
#   - In Command Prompt on Arctic, run
#     C:\Python27\ArcGIS10.2\Python.exe \\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Datasets\Export\Code\Python\ExportWeekday.py
#
# or
#
#   - In Command Prompt on DevArctic, run
#     E:\Python27\ArcGIS10.2\Python.exe \\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Datasets\Export\Code\Python\ExportWeekday.py


# WARNING
#
# This program works in personal geodatabases.  However, 64-bit Python -- the
# default version on the 64-bit servers -- does NOT support personal
# geodatabases.  Therefore, on the 64-bit servers, this program must be run in
# 32-bit Python as shown above.


################################################################################

import arcpy, os, smtplib, sys
from datetime import datetime


def SdePath(path, dataElementName, databaseName='sdeVector', dataOwner='sdeDataOwner'):
    return os.path.join(path, '%s.%s.%s' % (databaseName, dataOwner, dataElementName))

MdbPath = os.path.join


#
# Engineering personal geodatabase
#
#   - Current data will be exported from this database.
#

# Database
EngineeringDB = r'\\Nile\Applications\Databases\Engineering\Data\Distribution_GeoDB.mdb'

# Feature class
EngineeringDB_MainBreaks = MdbPath( EngineeringDB, 'MainBreaks' )

#
# Production ArcSDE geodatabase
#
#   - Current data will be exported from this database.
#

# Database
ProductionDB = r'\\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Datasets\Export\Interfaces\Connections\Conway_sdeVector_sdeViewer.sde'

# Address locator
ProductionDB_CenterlineAddressPlusZone = os.path.join( ProductionDB, 'sdeDataOwner.CenterlineAddressPlusZone' )

# Datasets
ProductionDB_Cadastral               = SdePath( ProductionDB, 'Cadastral' )
ProductionDB_DSIPriority             = SdePath( ProductionDB, 'DSIPriority' )
ProductionDB_OtherUtility            = SdePath( ProductionDB, 'OtherUtility' )
ProductionDB_Reference               = SdePath( ProductionDB, 'Reference' )
ProductionDB_Structure               = SdePath( ProductionDB, 'Structure' )
ProductionDB_Transportation          = SdePath( ProductionDB, 'Transportation' )
ProductionDB_WaterUtility            = SdePath( ProductionDB, 'WaterUtility' )

# Feature classes
ProductionDB_BuildingFootprint               = SdePath( ProductionDB_Structure,      'BuildingFootprint' )
ProductionDB_Casing                          = SdePath( ProductionDB_WaterUtility,   'Casing' )
ProductionDB_CustomerServiceUnit             = SdePath( ProductionDB_Reference,      'CustomerServiceUnit' )
ProductionDB_DSIPrioritizedArea              = SdePath( ProductionDB_DSIPriority,    'DSIPrioritizedArea' )
ProductionDB_GravelDirtAreaBoundary          = SdePath( ProductionDB_Transportation, 'GravelDirtAreaBoundary' )
ProductionDB_HydrantAnno3000                 = SdePath( ProductionDB_WaterUtility,   'HydrantAnno3000' )
ProductionDB_MeterReadingUnit                = SdePath( ProductionDB_Reference,      'MeterReadingUnit' )
ProductionDB_OtherWaterSystem                = SdePath( ProductionDB_OtherUtility,   'OtherWaterSystem' )
ProductionDB_PavedArea                       = SdePath( ProductionDB_Transportation, 'PavedArea' )
ProductionDB_Photograph                      = SdePath( ProductionDB_Reference,      'Photograph' )
ProductionDB_PressurizedMainAnno3000         = SdePath( ProductionDB_WaterUtility,   'PressurizedMainAnno3000' )
ProductionDB_Road                            = SdePath( ProductionDB_Transportation, 'Road' )
ProductionDB_RoadAnno3000Unlinked            = SdePath( ProductionDB_Transportation, 'RoadAnno3000Unlinked')
ProductionDB_SystemValveAnno3000             = SdePath( ProductionDB_WaterUtility,   'SystemValveAnno3000' )
ProductionDB_v_ConstructionProject_EAM       = SdePath( ProductionDB,                'v_ConstructionProject_EAM' )
ProductionDB_v_DamInundationAreaCustomer     = SdePath( ProductionDB,                'v_DamInundationAreaCustomer' )
ProductionDB_v_EngineeringDrawingArea_EDM    = SdePath( ProductionDB,                'v_EngineeringDrawingArea_EDM' )
ProductionDB_v_Fireflow                      = SdePath( ProductionDB,                'v_Fireflow' )
ProductionDB_v_Hydrant_EAM                   = SdePath( ProductionDB,                'v_Hydrant_EAM' )
ProductionDB_v_SchematicDrawingArea_EDM      = SdePath( ProductionDB,                'v_SchematicDrawingArea_EDM' )
ProductionDB_v_SystemValve_EAM               = SdePath( ProductionDB,                'v_SystemValve_EAM' )
ProductionDB_v_SystemValveAbandoned_EAM      = SdePath( ProductionDB,                'v_SystemValveAbandoned_EAM' )
ProductionDB_v_Tier1CriticalCustomer         = SdePath( ProductionDB,                'v_Tier1CriticalCustomer' )
ProductionDB_v_Tier2CriticalCustomer         = SdePath( ProductionDB,                'v_Tier2CriticalCustomer' )
ProductionDB_v_VirginiaNaturalGasMapGridArea = SdePath( ProductionDB,                'v_VirginiaNaturalGasMapGridArea' )
ProductionDB_v_wLateral_EDM                  = SdePath( ProductionDB,                'v_wLateral_EDM' )
ProductionDB_v_wPressurizedMain              = SdePath( ProductionDB,                'v_wPressurizedMain' )
ProductionDB_v_wPressurizedMainAbandoned     = SdePath( ProductionDB,                'v_wPressurizedMainAbandoned' )
ProductionDB_v_wServiceLocation_EAM          = SdePath( ProductionDB,                'v_wServiceLocation_EAM' )
ProductionDB_WaterworksEasement              = SdePath( ProductionDB_Cadastral,      'WaterworksEasement' )
ProductionDB_WaterworksRightOfWayBoundary    = SdePath( ProductionDB_Cadastral,      'WaterworksRightOfWayBoundary' )
ProductionDB_wServiceLocationAnno3000        = SdePath( ProductionDB_WaterUtility,   'wServiceLocationAnno3000' )

#
# Temporary personal geodatabase
#
#   - Data will be exported into this database.
#
#   - After data has been exported, this database will replace the stale copy of
#     sdeVectorFrequent.mdb.
#

# Database
TemporaryDB = r'\\Arctic\Desktop_GIS\GIS\OperationalSystems\Locations\Waterworks BaseMap\Data\Databases\sdeVectorFrequent_Temp.mdb'

# Address locator
TemporaryDB_CenterlineAddressPlusZone = MdbPath( TemporaryDB, 'CenterlineAddressPlusZone' )

# Datasets
TemporaryDB_Cadastral               = MdbPath( TemporaryDB, 'Cadastral' )
TemporaryDB_DSIPriority             = MdbPath( TemporaryDB, 'DSIPriority' )
TemporaryDB_OtherUtility            = MdbPath( TemporaryDB, 'OtherUtility' )
TemporaryDB_Reference               = MdbPath( TemporaryDB, 'Reference' )
TemporaryDB_Structure               = MdbPath( TemporaryDB, 'Structure' )
TemporaryDB_Transportation          = MdbPath( TemporaryDB, 'Transportation' )
TemporaryDB_WaterUtility            = MdbPath( TemporaryDB, 'WaterUtility' )

# Feature classes
TemporaryDB_BuildingFootprint               = MdbPath( TemporaryDB_Structure,      'BuildingFootprint' )
TemporaryDB_Casing                          = MdbPath( TemporaryDB_WaterUtility,   'Casing' )
TemporaryDB_CustomerServiceUnit             = MdbPath( TemporaryDB_Reference,      'CustomerServiceUnit' )
TemporaryDB_DamInundationAreaCustomer       = MdbPath( TemporaryDB,                'DamInundationAreaCustomer' )
TemporaryDB_DSIPrioritizedArea              = MdbPath( TemporaryDB_DSIPriority,    'DSIPrioritizedArea' )
TemporaryDB_GravelDirtAreaBoundary          = MdbPath( TemporaryDB_Transportation, 'GravelDirtAreaBoundary' )
TemporaryDB_HydrantAnno3000                 = MdbPath( TemporaryDB,                'HydrantAnno3000' )
TemporaryDB_MeterReadingUnit                = MdbPath( TemporaryDB_Reference,      'MeterReadingUnit' )
TemporaryDB_OtherWaterSystem                = MdbPath( TemporaryDB_OtherUtility,   'OtherWaterSystem' )
TemporaryDB_PavedArea                       = MdbPath( TemporaryDB_Transportation, 'PavedArea' )
TemporaryDB_Photograph                      = MdbPath( TemporaryDB_Reference,      'Photograph' )
TemporaryDB_PressurizedMainAnno3000         = MdbPath( TemporaryDB,                'PressurizedMainAnno3000' )
TemporaryDB_Road                            = MdbPath( TemporaryDB_Transportation, 'Road' )
TemporaryDB_RoadAnno3000                    = MdbPath( TemporaryDB,                'RoadAnno3000' )
TemporaryDB_SystemValveAnno3000             = MdbPath( TemporaryDB,                'SystemValveAnno3000' )
TemporaryDB_v_ConstructionProject_EAM       = MdbPath( TemporaryDB_Reference,      'v_ConstructionProject_EAM' )
TemporaryDB_v_EngineeringDrawingArea_EDM    = MdbPath( TemporaryDB_Reference,      'v_EngineeringDrawingArea_EDM' )
TemporaryDB_v_Fireflow                      = MdbPath( TemporaryDB_WaterUtility,   'v_Fireflow' )
TemporaryDB_v_Hydrant_EAM                   = MdbPath( TemporaryDB_WaterUtility,   'v_Hydrant_EAM' )
TemporaryDB_v_SchematicDrawingArea_EDM      = MdbPath( TemporaryDB_Reference,      'v_SchematicDrawingArea_EDM' )
TemporaryDB_v_SystemValve_EAM               = MdbPath( TemporaryDB_WaterUtility,   'v_SystemValve_EAM' )
TemporaryDB_v_SystemValveAbandoned_EAM      = MdbPath( TemporaryDB_WaterUtility,   'v_SystemValveAbandoned_EAM' )
TemporaryDB_v_Tier1CriticalCustomer         = MdbPath( TemporaryDB_WaterUtility,   'v_Tier1CriticalCustomer' )
TemporaryDB_v_Tier2CriticalCustomer         = MdbPath( TemporaryDB_WaterUtility,   'v_Tier2CriticalCustomer' )
TemporaryDB_v_VirginiaNaturalGasMapGridArea = MdbPath( TemporaryDB_Reference,      'v_VirginiaNaturalGasMapGridArea' )
TemporaryDB_v_wLateral_EDM                  = MdbPath( TemporaryDB_WaterUtility,   'v_wLateral_EDM' )
TemporaryDB_v_wPressurizedMain              = MdbPath( TemporaryDB_WaterUtility,   'v_wPressurizedMain' )
TemporaryDB_v_wPressurizedMainAbandoned     = MdbPath( TemporaryDB_WaterUtility,   'v_wPressurizedMainAbandoned' )
TemporaryDB_v_wServiceLocation_EAM          = MdbPath( TemporaryDB_WaterUtility,   'v_wServiceLocation_EAM' )
TemporaryDB_WaterworksEasement              = MdbPath( TemporaryDB_Cadastral,      'WaterworksEasement' )
TemporaryDB_WaterworksRightOfWayBoundary    = MdbPath( TemporaryDB_Cadastral,      'WaterworksRightOfWayBoundary' )
TemporaryDB_wServiceLocationAnno3000        = MdbPath( TemporaryDB,                'wServiceLocationAnno3000' )

#
# Basemap personal geodatabase
#
#   - If data cannot be exported from the production ArcSDE geodatabase, stale
#     data will be exported from this database instead.
#
#   - This database will be replaced by the temporary personal geodatabase.
#

# Database
BasemapDB = r'\\Arctic\Desktop_GIS\GIS\OperationalSystems\Locations\Waterworks BaseMap\Data\Databases\sdeVectorFrequent.mdb'

# Address locator
BasemapDB_CenterlineAddressPlusZone = MdbPath( BasemapDB, 'CenterlineAddressPlusZone')

# Datasets
BasemapDB_Cadastral               = MdbPath( BasemapDB, 'Cadastral' )
BasemapDB_DSIPriority             = MdbPath( BasemapDB, 'DSIPriority' )
BasemapDB_OtherUtility            = MdbPath( BasemapDB, 'OtherUtility' )
BasemapDB_Reference               = MdbPath( BasemapDB, 'Reference' )
BasemapDB_Structure               = MdbPath( BasemapDB, 'Structure' )
BasemapDB_Transportation          = MdbPath( BasemapDB, 'Transportation' )
BasemapDB_WaterUtility            = MdbPath( BasemapDB, 'WaterUtility' )

# Feature classes
BasemapDB_BuildingFootprint               = MdbPath( BasemapDB_Structure,      'BuildingFootprint' )
BasemapDB_Casing                          = MdbPath( BasemapDB_WaterUtility,   'Casing' )
BasemapDB_CustomerServiceUnit             = MdbPath( BasemapDB_Reference,      'CustomerServiceUnit' )
BasemapDB_DamInundationAreaCustomer       = MdbPath( BasemapDB,                'DamInundationAreaCustomer' )
BasemapDB_DSIPrioritizedArea              = MdbPath( BasemapDB_DSIPriority,    'DSIPrioritizedArea' )
BasemapDB_GravelDirtAreaBoundary          = MdbPath( BasemapDB_Transportation, 'GravelDirtAreaBoundary' )
BasemapDB_HydrantAnno3000                 = MdbPath( BasemapDB,                'HydrantAnno3000' )
BasemapDB_MeterReadingUnit                = MdbPath( BasemapDB_Reference,      'MeterReadingUnit' )
BasemapDB_OtherWaterSystem                = MdbPath( BasemapDB_OtherUtility,   'OtherWaterSystem' )
BasemapDB_PavedArea                       = MdbPath( BasemapDB_Transportation, 'PavedArea' )
BasemapDB_Photograph                      = MdbPath( BasemapDB_Reference,      'Photograph' )
BasemapDB_PressurizedMainAnno3000         = MdbPath( BasemapDB,                'PressurizedMainAnno3000' )
BasemapDB_Road                            = MdbPath( BasemapDB_Transportation, 'Road' )
BasemapDB_RoadAnno3000                    = MdbPath( BasemapDB,                'RoadAnno3000' )
BasemapDB_SystemValveAnno3000             = MdbPath( BasemapDB,                'SystemValveAnno3000' )
BasemapDB_v_ConstructionProject_EAM       = MdbPath( BasemapDB_Reference,      'v_ConstructionProject_EAM' )
BasemapDB_v_EngineeringDrawingArea_EDM    = MdbPath( BasemapDB_Reference,      'v_EngineeringDrawingArea_EDM' )
BasemapDB_v_Fireflow                      = MdbPath( BasemapDB_WaterUtility,   'v_Fireflow' )
BasemapDB_v_Hydrant_EAM                   = MdbPath( BasemapDB_WaterUtility,   'v_Hydrant_EAM' )
BasemapDB_v_SchematicDrawingArea_EDM      = MdbPath( BasemapDB_Reference,      'v_SchematicDrawingArea_EDM' )
BasemapDB_v_SystemValve_EAM               = MdbPath( BasemapDB_WaterUtility,   'v_SystemValve_EAM' )
BasemapDB_v_SystemValveAbandoned_EAM      = MdbPath( BasemapDB_WaterUtility,   'v_SystemValveAbandoned_EAM' )
BasemapDB_v_Tier1CriticalCustomer         = MdbPath( BasemapDB_WaterUtility,   'v_Tier1CriticalCustomer' )
BasemapDB_v_Tier2CriticalCustomer         = MdbPath( BasemapDB_WaterUtility,   'v_Tier2CriticalCustomer' )
BasemapDB_v_VirginiaNaturalGasMapGridArea = MdbPath( BasemapDB_Reference,      'v_VirginiaNaturalGasMapGridArea' )
BasemapDB_v_wLateral_EDM                  = MdbPath( BasemapDB_WaterUtility,   'v_wLateral_EDM' )
BasemapDB_v_wPressurizedMain              = MdbPath( BasemapDB_WaterUtility,   'v_wPressurizedMain' )
BasemapDB_v_wPressurizedMainAbandoned     = MdbPath( BasemapDB_WaterUtility,   'v_wPressurizedMainAbandoned' )
BasemapDB_v_wServiceLocation_EAM          = MdbPath( BasemapDB_WaterUtility,   'v_wServiceLocation_EAM' )
BasemapDB_WaterworksEasement              = MdbPath( BasemapDB_Cadastral,      'WaterworksEasement' )
BasemapDB_WaterworksRightOfWayBoundary    = MdbPath( BasemapDB_Cadastral,      'WaterworksRightOfWayBoundary' )
BasemapDB_wServiceLocationAnno3000        = MdbPath( BasemapDB,                'wServiceLocationAnno3000' )

#
# Shapefiles
#
#   - Data will be exported into these shapefiles.
#

Shapefile_Mainbreaks = r'\\Arctic\Desktop_GIS\ArcData\MainBreaks\MainBrks.shp'
IndexFields_MainBreaks = 'WKORDER;FISCALYEAR;ABANDONED;TYPE'

Shapefile_v_EngineeringDrawingArea_EDM = r'\\Arctic\Desktop_GIS\GIS\OperationalSystems\Locations\Waterworks BaseMap\Data\Shapefiles\Drawings\cadindex.shp'
IndexFields_v_EngineeringDrawingArea_EDM = 'CYCOSTATUS;ID;JURISDCTN;C_SHEETNUM;C_SSN;CO_STATUS;ENTRY_DATE;GIS;PFN_NO;PFN_EX;WW_NO;BOX_NO'

#
# Status mail
#
#   - Execution status email will be sent to these administrative users.
#

MailServer = 'nnww-smtp.nnww.nnva.gov'
MailRecipientsIfSuccess = ['Marietta Washington <mvwashington@nnva.gov>',
                           'Brian Kingery <bkingery@nnva.gov>',
                           'Barbara Gates <bgates@nnva.gov>']
MailRecipientsIfError   = ['Marietta Washington <mvwashington@nnva.gov>',
                           'Brian Kingery <bkingery@nnva.gov>',
                           'Barbara Gates <bgates@nnva.gov>']

#
# Log file
#
#   - Error messages will be recorded in this file.
#

LogFile = r'\\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Datasets\Export\Code\Python\ExportWeekday_Errors.txt'

################################################################################

StartDateTime = datetime.now()
ErrorMsgs = []

def ExportWeekday():
    global ErrorMsgs, StartDateTime
    StartDateTime = datetime.now()
    #
    # Initialize ErrorMsgs to an emppty list.
    # Throughout this procedure, catch exceptions and append their error messages onto this list.
    # Before terminating, email this list to administrative users.
    #
    ErrorMsgs = []
    #
    # Create temporary database.
    #
    CreateDatabase( TemporaryDB )
    #
    # Export address locator from production database to temporary database.
    # If unsuccessful, export address locator from existing (stale) basemap database instead.
    #
    ExportAddressLocator( ProductionDB_CenterlineAddressPlusZone, BasemapDB_CenterlineAddressPlusZone, TemporaryDB_CenterlineAddressPlusZone )
    #
    # Create empty datasets in temporary database.
    # If available, obtain spatial reference information from datasets in production database.
    # Otherwise, obtain it from datasets in existing (stale) basemap database.
    #
    CreateDataset( ProductionDB_Cadastral,               BasemapDB_Cadastral,               TemporaryDB_Cadastral )
    CreateDataset( ProductionDB_DSIPriority,             BasemapDB_DSIPriority,             TemporaryDB_DSIPriority )
    CreateDataset( ProductionDB_OtherUtility,            BasemapDB_OtherUtility,            TemporaryDB_OtherUtility )
    CreateDataset( ProductionDB_Reference,               BasemapDB_Reference,               TemporaryDB_Reference )
    CreateDataset( ProductionDB_Structure,               BasemapDB_Structure,               TemporaryDB_Structure )
    CreateDataset( ProductionDB_Transportation,          BasemapDB_Transportation,          TemporaryDB_Transportation )
    CreateDataset( ProductionDB_WaterUtility,            BasemapDB_WaterUtility,            TemporaryDB_WaterUtility )
    #
    # For each feature class to be exported:
    #   Export feature class from production database to temporary database.
    #   If unsuccessful, export feature class from existing (stale) basemap database instead.
    #
    ExportFeatureClass( ProductionDB_BuildingFootprint,               BasemapDB_BuildingFootprint,               TemporaryDB_BuildingFootprint )
    ExportFeatureClass( ProductionDB_Casing,                          BasemapDB_Casing,                          TemporaryDB_Casing )
    ExportFeatureClass( ProductionDB_CustomerServiceUnit,             BasemapDB_CustomerServiceUnit,             TemporaryDB_CustomerServiceUnit )
    ExportFeatureClass( ProductionDB_DSIPrioritizedArea,              BasemapDB_DSIPrioritizedArea,              TemporaryDB_DSIPrioritizedArea )
    ExportFeatureClass( ProductionDB_GravelDirtAreaBoundary,          BasemapDB_GravelDirtAreaBoundary,          TemporaryDB_GravelDirtAreaBoundary )
    ExportFeatureClass( ProductionDB_HydrantAnno3000,                 BasemapDB_HydrantAnno3000,                 TemporaryDB_HydrantAnno3000 )
    ExportFeatureClass( ProductionDB_MeterReadingUnit,                BasemapDB_MeterReadingUnit,                TemporaryDB_MeterReadingUnit )
    ExportFeatureClass( ProductionDB_OtherWaterSystem,                BasemapDB_OtherWaterSystem,                TemporaryDB_OtherWaterSystem )
    ExportFeatureClass( ProductionDB_PavedArea,                       BasemapDB_PavedArea,                       TemporaryDB_PavedArea )
    ExportFeatureClass( ProductionDB_Photograph,                      BasemapDB_Photograph,                      TemporaryDB_Photograph )
    ExportFeatureClass( ProductionDB_PressurizedMainAnno3000,         BasemapDB_PressurizedMainAnno3000,         TemporaryDB_PressurizedMainAnno3000 )
    ExportFeatureClass( ProductionDB_Road,                            BasemapDB_Road,                            TemporaryDB_Road )
    ExportFeatureClass( ProductionDB_RoadAnno3000Unlinked,            BasemapDB_RoadAnno3000,                    TemporaryDB_RoadAnno3000 )
    ExportFeatureClass( ProductionDB_SystemValveAnno3000,             BasemapDB_SystemValveAnno3000,             TemporaryDB_SystemValveAnno3000 )
    ExportFeatureClass( ProductionDB_WaterworksEasement,              BasemapDB_WaterworksEasement,              TemporaryDB_WaterworksEasement )
    ExportFeatureClass( ProductionDB_WaterworksRightOfWayBoundary,    BasemapDB_WaterworksRightOfWayBoundary,    TemporaryDB_WaterworksRightOfWayBoundary )
    ExportFeatureClass( ProductionDB_v_ConstructionProject_EAM,       BasemapDB_v_ConstructionProject_EAM,       TemporaryDB_v_ConstructionProject_EAM )
    ExportFeatureClass( ProductionDB_v_DamInundationAreaCustomer,     BasemapDB_DamInundationAreaCustomer,       TemporaryDB_DamInundationAreaCustomer )
    ExportFeatureClass( ProductionDB_v_EngineeringDrawingArea_EDM,    BasemapDB_v_EngineeringDrawingArea_EDM,    TemporaryDB_v_EngineeringDrawingArea_EDM )
    ExportFeatureClass( ProductionDB_v_Fireflow,                      BasemapDB_v_Fireflow,                      TemporaryDB_v_Fireflow )
    ExportFeatureClass( ProductionDB_v_Hydrant_EAM,                   BasemapDB_v_Hydrant_EAM,                   TemporaryDB_v_Hydrant_EAM )
    ExportFeatureClass( ProductionDB_v_SchematicDrawingArea_EDM,      BasemapDB_v_SchematicDrawingArea_EDM,      TemporaryDB_v_SchematicDrawingArea_EDM )
    ExportFeatureClass( ProductionDB_v_SystemValve_EAM,               BasemapDB_v_SystemValve_EAM,               TemporaryDB_v_SystemValve_EAM )
    ExportFeatureClass( ProductionDB_v_SystemValveAbandoned_EAM,      BasemapDB_v_SystemValveAbandoned_EAM,      TemporaryDB_v_SystemValveAbandoned_EAM )
    ExportFeatureClass( ProductionDB_v_Tier1CriticalCustomer,         BasemapDB_v_Tier1CriticalCustomer,         TemporaryDB_v_Tier1CriticalCustomer )
    ExportFeatureClass( ProductionDB_v_Tier2CriticalCustomer,         BasemapDB_v_Tier2CriticalCustomer,         TemporaryDB_v_Tier2CriticalCustomer )
    ExportFeatureClass( ProductionDB_v_VirginiaNaturalGasMapGridArea, BasemapDB_v_VirginiaNaturalGasMapGridArea, TemporaryDB_v_VirginiaNaturalGasMapGridArea )
    ExportFeatureClass( ProductionDB_v_wLateral_EDM,                  BasemapDB_v_wLateral_EDM,                  TemporaryDB_v_wLateral_EDM )
    ExportFeatureClass( ProductionDB_v_wPressurizedMain,              BasemapDB_v_wPressurizedMain,              TemporaryDB_v_wPressurizedMain )
    ExportFeatureClass( ProductionDB_v_wPressurizedMainAbandoned,     BasemapDB_v_wPressurizedMainAbandoned,     TemporaryDB_v_wPressurizedMainAbandoned )
    ExportFeatureClass( ProductionDB_v_wServiceLocation_EAM,          BasemapDB_v_wServiceLocation_EAM,          TemporaryDB_v_wServiceLocation_EAM )
    ExportFeatureClass( ProductionDB_wServiceLocationAnno3000,        BasemapDB_wServiceLocationAnno3000,        TemporaryDB_wServiceLocationAnno3000 )
    #
    # Replace existing (stale) basemap database with temporary database
    # to make it available to basemap users.
    #
    CompactDatabase( TemporaryDB )
    DeleteDatabase( BasemapDB )
    RenameDatabase( TemporaryDB, BasemapDB )
    #
    # For each feature class to be exported to a shapefile:
    #   Export feature class to temporary shapefile.
    #   If successful:
    #       Replace existing (stale) shapefile with temporary shapefile
    #       to make it available to basemap users.
    #       Create spatial index.
    #       Create attribute index.
    #
    ExportToShapefile( ProductionDB_v_EngineeringDrawingArea_EDM, Shapefile_v_EngineeringDrawingArea_EDM, IndexFields_v_EngineeringDrawingArea_EDM )
    ExportToShapefile( EngineeringDB_MainBreaks,                  Shapefile_Mainbreaks,                   IndexFields_MainBreaks )
    #
    # Write error messages to log file
    #
    LogErrors( )
    #
    # Send email which states whether task succeeded or failed.
    # Include error messages, if any.
    #
    SendStatusMail( )

def CreateDatabase(newDB):
    print 'Creating database', newDB, '...'
    try:
        if arcpy.Exists(newDB):
            arcpy.Delete_management(newDB)
        (path, file) = os.path.split(newDB)
        version = '10.0'
        arcpy.CreatePersonalGDB_management(path, file, version)
    except BaseException, e:
        ErrorMsgs.append('CreateDatabase(newDB=%s)' % newDB)
        ErrorMsgs.append(e)

def CompactDatabase(DB):
    print 'Compacting database', DB, '...'
    try:
        arcpy.Compact_management(DB)
    except BaseException, e:
        ErrorMsgs.append('CompactDatabase(DB=%s)' % DB)
        ErrorMsgs.append(e)

def DeleteDatabase(DB):
    print 'Deleting database', DB, '...'
    try:
        if arcpy.Exists(DB):
            arcpy.Delete_management(DB)
    except BaseException, e:
        ErrorMsgs.append('DeleteDatabase(DB=%s)' % DB)
        ErrorMsgs.append(e)

def RenameDatabase(oldDB, newDB):
    print 'Renaming database', oldDB, 'to', newDB, '...'
    try:
        arcpy.Rename_management(oldDB, newDB)
    except BaseException, e:
        ErrorMsgs.append('RenameDatabase(oldDB=%s, newDB=%s)' % (oldDB, newDB))
        ErrorMsgs.append(e)

def CreateDataset(spatialRefDataset, spatialRefDatasetAlt, newDataset):
    print 'Creating empty dataset', newDataset, '...'
    try:
        (newDatasetFolder, newDatasetName) = os.path.split(newDataset)
        arcpy.CreateFeatureDataset_management(newDatasetFolder, newDatasetName, spatialRefDataset)
    except BaseException, e:
        ErrorMsgs.append('CreateDataset(spatialRefDataset=%s, newDataset=%s)' % (spatialRefDataset, newDataset))
        ErrorMsgs.append(e)
        ErrorMsgs.append('Trying to create dataset from stale data source "%s" instead' % spatialRefDatasetAlt)
        try:
            arcpy.CreateFeatureDataset_management(newDatasetFolder, newDatasetName, spatialRefDatasetAlt)
        except BaseException, e:
            ErrorMsgs.append('CreateDataset(spatialRefDatasetAlt=%s, newDataset=%s)' % (spatialRefDatasetAlt, newDataset))
            ErrorMsgs.append(e)

def ExportAddressLocator(sourceAddressLocator, sourceAddressLocatorAlt, destAddressLocator):
    print 'Exporting address locator', sourceAddressLocator, '...'
    try:
        arcpy.Copy_management(sourceAddressLocator, destAddressLocator)
    except BaseException, e:
        ErrorMsgs.append('ExportAddressLocator(sourceAddressLocator=%s, destAddressLocator=%s)' % (sourceAddressLocator, destAddressLocator))
        ErrorMsgs.append(e)
        ErrorMsgs.append('Trying to export address locator from stale data source "%s" instead' % sourceAddressLocatorAlt)
        try:
            arcpy.Copy_management(sourceAddressLocatorAlt, destAddressLocator)
        except BaseException, e:
            ErrorMsgs.append('ExportAddressLocator(sourceAddressLocatorAlt=%s, destAddressLocator=%s)' % (sourceAddressLocatorAlt, destAddressLocator))
            ErrorMsgs.append(e)

def ExportFeatureClass(sourceFeatureClass, sourceFeatureClassAlt, destFeatureClass):
    print 'Exporting feature class', sourceFeatureClass, '...'
    try:
        (destDataset, destFeatureClassName) = os.path.split(destFeatureClass)
        arcpy.FeatureClassToFeatureClass_conversion(sourceFeatureClass, destDataset, destFeatureClassName)
    except BaseException, e:
        ErrorMsgs.append('ExportFeatureClass(sourceFeatureClass=%s, destFeatureClass=%s)' % (sourceFeatureClass, destFeatureClass))
        ErrorMsgs.append(e)
        ErrorMsgs.append('Trying to export feature class from stale data source "%s" instead' % sourceFeatureClassAlt)
        try:
            arcpy.FeatureClassToFeatureClass_conversion(sourceFeatureClassAlt, destDataset, destFeatureClassName)
        except BaseException, e:
            ErrorMsgs.append('ExportFeatureClass(sourceFeatureClassAlt=%s, destFeatureClass=%s)' % (sourceFeatureClassAlt, destFeatureClass))
            ErrorMsgs.append(e)

def ExportToShapefile(featureClass, shapefile, indexFields):
    print 'Exporting to shapefile', shapefile, '...'
    try:
        # Export feature class to temporary shapefile.
        # If successful, replace existing (stale) shapefile with temporary shapefile
        # to make it available to basemap users.
        (shapefilePath, shapefileName) = os.path.split(shapefile)
        (shapefileNameBase, shapefileNameExt) = os.path.splitext(shapefileName)
        tempShapefileName = '%s_Temp%s' % (shapefileNameBase, shapefileNameExt)
        tempShapefile = os.path.join(shapefilePath, tempShapefileName)
        if arcpy.Exists(tempShapefile):
            arcpy.Delete_management(tempShapefile)
        arcpy.FeatureClassToFeatureClass_conversion(featureClass, shapefilePath, tempShapefileName)
        if arcpy.Exists(shapefile):
            arcpy.Delete_management(shapefile)
        arcpy.Rename_management(tempShapefile, shapefile)
    except BaseException, e:
        # Export failed.
        ErrorMsgs.append('ExportToShapefile(featureClass=%s, shapefile=%s)' % (featureClass, shapefile))
        ErrorMsgs.append(e)
        ErrorMsgs.append('Using stale shapefile "%s" instead' % shapefile)
    else:
        # Export succeeded.
        # Create spatial index and attribute index.
        try:
            arcpy.AddSpatialIndex_management(shapefile)
        except BaseException, e:
            ErrorMsgs.append('arcpy.AddSpatialIndex_management(shapefile=%s)' % shapefile)
            ErrorMsgs.append(e)
        try:
            arcpy.AddIndex_management(shapefile, indexFields, '')
        except BaseException, e:
            ErrorMsgs.append("arcpy.AddIndex_management(shapefile=%s, indexFields=%s, '')" % (shapefile, indexFields))
            ErrorMsgs.append(e)

def LogErrors():
    f = open(LogFile, 'w')
    f.writelines(map(lambda e: '%s\r\n' % str(e), ErrorMsgs))
    f.close()
            
def SendStatusMail():
    print 'Sending mail ...'
    if not ErrorMsgs:
        status = 'Success'
        mailRecipients = MailRecipientsIfSuccess
    else:
        status = 'Error'
        mailRecipients = MailRecipientsIfError
    task = sys.argv[0]
    taskName = os.path.basename(task)
    endDateTime = datetime.now()
    elapsedTime = endDateTime - StartDateTime
    mailSender = '%s <%s@nnva.gov>' % (os.environ['USERNAME'], os.environ['USERNAME'])
    subject = 'Scheduled Task - %s - %s' % (taskName, status)
    body = 'Scheduled Task:   %s\r\n\r\n' % taskName
    body += 'Server:  %s\r\n\r\n' % os.environ['COMPUTERNAME'].lower()
    body += 'Started:   %s\r\n\r\n' % StartDateTime.strftime('%A, %B %d, %Y %I:%M:%S %p')
    body += 'Terminated:   %s\r\n\r\n' % endDateTime.strftime('%A, %B %d, %Y %I:%M %p')
    body += 'Elapsed Time:  %s\r\n\r\n' % str(elapsedTime).split('.')[0]
    body += 'Status:   %s\r\n\r\n' % status
    if ErrorMsgs:
        body += 'Error Messages:\r\n\r\n'
        for e in ErrorMsgs:
            body += '%s\r\n' % str(e)
        body += '\r\n'
    body += 'This is an automatically generated message.  Please do not reply.\r\n'
    message = ''
    message += 'From: %s\r\n' % mailSender
    message += 'To: %s\r\n' % ', '.join(mailRecipients)
    message += 'Subject: %s\r\n\r\n' % subject
    message += '%s\r\n' % body
    server = smtplib.SMTP(MailServer)
    server.sendmail(mailSender, mailRecipients, message)
    server.quit()

################################################################################

ExportWeekday()
