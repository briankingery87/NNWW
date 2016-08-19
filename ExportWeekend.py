# ExportWeekend.py
# Barbara Gates
# 10/14/15


# OVERVIEW
#
# This program exports data from the production ArcSDE geodatabase to a personal
# geodatabase for Basemap users.
#
# After this program exports data, Local Basemap users run "Update Local GIS
# Features" to copy the exported data to their machines.  They can then display
# the data in Local Basemap on their machines without a network connection.


# DETAILS
#
# This program does the following:
#
#   1.  Create a temporary geodatabase by copying a template personal
#       geodatabase.  (The template contains feature classes that are never
#       updated.)
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
#           - If unsuccessful, export feature class from existing (stale)
#             basemap personal geodatabase to the temporary personal
#             geodatabase, instead.
#
#   4.  Replace existing (stale) basemap personal geodatabase with the temporary
#       personal geodatabase to make it available to basemap users.
#
#   5.  Send email to administrative users which states whether the task
#       completed successfully.  Include error messages, if any.


# HOW TO RUN
#
# The task scheduler on Arctic runs this program every weekend.
#
# To run this program manually:
#
#   - In the task scheduler on Arctic, run it manually.
#
# or
#
#   - In Command Prompt on Arctic, run
#     C:\Python27\ArcGIS10.2\Python.exe \\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Datasets\Export\Code\Python\ExportWeekend.py
#
# or
#
#   - In Command Prompt on DevArctic, run
#     E:\Python27\ArcGIS10.2\Python.exe \\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Datasets\Export\Code\Python\ExportWeekend.py


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
# Production ArcSDE geodatabase
#
#   - Current data will be exported from this database.
#

# Database
ProductionDB = r'\\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Datasets\Export\Interfaces\Connections\Conway_sdeVector_sdeViewer.sde'

# Datasets
ProductionDB_Administrative          = SdePath( ProductionDB, 'Administrative' )
ProductionDB_Cadastral               = SdePath( ProductionDB, 'Cadastral' )
#ProductionDB_Elevation               = SdePath( ProductionDB, 'Elevation' ) # This dataset is included in the template database.
ProductionDB_EmergencyManagement     = SdePath( ProductionDB, 'EmergencyManagement' )
ProductionDB_EnvironmentalManagement = SdePath( ProductionDB, 'EnvironmentalManagement' )
ProductionDB_GeodeticControl         = SdePath( ProductionDB, 'GeodeticControl' )
ProductionDB_Hydrography             = SdePath( ProductionDB, 'Hydrography' )
ProductionDB_OtherUtility            = SdePath( ProductionDB, 'OtherUtility' )
ProductionDB_Reference               = SdePath( ProductionDB, 'Reference' )
ProductionDB_Structure               = SdePath( ProductionDB, 'Structure' )
ProductionDB_SurfaceOverlay          = SdePath( ProductionDB, 'SurfaceOverlay' )
ProductionDB_Transportation          = SdePath( ProductionDB, 'Transportation' )
ProductionDB_WaterUtility            = SdePath( ProductionDB, 'WaterUtility' )

# Feature classes
ProductionDB_MunicipalArea                    = SdePath( ProductionDB_Administrative,          'MunicipalArea' )
ProductionDB_MunicipalBoundary                = SdePath( ProductionDB_Administrative,          'MunicipalBoundary' )
ProductionDB_PeninsulaMunicipalArea           = SdePath( ProductionDB_Administrative,          'PeninsulaMunicipalArea' )
ProductionDB_FederalReservationArea           = SdePath( ProductionDB_Cadastral,               'FederalReservationArea' )
ProductionDB_NationalPark                     = SdePath( ProductionDB_Cadastral,               'NationalPark' )
ProductionDB_OpenSpaceEasement                = SdePath( ProductionDB_Cadastral,               'OpenSpaceEasement' )
ProductionDB_RealPropertyParcel               = SdePath( ProductionDB_Cadastral,               'RealPropertyParcel' )
ProductionDB_WaterworksProperty               = SdePath( ProductionDB_Cadastral,               'WaterworksProperty' )
#ProductionDB_TenFootContour                   = SdePath( ProductionDB_Elevation,               'TenFootContour' ) # This feature class is included in the template database.
#ProductionDB_TwoFootContour                   = SdePath( ProductionDB_Elevation,               'TwoFootContour' ) # This feature class is included in the template database.
ProductionDB_DamInundationAreaFullPMF         = SdePath( ProductionDB_EmergencyManagement,     'DamInundationAreaFullPMF' )
ProductionDB_DamInundationAreaRoad            = SdePath( ProductionDB_EmergencyManagement,     'DamInundationAreaRoad' )
ProductionDB_DamInundationAreaServiceLocation = SdePath( ProductionDB_EmergencyManagement,     'DamInundationAreaServiceLocation' )
ProductionDB_DamInundationParcel              = SdePath( ProductionDB_EmergencyManagement,     'DamInundationParcel' )
ProductionDB_EmergencyManagementLocation      = SdePath( ProductionDB_EmergencyManagement,     'EmergencyManagementLocation' )
ProductionDB_BeaverDam                        = SdePath( ProductionDB_EnvironmentalManagement, 'BeaverDam' )
ProductionDB_ColonialPipelineSpillArea        = SdePath( ProductionDB_EnvironmentalManagement, 'ColonialPipelineSpillArea' )
ProductionDB_Culvert                          = SdePath( ProductionDB_EnvironmentalManagement, 'Culvert' )
ProductionDB_EcologicallySensitiveArea        = SdePath( ProductionDB_EnvironmentalManagement, 'EcologicallySensitiveArea' )
ProductionDB_HistoricArea                     = SdePath( ProductionDB_EnvironmentalManagement, 'HistoricArea' )
ProductionDB_HistoricLocation                 = SdePath( ProductionDB_EnvironmentalManagement, 'HistoricLocation' )
ProductionDB_RawSampleLocation                = SdePath( ProductionDB_EnvironmentalManagement, 'RawSampleLocation' )
ProductionDB_ResidualsMonitoringSite          = SdePath( ProductionDB_EnvironmentalManagement, 'ResidualsMonitoringSite' )
ProductionDB_ResidualsParcel                  = SdePath( ProductionDB_EnvironmentalManagement, 'ResidualsParcel' )
ProductionDB_RookeryArea                      = SdePath( ProductionDB_EnvironmentalManagement, 'RookeryArea' )
ProductionDB_TreeStand                        = SdePath( ProductionDB_EnvironmentalManagement, 'TreeStand' )
ProductionDB_VirginiaPollutionAbatementWell   = SdePath( ProductionDB_EnvironmentalManagement, 'VirginiaPollutionAbatementWell' )
ProductionDB_Watershed                        = SdePath( ProductionDB_EnvironmentalManagement, 'Watershed' )
ProductionDB_WatershedBoundary                = SdePath( ProductionDB_EnvironmentalManagement, 'WatershedBoundary' )
ProductionDB_WatershedProtectionMapGrid       = SdePath( ProductionDB_EnvironmentalManagement, 'WatershedProtectionMapGrid' )
ProductionDB_WatershedStreamProtectionBuffer  = SdePath( ProductionDB_EnvironmentalManagement, 'WatershedStreamProtectionBuffer' )
ProductionDB_Wetland                          = SdePath( ProductionDB_EnvironmentalManagement, 'Wetland' )
ProductionDB_Wildfire                         = SdePath( ProductionDB_EnvironmentalManagement, 'Wildfire' )
ProductionDB_Monument                         = SdePath( ProductionDB_GeodeticControl,         'Monument' )
ProductionDB_Dam                              = SdePath( ProductionDB_Hydrography,             'Dam' )
ProductionDB_HydroArea                        = SdePath( ProductionDB_Hydrography,             'HydroArea' )
ProductionDB_HydroAreaWatershed               = SdePath( ProductionDB_Hydrography,             'HydroAreaWatershed' )
ProductionDB_HydroLine                        = SdePath( ProductionDB_Hydrography,             'HydroLine' )
ProductionDB_HydroPoint                       = SdePath( ProductionDB_Hydrography,             'HydroPoint' )
ProductionDB_ColonialPipeline                 = SdePath( ProductionDB_OtherUtility,            'ColonialPipeline' )
ProductionDB_DominionElectricEasement         = SdePath( ProductionDB_OtherUtility,            'DominionElectricEasement' )
ProductionDB_FiberOpticLine                   = SdePath( ProductionDB_OtherUtility,            'FiberOpticLine' )
ProductionDB_Sewer                            = SdePath( ProductionDB_OtherUtility,            'Sewer' )
ProductionDB_VirginiaNaturalGasMain           = SdePath( ProductionDB_OtherUtility,            'VirginiaNaturalGasMain' )
ProductionDB_ADCVirginiaPeninsulaGridArea     = SdePath( ProductionDB_Reference,               'ADCVirginiaPeninsulaGridArea' )
ProductionDB_WDSGridArea                      = SdePath( ProductionDB_Reference,               'WDSGridArea' )
ProductionDB_WDSMapArea                       = SdePath( ProductionDB_Reference,               'WDSMapArea' )
ProductionDB_FenceWaterworksProperty          = SdePath( ProductionDB_Structure,               'FenceWaterworksProperty' )
ProductionDB_GateWatershedTrail               = SdePath( ProductionDB_Structure,               'GateWatershedTrail' )
ProductionDB_ResidualsComplexFootprint        = SdePath( ProductionDB_Structure,               'ResidualsComplexFootprint' )
ProductionDB_CulturalArea                     = SdePath( ProductionDB_SurfaceOverlay,          'CulturalArea' )
ProductionDB_BoatRamp                         = SdePath( ProductionDB_Transportation,          'BoatRamp' )
ProductionDB_InfrastructureBoundary           = SdePath( ProductionDB_Transportation,          'InfrastructureBoundary' )
ProductionDB_MajorHighwayPoint                = SdePath( ProductionDB_Transportation,          'MajorHighwayPoint' )
ProductionDB_Railroad                         = SdePath( ProductionDB_Transportation,          'Railroad' )
ProductionDB_WatershedTrail                   = SdePath( ProductionDB_Transportation,          'WatershedTrail' )
ProductionDB_WatershedTrailBridge             = SdePath( ProductionDB_Transportation,          'WatershedTrailBridge' )
ProductionDB_ConfinedSpace                    = SdePath( ProductionDB_WaterUtility,            'ConfinedSpace' )
ProductionDB_NetworkStructure                 = SdePath( ProductionDB_WaterUtility,            'NetworkStructure' )
ProductionDB_Node_PressurizedMainRawWater     = SdePath( ProductionDB_WaterUtility,            'Node_PressurizedMainRawWater' )
ProductionDB_WaterStructure                   = SdePath( ProductionDB_WaterUtility,            'WaterStructure' )
ProductionDB_wSamplingLocation                = SdePath( ProductionDB_WaterUtility,            'wSamplingLocation' )
ProductionDB_DistributionArea                 = SdePath( ProductionDB,                         'DistributionArea' )
ProductionDB_DistributionSourceSystem         = SdePath( ProductionDB,                         'DistributionSourceSystem' )
ProductionDB_PlaceNameAnno3000                = SdePath( ProductionDB_Structure,               'PlaceNameAnno3000' )
ProductionDB_StreamAnno3000                   = SdePath( ProductionDB_Hydrography,             'StreamAnno3000' )

#
# Source ArcSDE geodatabase
#
#   - Current HRSD data will be exported from this database.
#

# Database
SourceDB = r'\\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Datasets\Export\Interfaces\Connections\Conway_sdeSource_sdeViewer.sde'

# Dataset
SourceDB_HRSD = SdePath( SourceDB, 'HRSD', 'sdeSource' )

# Feature class
SourceDB_HRSD_Interceptor = SdePath( SourceDB_HRSD, 'HRSD_Interceptor', 'sdeSource' )

#
# Template personal geodatabase
#
#   - This database contains data that is never updated -- the Elevation dataset
#     and its two contained feature classes, TwoFootContour and TenFootContour.
#
#   - This database will be copied to create the temporary personal geodatabase.
#

# Database
# Future to do:  Manually upgrade this 10.0 personal geodatabase after all users have been upgraded to ArcGIS 11.0.
TemplateDB = r'\\Arctic\Desktop_GIS\GIS\OperationalSystems\Locations\Waterworks BaseMap\Data\Databases\sdeVectorInfrequent_Template.mdb'

#
# Temporary personal geodatabase
#
#   - Data will be exported into this database.
#
#   - This database will replace the stale copy of the Basemap personal
#     geodatabase.
#

# Database
TemporaryDB = r'\\Arctic\Desktop_GIS\GIS\OperationalSystems\Locations\Waterworks BaseMap\Data\Databases\sdeVectorInfrequent_Temp.mdb'

# Datasets
TemporaryDB_Administrative          = MdbPath( TemporaryDB, 'Administrative' )
TemporaryDB_Cadastral               = MdbPath( TemporaryDB, 'Cadastral' )
#TemporaryDB_Elevation               = MdbPath( TemporaryDB, 'Elevation' ) # This dataset is included in the template database.
TemporaryDB_EmergencyManagement     = MdbPath( TemporaryDB, 'EmergencyManagement' )
TemporaryDB_EnvironmentalManagement = MdbPath( TemporaryDB, 'EnvironmentalManagement' )
TemporaryDB_GeodeticControl         = MdbPath( TemporaryDB, 'GeodeticControl' )
TemporaryDB_HRSD                    = MdbPath( TemporaryDB, 'HRSD' )
TemporaryDB_Hydrography             = MdbPath( TemporaryDB, 'Hydrography' )
TemporaryDB_OtherUtility            = MdbPath( TemporaryDB, 'OtherUtility' )
TemporaryDB_Reference               = MdbPath( TemporaryDB, 'Reference' )
TemporaryDB_Structure               = MdbPath( TemporaryDB, 'Structure' )
TemporaryDB_SurfaceOverlay          = MdbPath( TemporaryDB, 'SurfaceOverlay' )
TemporaryDB_Transportation          = MdbPath( TemporaryDB, 'Transportation' )
TemporaryDB_WaterUtility            = MdbPath( TemporaryDB, 'WaterUtility' )

# Feature classes
TemporaryDB_MunicipalArea                    = MdbPath( TemporaryDB_Administrative,          'MunicipalArea' )
TemporaryDB_MunicipalBoundary                = MdbPath( TemporaryDB_Administrative,          'MunicipalBoundary' )
TemporaryDB_PeninsulaMunicipalArea           = MdbPath( TemporaryDB_Administrative,          'PeninsulaMunicipalArea' )
TemporaryDB_FederalReservationArea           = MdbPath( TemporaryDB_Cadastral,               'FederalReservationArea' )
TemporaryDB_NationalPark                     = MdbPath( TemporaryDB_Cadastral,               'NationalPark' )
TemporaryDB_OpenSpaceEasement                = MdbPath( TemporaryDB_Cadastral,               'OpenSpaceEasement' )
TemporaryDB_RealPropertyParcel               = MdbPath( TemporaryDB_Cadastral,               'RealPropertyParcel' )
TemporaryDB_WaterworksProperty               = MdbPath( TemporaryDB_Cadastral,               'WaterworksProperty' )
#TemporaryDB_TenFootContour                   = MdbPath( TemporaryDB_Elevation,               'TenFootContour' ) # This feature class is included in the template database.
#TemporaryDB_TwoFootContour                   = MdbPath( TemporaryDB_Elevation,               'TwoFootContour' ) # This feature class is included in the template database.
TemporaryDB_DamInundationAreaFullPMF         = MdbPath( TemporaryDB_EmergencyManagement,     'DamInundationAreaFullPMF' )
TemporaryDB_DamInundationAreaRoad            = MdbPath( TemporaryDB_EmergencyManagement,     'DamInundationAreaRoad' )
TemporaryDB_DamInundationAreaServiceLocation = MdbPath( TemporaryDB_EmergencyManagement,     'DamInundationAreaServiceLocation' )
TemporaryDB_DamInundationParcel              = MdbPath( TemporaryDB_EmergencyManagement,     'DamInundationParcel' )
TemporaryDB_EmergencyManagementLocation      = MdbPath( TemporaryDB_EmergencyManagement,     'EmergencyManagementLocation' )
TemporaryDB_BeaverDam                        = MdbPath( TemporaryDB_EnvironmentalManagement, 'BeaverDam' )
TemporaryDB_ColonialPipelineSpillArea        = MdbPath( TemporaryDB_EnvironmentalManagement, 'ColonialPipelineSpillArea' )
TemporaryDB_Culvert                          = MdbPath( TemporaryDB_EnvironmentalManagement, 'Culvert' )
TemporaryDB_EcologicallySensitiveArea        = MdbPath( TemporaryDB_EnvironmentalManagement, 'EcologicallySensitiveArea' )
TemporaryDB_HistoricArea                     = MdbPath( TemporaryDB_EnvironmentalManagement, 'HistoricArea' )
TemporaryDB_HistoricLocation                 = MdbPath( TemporaryDB_EnvironmentalManagement, 'HistoricLocation' )
TemporaryDB_RawSampleLocation                = MdbPath( TemporaryDB_EnvironmentalManagement, 'RawSampleLocation' )
TemporaryDB_ResidualsMonitoringSite          = MdbPath( TemporaryDB_EnvironmentalManagement, 'ResidualsMonitoringSite' )
TemporaryDB_ResidualsParcel                  = MdbPath( TemporaryDB_EnvironmentalManagement, 'ResidualsParcel' )
TemporaryDB_RookeryArea                      = MdbPath( TemporaryDB_EnvironmentalManagement, 'RookeryArea' )
TemporaryDB_TreeStand                        = MdbPath( TemporaryDB_EnvironmentalManagement, 'TreeStand' )
TemporaryDB_VirginiaPollutionAbatementWell   = MdbPath( TemporaryDB_EnvironmentalManagement, 'VirginiaPollutionAbatementWell' )
TemporaryDB_Watershed                        = MdbPath( TemporaryDB_EnvironmentalManagement, 'Watershed' )
TemporaryDB_WatershedBoundary                = MdbPath( TemporaryDB_EnvironmentalManagement, 'WatershedBoundary' )
TemporaryDB_WatershedProtectionMapGrid       = MdbPath( TemporaryDB_EnvironmentalManagement, 'WatershedProtectionMapGrid' )
TemporaryDB_WatershedStreamProtectionBuffer  = MdbPath( TemporaryDB_EnvironmentalManagement, 'WatershedStreamProtectionBuffer' )
TemporaryDB_Wetland                          = MdbPath( TemporaryDB_EnvironmentalManagement, 'Wetland' )
TemporaryDB_Wildfire                         = MdbPath( TemporaryDB_EnvironmentalManagement, 'Wildfire' )
TemporaryDB_Monument                         = MdbPath( TemporaryDB_GeodeticControl,         'Monument' )
TemporaryDB_HRSD_Interceptor                 = MdbPath( TemporaryDB_HRSD,                    'HRSD_Interceptor' )
TemporaryDB_Dam                              = MdbPath( TemporaryDB_Hydrography,             'Dam' )
TemporaryDB_HydroArea                        = MdbPath( TemporaryDB_Hydrography,             'HydroArea' )
TemporaryDB_HydroAreaWatershed               = MdbPath( TemporaryDB_Hydrography,             'HydroAreaWatershed' )
TemporaryDB_HydroLine                        = MdbPath( TemporaryDB_Hydrography,             'HydroLine' )
TemporaryDB_HydroPoint                       = MdbPath( TemporaryDB_Hydrography,             'HydroPoint' )
TemporaryDB_ColonialPipeline                 = MdbPath( TemporaryDB_OtherUtility,            'ColonialPipeline' )
TemporaryDB_DominionElectricEasement         = MdbPath( TemporaryDB_OtherUtility,            'DominionElectricEasement' )
TemporaryDB_FiberOpticLine                   = MdbPath( TemporaryDB_OtherUtility,            'FiberOpticLine' )
TemporaryDB_Sewer                            = MdbPath( TemporaryDB_OtherUtility,            'Sewer' )
TemporaryDB_VirginiaNaturalGasMain           = MdbPath( TemporaryDB_OtherUtility,            'VirginiaNaturalGasMain' )
TemporaryDB_ADCVirginiaPeninsulaGridArea     = MdbPath( TemporaryDB_Reference,               'ADCVirginiaPeninsulaGridArea' )
TemporaryDB_WDSGridArea                      = MdbPath( TemporaryDB_Reference,               'WDSGridArea' )
TemporaryDB_WDSMapArea                       = MdbPath( TemporaryDB_Reference,               'WDSMapArea' )
TemporaryDB_FenceWaterworksProperty          = MdbPath( TemporaryDB_Structure,               'FenceWaterworksProperty' )
TemporaryDB_GateWatershedTrail               = MdbPath( TemporaryDB_Structure,               'GateWatershedTrail' )
TemporaryDB_ResidualsComplexFootprint        = MdbPath( TemporaryDB_Structure,               'ResidualsComplexFootprint' )
TemporaryDB_CulturalArea                     = MdbPath( TemporaryDB_SurfaceOverlay,          'CulturalArea' )
TemporaryDB_BoatRamp                         = MdbPath( TemporaryDB_Transportation,          'BoatRamp' )
TemporaryDB_InfrastructureBoundary           = MdbPath( TemporaryDB_Transportation,          'InfrastructureBoundary' )
TemporaryDB_MajorHighwayPoint                = MdbPath( TemporaryDB_Transportation,          'MajorHighwayPoint' )
TemporaryDB_Railroad                         = MdbPath( TemporaryDB_Transportation,          'Railroad' )
TemporaryDB_WatershedTrail                   = MdbPath( TemporaryDB_Transportation,          'WatershedTrail' )
TemporaryDB_WatershedTrailBridge             = MdbPath( TemporaryDB_Transportation,          'WatershedTrailBridge' )
TemporaryDB_ConfinedSpace                    = MdbPath( TemporaryDB_WaterUtility,            'ConfinedSpace' )
TemporaryDB_NetworkStructure                 = MdbPath( TemporaryDB_WaterUtility,            'NetworkStructure' )
TemporaryDB_Node_PressurizedMainRawWater     = MdbPath( TemporaryDB_WaterUtility,            'Node_PressurizedMainRawWater' )
TemporaryDB_WaterStructure                   = MdbPath( TemporaryDB_WaterUtility,            'WaterStructure' )
TemporaryDB_wSamplingLocation                = MdbPath( TemporaryDB_WaterUtility,            'wSamplingLocation' )
TemporaryDB_DistributionArea                 = MdbPath( TemporaryDB,                         'DistributionArea' )
TemporaryDB_DistributionSystemSource         = MdbPath( TemporaryDB,                         'DistributionSystemSource' )
TemporaryDB_PlaceNameAnno3000                = MdbPath( TemporaryDB,                         'PlaceNameAnno3000' )
TemporaryDB_StreamAnno3000                   = MdbPath( TemporaryDB,                         'StreamAnno3000' )

#
# Basemap personal geodatabase
#
#   - If data cannot be exported from the production ArcSDE geodatabase, stale
#     data will be exported from this database instead.
#
#   - This database will be replaced by the temporary personal geodatabase.
#

# Database
BasemapDB = r'\\Arctic\Desktop_GIS\GIS\OperationalSystems\Locations\Waterworks BaseMap\Data\Databases\sdeVectorInfrequent.mdb'

# Datasets
BasemapDB_Administrative          = MdbPath( BasemapDB, 'Administrative' )
BasemapDB_Cadastral               = MdbPath( BasemapDB, 'Cadastral' )
#BasemapDB_Elevation               = MdbPath( BasemapDB, 'Elevation' ) # This dataset is included in the template database.
BasemapDB_EmergencyManagement     = MdbPath( BasemapDB, 'EmergencyManagement' )
BasemapDB_EnvironmentalManagement = MdbPath( BasemapDB, 'EnvironmentalManagement' )
BasemapDB_GeodeticControl         = MdbPath( BasemapDB, 'GeodeticControl' )
BasemapDB_HRSD                    = MdbPath( BasemapDB, 'HRSD' )
BasemapDB_Hydrography             = MdbPath( BasemapDB, 'Hydrography' )
BasemapDB_OtherUtility            = MdbPath( BasemapDB, 'OtherUtility' )
BasemapDB_Reference               = MdbPath( BasemapDB, 'Reference' )
BasemapDB_Structure               = MdbPath( BasemapDB, 'Structure' )
BasemapDB_SurfaceOverlay          = MdbPath( BasemapDB, 'SurfaceOverlay' )
BasemapDB_Transportation          = MdbPath( BasemapDB, 'Transportation' )
BasemapDB_WaterUtility            = MdbPath( BasemapDB, 'WaterUtility' )

# Feature classes
BasemapDB_MunicipalArea                    = MdbPath( BasemapDB_Administrative,          'MunicipalArea' )
BasemapDB_MunicipalBoundary                = MdbPath( BasemapDB_Administrative,          'MunicipalBoundary' )
BasemapDB_PeninsulaMunicipalArea           = MdbPath( BasemapDB_Administrative,          'PeninsulaMunicipalArea' )
BasemapDB_FederalReservationArea           = MdbPath( BasemapDB_Cadastral,               'FederalReservationArea' )
BasemapDB_NationalPark                     = MdbPath( BasemapDB_Cadastral,               'NationalPark' )
BasemapDB_OpenSpaceEasement                = MdbPath( BasemapDB_Cadastral,               'OpenSpaceEasement' )
BasemapDB_RealPropertyParcel               = MdbPath( BasemapDB_Cadastral,               'RealPropertyParcel' )
BasemapDB_WaterworksProperty               = MdbPath( BasemapDB_Cadastral,               'WaterworksProperty' )
#BasemapDB_TenFootContour                   = MdbPath( BasemapDB_Elevation,               'TenFootContour' ) # This feature class is included in the template database.
#BasemapDB_TwoFootContour                   = MdbPath( BasemapDB_Elevation,               'TwoFootContour' ) # This feature class is included in the template database.
BasemapDB_DamInundationAreaFullPMF         = MdbPath( BasemapDB_EmergencyManagement,     'DamInundationAreaFullPMF' )
BasemapDB_DamInundationAreaRoad            = MdbPath( BasemapDB_EmergencyManagement,     'DamInundationAreaRoad' )
BasemapDB_DamInundationAreaServiceLocation = MdbPath( BasemapDB_EmergencyManagement,     'DamInundationAreaServiceLocation' )
BasemapDB_DamInundationParcel              = MdbPath( BasemapDB_EmergencyManagement,     'DamInundationParcel' )
BasemapDB_EmergencyManagementLocation      = MdbPath( BasemapDB_EmergencyManagement,     'EmergencyManagementLocation' )
BasemapDB_BeaverDam                        = MdbPath( BasemapDB_EnvironmentalManagement, 'BeaverDam' )
BasemapDB_ColonialPipelineSpillArea        = MdbPath( BasemapDB_EnvironmentalManagement, 'ColonialPipelineSpillArea' )
BasemapDB_Culvert                          = MdbPath( BasemapDB_EnvironmentalManagement, 'Culvert' )
BasemapDB_EcologicallySensitiveArea        = MdbPath( BasemapDB_EnvironmentalManagement, 'EcologicallySensitiveArea' )
BasemapDB_HistoricArea                     = MdbPath( BasemapDB_EnvironmentalManagement, 'HistoricArea' )
BasemapDB_HistoricLocation                 = MdbPath( BasemapDB_EnvironmentalManagement, 'HistoricLocation' )
BasemapDB_RawSampleLocation                = MdbPath( BasemapDB_EnvironmentalManagement, 'RawSampleLocation' )
BasemapDB_ResidualsMonitoringSite          = MdbPath( BasemapDB_EnvironmentalManagement, 'ResidualsMonitoringSite' )
BasemapDB_ResidualsParcel                  = MdbPath( BasemapDB_EnvironmentalManagement, 'ResidualsParcel' )
BasemapDB_RookeryArea                      = MdbPath( BasemapDB_EnvironmentalManagement, 'RookeryArea' )
BasemapDB_TreeStand                        = MdbPath( BasemapDB_EnvironmentalManagement, 'TreeStand' )
BasemapDB_VirginiaPollutionAbatementWell   = MdbPath( BasemapDB_EnvironmentalManagement, 'VirginiaPollutionAbatementWell' )
BasemapDB_Watershed                        = MdbPath( BasemapDB_EnvironmentalManagement, 'Watershed' )
BasemapDB_WatershedBoundary                = MdbPath( BasemapDB_EnvironmentalManagement, 'WatershedBoundary' )
BasemapDB_WatershedProtectionMapGrid       = MdbPath( BasemapDB_EnvironmentalManagement, 'WatershedProtectionMapGrid' )
BasemapDB_WatershedStreamProtectionBuffer  = MdbPath( BasemapDB_EnvironmentalManagement, 'WatershedStreamProtectionBuffer' )
BasemapDB_Wetland                          = MdbPath( BasemapDB_EnvironmentalManagement, 'Wetland' )
BasemapDB_Wildfire                         = MdbPath( BasemapDB_EnvironmentalManagement, 'Wildfire' )
BasemapDB_Monument                         = MdbPath( BasemapDB_GeodeticControl,         'Monument' )
BasemapDB_HRSD_Interceptor                 = MdbPath( BasemapDB_HRSD,                    'HRSD_Interceptor' )
BasemapDB_Dam                              = MdbPath( BasemapDB_Hydrography,             'Dam' )
BasemapDB_HydroArea                        = MdbPath( BasemapDB_Hydrography,             'HydroArea' )
BasemapDB_HydroAreaWatershed               = MdbPath( BasemapDB_Hydrography,             'HydroAreaWatershed' )
BasemapDB_HydroLine                        = MdbPath( BasemapDB_Hydrography,             'HydroLine' )
BasemapDB_HydroPoint                       = MdbPath( BasemapDB_Hydrography,             'HydroPoint' )
BasemapDB_ColonialPipeline                 = MdbPath( BasemapDB_OtherUtility,            'ColonialPipeline' )
BasemapDB_DominionElectricEasement         = MdbPath( BasemapDB_OtherUtility,            'DominionElectricEasement' )
BasemapDB_FiberOpticLine                   = MdbPath( BasemapDB_OtherUtility,            'FiberOpticLine' )
BasemapDB_Sewer                            = MdbPath( BasemapDB_OtherUtility,            'Sewer' )
BasemapDB_VirginiaNaturalGasMain           = MdbPath( BasemapDB_OtherUtility,            'VirginiaNaturalGasMain' )
BasemapDB_ADCVirginiaPeninsulaGridArea     = MdbPath( BasemapDB_Reference,               'ADCVirginiaPeninsulaGridArea' )
BasemapDB_WDSGridArea                      = MdbPath( BasemapDB_Reference,               'WDSGridArea' )
BasemapDB_WDSMapArea                       = MdbPath( BasemapDB_Reference,               'WDSMapArea' )
BasemapDB_FenceWaterworksProperty          = MdbPath( BasemapDB_Structure,               'FenceWaterworksProperty' )
BasemapDB_GateWatershedTrail               = MdbPath( BasemapDB_Structure,               'GateWatershedTrail' )
BasemapDB_ResidualsComplexFootprint        = MdbPath( BasemapDB_Structure,               'ResidualsComplexFootprint' )
BasemapDB_CulturalArea                     = MdbPath( BasemapDB_SurfaceOverlay,          'CulturalArea' )
BasemapDB_BoatRamp                         = MdbPath( BasemapDB_Transportation,          'BoatRamp' )
BasemapDB_InfrastructureBoundary           = MdbPath( BasemapDB_Transportation,          'InfrastructureBoundary' )
BasemapDB_MajorHighwayPoint                = MdbPath( BasemapDB_Transportation,          'MajorHighwayPoint' )
BasemapDB_Railroad                         = MdbPath( BasemapDB_Transportation,          'Railroad' )
BasemapDB_WatershedTrail                   = MdbPath( BasemapDB_Transportation,          'WatershedTrail' )
BasemapDB_WatershedTrailBridge             = MdbPath( BasemapDB_Transportation,          'WatershedTrailBridge' )
BasemapDB_ConfinedSpace                    = MdbPath( BasemapDB_WaterUtility,            'ConfinedSpace' )
BasemapDB_NetworkStructure                 = MdbPath( BasemapDB_WaterUtility,            'NetworkStructure' )
BasemapDB_Node_PressurizedMainRawWater     = MdbPath( BasemapDB_WaterUtility,            'Node_PressurizedMainRawWater' )
BasemapDB_WaterStructure                   = MdbPath( BasemapDB_WaterUtility,            'WaterStructure' )
BasemapDB_wSamplingLocation                = MdbPath( BasemapDB_WaterUtility,            'wSamplingLocation' )
BasemapDB_DistributionArea                 = MdbPath( BasemapDB,                         'DistributionArea' )
BasemapDB_DistributionSystemSource         = MdbPath( BasemapDB,                         'DistributionSystemSource' )
BasemapDB_PlaceNameAnno3000                = MdbPath( BasemapDB,                         'PlaceNameAnno3000' )
BasemapDB_StreamAnno3000                   = MdbPath( BasemapDB,                         'StreamAnno3000' )

#
# Status mail
#
#   - Execution status email will be sent to these administrative users.
#

MailServer              = 'nnww-smtp.nnww.nnva.gov'
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

LogFile = r'\\Arctic\GIS_Data\GIS_Private\DataResources\DBA\Datasets\Export\Code\Python\ExportWeekend_Errors.txt'

################################################################################

StartDateTime = datetime.now()
ErrorMsgs = []

def ExportWeekend():
    global ErrorMsgs, StartDateTime
    StartDateTime = datetime.now()
    #
    # Initialize ErrorMsgs to an emppty list.
    # Throughout this procedure, catch exceptions and append their error messages onto this list.
    # Before terminating, email this list to interested parties.
    #
    ErrorMsgs = []
    #
    # Create temporary database from template.
    #
    CreateDatabase( TemplateDB, TemporaryDB )
    #
    # Create empty datasets in temporary database.
    # If available, obtain spatial reference information from datasets in production database.
    # Otherwise, obtain it from datasets in existing (stale) basemap database.
    #
    CreateDataset( ProductionDB_Administrative,          BasemapDB_Administrative,          TemporaryDB_Administrative )
    CreateDataset( ProductionDB_Cadastral,               BasemapDB_Cadastral,               TemporaryDB_Cadastral )
#    CreateDataset( ProductionDB_Elevation,               BasemapDB_Elevation,               TemporaryDB_Elevation ) # This dataset is included in the template database.
    CreateDataset( ProductionDB_EmergencyManagement,     BasemapDB_EmergencyManagement,     TemporaryDB_EmergencyManagement )
    CreateDataset( ProductionDB_EnvironmentalManagement, BasemapDB_EnvironmentalManagement, TemporaryDB_EnvironmentalManagement )
    CreateDataset( ProductionDB_GeodeticControl,         BasemapDB_GeodeticControl,         TemporaryDB_GeodeticControl )
    CreateDataset( SourceDB_HRSD,                        BasemapDB_HRSD,                    TemporaryDB_HRSD )
    CreateDataset( ProductionDB_Hydrography,             BasemapDB_Hydrography,             TemporaryDB_Hydrography )
    CreateDataset( ProductionDB_OtherUtility,            BasemapDB_OtherUtility,            TemporaryDB_OtherUtility )
    CreateDataset( ProductionDB_Reference,               BasemapDB_Reference,               TemporaryDB_Reference )
    CreateDataset( ProductionDB_Structure,               BasemapDB_Structure,               TemporaryDB_Structure )
    CreateDataset( ProductionDB_SurfaceOverlay,          BasemapDB_SurfaceOverlay,          TemporaryDB_SurfaceOverlay )
    CreateDataset( ProductionDB_Transportation,          BasemapDB_Transportation,          TemporaryDB_Transportation )
    CreateDataset( ProductionDB_WaterUtility,            BasemapDB_WaterUtility,            TemporaryDB_WaterUtility )
    #
    # For each feature class to be exported:
    #   Export feature class from production database to temporary database.
    #   If unsuccessful, export feature class from existing (stale) basemap database instead.
    #
    ExportFeatureClass( SourceDB_HRSD_Interceptor,                     BasemapDB_HRSD_Interceptor,                 TemporaryDB_HRSD_Interceptor )
    ExportFeatureClass( ProductionDB_MunicipalArea,                    BasemapDB_MunicipalArea,                    TemporaryDB_MunicipalArea )
    ExportFeatureClass( ProductionDB_MunicipalBoundary,                BasemapDB_MunicipalBoundary,                TemporaryDB_MunicipalBoundary )
    ExportFeatureClass( ProductionDB_PeninsulaMunicipalArea,           BasemapDB_PeninsulaMunicipalArea,           TemporaryDB_PeninsulaMunicipalArea )
    ExportFeatureClass( ProductionDB_FederalReservationArea,           BasemapDB_FederalReservationArea,           TemporaryDB_FederalReservationArea )
    ExportFeatureClass( ProductionDB_NationalPark,                     BasemapDB_NationalPark,                     TemporaryDB_NationalPark )
    ExportFeatureClass( ProductionDB_OpenSpaceEasement,                BasemapDB_OpenSpaceEasement,                TemporaryDB_OpenSpaceEasement )
    ExportFeatureClass( ProductionDB_RealPropertyParcel,               BasemapDB_RealPropertyParcel,               TemporaryDB_RealPropertyParcel )
    ExportFeatureClass( ProductionDB_WaterworksProperty,               BasemapDB_WaterworksProperty,               TemporaryDB_WaterworksProperty )
#    ExportFeatureClass( ProductionDB_TenFootContour,                   BasemapDB_TenFootContour,                   TemporaryDB_TenFootContour )   # This feature class is included in the template database.
#    ExportFeatureClass( ProductionDB_TwoFootContour,                   BasemapDB_TwoFootContour,                   TemporaryDB_TwoFootContour )   # This feature class is included in the template database.
    ExportFeatureClass( ProductionDB_DamInundationAreaFullPMF,         BasemapDB_DamInundationAreaFullPMF,         TemporaryDB_DamInundationAreaFullPMF )
    ExportFeatureClass( ProductionDB_DamInundationAreaRoad,            BasemapDB_DamInundationAreaRoad,            TemporaryDB_DamInundationAreaRoad )
    ExportFeatureClass( ProductionDB_DamInundationAreaServiceLocation, BasemapDB_DamInundationAreaServiceLocation, TemporaryDB_DamInundationAreaServiceLocation )
    ExportFeatureClass( ProductionDB_DamInundationParcel,              BasemapDB_DamInundationParcel,              TemporaryDB_DamInundationParcel )
    ExportFeatureClass( ProductionDB_EmergencyManagementLocation,      BasemapDB_EmergencyManagementLocation,      TemporaryDB_EmergencyManagementLocation )
    ExportFeatureClass( ProductionDB_BeaverDam,                        BasemapDB_BeaverDam,                        TemporaryDB_BeaverDam )
    ExportFeatureClass( ProductionDB_ColonialPipelineSpillArea,        BasemapDB_ColonialPipelineSpillArea,        TemporaryDB_ColonialPipelineSpillArea )
    ExportFeatureClass( ProductionDB_Culvert,                          BasemapDB_Culvert,                          TemporaryDB_Culvert )
    ExportFeatureClass( ProductionDB_EcologicallySensitiveArea,        BasemapDB_EcologicallySensitiveArea,        TemporaryDB_EcologicallySensitiveArea )
    ExportFeatureClass( ProductionDB_HistoricArea,                     BasemapDB_HistoricArea,                     TemporaryDB_HistoricArea )
    ExportFeatureClass( ProductionDB_HistoricLocation,                 BasemapDB_HistoricLocation,                 TemporaryDB_HistoricLocation )
    ExportFeatureClass( ProductionDB_RawSampleLocation,                BasemapDB_RawSampleLocation,                TemporaryDB_RawSampleLocation )
    ExportFeatureClass( ProductionDB_ResidualsMonitoringSite,          BasemapDB_ResidualsMonitoringSite,          TemporaryDB_ResidualsMonitoringSite )
    ExportFeatureClass( ProductionDB_ResidualsParcel,                  BasemapDB_ResidualsParcel,                  TemporaryDB_ResidualsParcel )
    ExportFeatureClass( ProductionDB_RookeryArea,                      BasemapDB_RookeryArea,                      TemporaryDB_RookeryArea )
    ExportFeatureClass( ProductionDB_TreeStand,                        BasemapDB_TreeStand,                        TemporaryDB_TreeStand )
    ExportFeatureClass( ProductionDB_VirginiaPollutionAbatementWell,   BasemapDB_VirginiaPollutionAbatementWell,   TemporaryDB_VirginiaPollutionAbatementWell )
    ExportFeatureClass( ProductionDB_Watershed,                        BasemapDB_Watershed,                        TemporaryDB_Watershed )
    ExportFeatureClass( ProductionDB_WatershedBoundary,                BasemapDB_WatershedBoundary,                TemporaryDB_WatershedBoundary )
    ExportFeatureClass( ProductionDB_WatershedProtectionMapGrid,       BasemapDB_WatershedProtectionMapGrid,       TemporaryDB_WatershedProtectionMapGrid )
    ExportFeatureClass( ProductionDB_WatershedStreamProtectionBuffer,  BasemapDB_WatershedStreamProtectionBuffer,  TemporaryDB_WatershedStreamProtectionBuffer )
    ExportFeatureClass( ProductionDB_Wetland,                          BasemapDB_Wetland,                          TemporaryDB_Wetland )
    ExportFeatureClass( ProductionDB_Wildfire,                         BasemapDB_Wildfire,                         TemporaryDB_Wildfire )
    ExportFeatureClass( ProductionDB_Monument,                         BasemapDB_Monument,                         TemporaryDB_Monument )
    ExportFeatureClass( ProductionDB_Dam,                              BasemapDB_Dam,                              TemporaryDB_Dam )
    ExportFeatureClass( ProductionDB_HydroArea,                        BasemapDB_HydroArea,                        TemporaryDB_HydroArea )
    ExportFeatureClass( ProductionDB_HydroAreaWatershed,               BasemapDB_HydroAreaWatershed,               TemporaryDB_HydroAreaWatershed )
    ExportFeatureClass( ProductionDB_HydroLine,                        BasemapDB_HydroLine,                        TemporaryDB_HydroLine )
    ExportFeatureClass( ProductionDB_HydroPoint,                       BasemapDB_HydroPoint,                       TemporaryDB_HydroPoint )
    ExportFeatureClass( ProductionDB_ColonialPipeline,                 BasemapDB_ColonialPipeline,                 TemporaryDB_ColonialPipeline )
    ExportFeatureClass( ProductionDB_DominionElectricEasement,         BasemapDB_DominionElectricEasement,         TemporaryDB_DominionElectricEasement )
    ExportFeatureClass( ProductionDB_FiberOpticLine,                   BasemapDB_FiberOpticLine,                   TemporaryDB_FiberOpticLine )
    ExportFeatureClass( ProductionDB_Sewer,                            BasemapDB_Sewer,                            TemporaryDB_Sewer )
    ExportFeatureClass( ProductionDB_VirginiaNaturalGasMain,           BasemapDB_VirginiaNaturalGasMain,           TemporaryDB_VirginiaNaturalGasMain )
    ExportFeatureClass( ProductionDB_ADCVirginiaPeninsulaGridArea,     BasemapDB_ADCVirginiaPeninsulaGridArea,     TemporaryDB_ADCVirginiaPeninsulaGridArea )
    ExportFeatureClass( ProductionDB_WDSGridArea,                      BasemapDB_WDSGridArea,                      TemporaryDB_WDSGridArea )
    ExportFeatureClass( ProductionDB_WDSMapArea,                       BasemapDB_WDSMapArea,                       TemporaryDB_WDSMapArea )
    ExportFeatureClass( ProductionDB_FenceWaterworksProperty,          BasemapDB_FenceWaterworksProperty,          TemporaryDB_FenceWaterworksProperty )
    ExportFeatureClass( ProductionDB_GateWatershedTrail,               BasemapDB_GateWatershedTrail,               TemporaryDB_GateWatershedTrail )
    ExportFeatureClass( ProductionDB_ResidualsComplexFootprint,        BasemapDB_ResidualsComplexFootprint,        TemporaryDB_ResidualsComplexFootprint )
    ExportFeatureClass( ProductionDB_CulturalArea,                     BasemapDB_CulturalArea,                     TemporaryDB_CulturalArea )
    ExportFeatureClass( ProductionDB_BoatRamp,                         BasemapDB_BoatRamp,                         TemporaryDB_BoatRamp )
    ExportFeatureClass( ProductionDB_InfrastructureBoundary,           BasemapDB_InfrastructureBoundary,           TemporaryDB_InfrastructureBoundary )
    ExportFeatureClass( ProductionDB_MajorHighwayPoint,                BasemapDB_MajorHighwayPoint,                TemporaryDB_MajorHighwayPoint )
    ExportFeatureClass( ProductionDB_Railroad,                         BasemapDB_Railroad,                         TemporaryDB_Railroad )
    ExportFeatureClass( ProductionDB_WatershedTrail,                   BasemapDB_WatershedTrail,                   TemporaryDB_WatershedTrail )
    ExportFeatureClass( ProductionDB_WatershedTrailBridge,             BasemapDB_WatershedTrailBridge,             TemporaryDB_WatershedTrailBridge )
    ExportFeatureClass( ProductionDB_ConfinedSpace,                    BasemapDB_ConfinedSpace,                    TemporaryDB_ConfinedSpace )
    ExportFeatureClass( ProductionDB_NetworkStructure,                 BasemapDB_NetworkStructure,                 TemporaryDB_NetworkStructure )
    ExportFeatureClass( ProductionDB_Node_PressurizedMainRawWater,     BasemapDB_Node_PressurizedMainRawWater,     TemporaryDB_Node_PressurizedMainRawWater )
    ExportFeatureClass( ProductionDB_WaterStructure,                   BasemapDB_WaterStructure,                   TemporaryDB_WaterStructure )
    ExportFeatureClass( ProductionDB_wSamplingLocation,                BasemapDB_wSamplingLocation,                TemporaryDB_wSamplingLocation )
    ExportFeatureClass( ProductionDB_DistributionArea,                 BasemapDB_DistributionArea,                 TemporaryDB_DistributionArea )
    ExportFeatureClass( ProductionDB_DistributionSourceSystem,         BasemapDB_DistributionSystemSource,         TemporaryDB_DistributionSystemSource )   # Export DistributionSourceSystem to DistributionSystemSource.
    ExportFeatureClass( ProductionDB_PlaceNameAnno3000,                BasemapDB_PlaceNameAnno3000,                TemporaryDB_PlaceNameAnno3000 )          # Export Structure\PlaceNameAnno3000 to PlaceNameAnno3000.
    ExportFeatureClass( ProductionDB_StreamAnno3000,                   BasemapDB_StreamAnno3000,                   TemporaryDB_StreamAnno3000 )             # Export Hydrography\StreamAnno3000 to StreamAnno3000.
    #
    # Replace existing (stale) basemap database with temporary database
    # to make it available to basemap users.
    #
    CompactDatabase( TemporaryDB )
    DeleteDatabase( BasemapDB )
    RenameDatabase( TemporaryDB, BasemapDB )
    #
    # Write error messages to log file
    #
    LogErrors( )
    #
    # Send email which states whether task succeeded or failed.
    # If the latter, include error messages.
    #
    SendStatusMail( )

#
# Create a new personal geodatabase by copying a template personal geodatabase.
#
# The template personal geodatabase contains data that is never updated -- the
# Elevation dataset and its two contained feature classes, TwoFootContour and
# TenFootContour.
#
# Do this because:
#
#   - It is faster to copy the template personal geodatabase which already
#     contains data than it is to export the data from the production database.
#
#   - In 10.2, the FeatureClassToFeatureClass geoprocessing tool does not export
#     the TwoFootContour feature class after running for more than an hour.
#     (In 9.3, it terminated with an error for this feature class.)
#
def CreateDatabase(templateDB, newDB):
    print 'Creating database', newDB, '...'
    try:
        if arcpy.Exists(newDB):
            arcpy.Delete_management(newDB)
        arcpy.Copy_management(templateDB, newDB)
    except BaseException, e:
        ErrorMsgs.append('CreateDatabase(templateDB=%s, newDB=%s)' % (templateDB, newDB))
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

ExportWeekend()
