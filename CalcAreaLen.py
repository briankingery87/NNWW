from win32com.client import Dispatch
import sys
GP = Dispatch("esriGeoprocessing.GpDispatch.1")

# Get the input shapefile name
in_shapefile = sys.argv[1] #GP.GetParameterAsText(0)

#Add the area or length field
desc = GP.Describe(in_shapefile)
shapeField = desc.ShapeFieldName
if desc.ShapeType == "Polygon":
    type = "poly"
    try:
        GP.AddField(in_shapefile, "Area","double")
        GP.AddMessage("Added Area field...")
    except:
        GP.AddMessage("Updating existing Area field.")
elif desc.ShapeType == "Polyline":
    type = "line"
    try:
        GP.AddField(in_shapefile, "Length","double")
        GP.AddMessage("Added Length field...")
    except:
        GP.AddMessage("Updating existing Length field.")
else:
    GP.AddMessage("Input must either be a polygon or line shapefile.")
    raise("Error")

# Create Update cursor
try:
    rows = GP.UpdateCursor(in_shapefile)
    row = rows.Next()
    GP.AddMessage("Calculating values...")
    while row:
        # Create the geometry object
        feat = row.GetValue(shapeField)
        # Calculate the appropriate statistic
        if type == "poly":
            row.SetValue("Area", feat.area)
        else:
            row.SetValue("Length",feat.length)
        rows.UpdateRow(row)
        row = rows.Next()
        
    del rows
except:
    GP.AddError("Failure during field calculation.")
    if not rows:
        GP.AddError("Unable to open Update cursor.")
    # Remove cursor
    del row
    del rows