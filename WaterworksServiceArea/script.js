var map, navToolbar;

//List of required items
require(["esri/map",
	
	"esri/dijit/LocateButton",
	"esri/dijit/HomeButton",
	"esri/dijit/BasemapToggle",
	"esri/dijit/Scalebar",
	"esri/layers/FeatureLayer",
	"esri/InfoTemplate",
	
	//NAVIGATION BAR
	"esri/geometry/Extent",
	"esri/toolbars/navigation",
	"dijit/form/Button",
	"dojo/on",
	"dijit/registry",
	"dojo/parser",
	
	"esri/dijit/Search",
	
	"dojo/domReady!"

//List of Functions
], function(Map,
	
	LocateButton,
	HomeButton,
	BasemapToggle,
	Scalebar,
	FeatureLayer,
	InfoTemplate,
	
	//Required for Navigation Toolbar
	Extent, Navigation, Button, on, registry, parser,
	
	Search
	
)

{
	parser.parse();

	var startExtent = new Extent({
		//Extent finder 	http://help.arcgis.com/en/webapps/flexviewer/extenthelper/flexviewer_extenthelper.html
		"xmin": -8570000,
		"ymin": 4432000,
		"xmax": -8478000,
		"ymax": 4490000,
		"spatialReference": {"wkid": 102100}
	});
	
	map = new Map("map", {
	basemap: "hybrid",  //For full list of pre-defined basemaps, navigate to http://arcg.is/1JVo6Wd
	extent: startExtent,
	//center: [-76.65, 37.35], // longitude, latitude
	//zoom: 10
});

	var geoLocate = new LocateButton({
		map: map
	}, "LocateButton");
	geoLocate.startup();
	
	var homeButton = new HomeButton({
		theme: "HomeButton",
		map: map,
		extent: null,
		visible: true
	}, "HomeButton");
	homeButton.startup();

	var toggle = new BasemapToggle({
        map: map,
        basemap: "streets"
    }, "BasemapToggle");
    toggle.startup();
	
	var scalebar = new Scalebar({
		map: map,
	  // "dual" displays both miles and kilmometers
	  // "english" is the default, which displays miles
	  // use "metric" for kilometers
		scalebarUnit: "dual"
	});
	
	var s = new Search({
		enableButtonMode: true, //this enables the search widget to display as a single button
		enableLabel: false,
		enableInfoWindow: true,
		showInfoWindowOnSelect: false,
		map: map
	}, "search");
    s.startup();
	
	navToolbar = new Navigation(map);
        on(navToolbar, "extent-history-change", function () {
			registry.byId("zoomprev").disabled = navToolbar.isFirstExtent();
			registry.byId("zoomnext").disabled = navToolbar.isLastExtent();
		});
	
	
	var infoTemplate = new InfoTemplate("Attributes", "${*}");
	
	var hydrantsFeatureLayer = new FeatureLayer("http://localhost:6080/arcgis/rest/services/Waterworks_MapIt_BaseMap/MapServer/12",
	{
		mode: FeatureLayer.MODE_ONDEMAND, //https://developers.arcgis.com/javascript/jsapi/featurelayer.html - Constants
		infoTemplate: infoTemplate,
		outFields: ['*']
	});
	
	var parcelsFeatureLayer = new FeatureLayer("http://localhost:6080/arcgis/rest/services/Waterworks_MapIt_BaseMap/MapServer/32",
	{
		mode: FeatureLayer.MODE_ONDEMAND,
		infoTemplate: infoTemplate,
		outFields: ['*']
	});
	
	var pressurizedmainsFeatureLayer = new FeatureLayer("http://localhost:6080/arcgis/rest/services/Waterworks_MapIt_BaseMap/MapServer/15",
	{
		mode: FeatureLayer.MODE_ONDEMAND,
		infoTemplate: infoTemplate,
		outFields: ['*']
	});
	
	var waterqualityFeatureLayer = new FeatureLayer("http://localhost:6080/arcgis/rest/services/Waterworks_MapIt_BaseMap/MapServer/1",
	{
		mode: FeatureLayer.MODE_ONDEMAND,
		infoTemplate: infoTemplate,
		outFields: ['*']
	});
	
	map.on("load", function (){
		map.addLayer(hydrantsFeatureLayer);
		map.addLayer(parcelsFeatureLayer);
		map.addLayer(pressurizedmainsFeatureLayer);
		map.addLayer(waterqualityFeatureLayer);
	});
	

	
});
