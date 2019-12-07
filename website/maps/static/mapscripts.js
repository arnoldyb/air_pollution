// Google Map
var map, heatmap;

// markers for map
var recMarkers = [];    // recommendation markers
var sensMarkers = [];   // existing sensor markers
var pollMarkers = [];   // polluter markers

// info window
var info = new google.maps.InfoWindow();

// marker counter
var mCount = 0;

// marker Order
var vzIndex = 1;

//toggle variables
var toggle_existing = "false";
var toggle_polluters = "false";

// execute when the DOM is fully loaded
$(function() {

    document.getElementById('form').addEventListener('submit', function(e) {
      update();
      e.preventDefault();
    }, false);
    // styles for map
    // https://developers.google.com/maps/documentation/javascript/styling
    var styles = [

        // hide Google's labels
        {
            featureType: "landscape",
            elementType: "labels",
            stylers: [
                {visibility: "off"}
            ]
        }

    ];

    // options for map
    // https://developers.google.com/maps/documentation/javascript/reference#MapOptions
    var options = {
        center: {lat: 37.773972, lng: -122.431297},
        disableDefaultUI: true,
        mapTypeId: google.maps.MapTypeId.ROADMAP,
        maxZoom: 16,
        panControl: true,
        styles: styles,
        zoom: 13,
        zoomControl: true
    };

    // get DOM node in which map will be instantiated
    var canvas = $("#map-canvas").get(0);

    // instantiate map
    map = new google.maps.Map(canvas, options);

    // instantiate heatmap
    heatmap = new google.maps.visualization.HeatmapLayer({
        });

    // // Add bounding box
    // var rectangle = new google.maps.Rectangle({
    //       strokeColor: '#ff4000',
    //       strokeOpacity: 0.8,
    //       strokeWeight: 2,
    //       // fillColor: '#A9A9A9',
    //       fillOpacity: 0.005,
    //       map: map,
    //       bounds: {
    //         north: 38.008050,
    //         south: 37.701933,
    //         east: -122.186437,
    //         west: -122.536985
    //           // north: 38.063446,
    //           // south: 37.2781261,
    //           // east: -121.814281,
    //           // west: -122.683496
    //       }
    //     });

    var outerCoords = [
      new google.maps.LatLng(85, 180),
      new google.maps.LatLng(85, 90),
      new google.maps.LatLng(85, 0),
      new google.maps.LatLng(85, -90),
      new google.maps.LatLng(85, -180),
      new google.maps.LatLng(0, -180),
      new google.maps.LatLng(-85, -180),
      new google.maps.LatLng(-85, -90),
      new google.maps.LatLng(-85, 0),
      new google.maps.LatLng(-85, 90),
      new google.maps.LatLng(-85, 180),
      new google.maps.LatLng(0, 180),
      new google.maps.LatLng(85, 180)
    ];

    var innerCoords = [
      {lat: 38.008050, lng: -122.536985},
      {lat: 38.008050, lng: -122.186437},
      {lat: 37.701933, lng: -122.186437},
      {lat: 37.701933, lng: -122.536985}
    ];

    // Construct the polygon, including both paths.
    var boundingBox = new google.maps.Polygon({
      paths: [outerCoords, innerCoords],
      strokeColor: '#808080',
      strokeOpacity: 0.8,
      strokeWeight: 2,
      fillColor: '#b380ff',
      fillOpacity: 0.35
    });
    boundingBox.setMap(map);

    // configure UI once Google Map is idle (i.e., loaded)
    google.maps.event.addListenerOnce(map, "idle", configure);

});

/**
 * Adds marker for place to map.
 */
function addMarker(place, type)
{
    // where are we
    var myloc = new google.maps.LatLng(place[0], place[1]);
    // console.log("Count2", mCount);
    // console.log(place[0], place[1]);
    if (type == "recommendation") {
        icon_path = "http://maps.google.com/mapfiles/kml/paddle/grn-blank.png",
        contentString = '<div id="content">'+
            '<div id="siteNotice">'+
            '</div>'+
            '<div id="bodyContent">'+
            '<p><b>Latitude: </b>' + place[0].toFixed(3) + '</p>'+
            '<p><b>Longitude: </b>' + place[1].toFixed(3) + '</p>'+
            '</div>'+
            '</div>',
        titleString = "Sensor " + mCount.toString(),
        labelString = mCount.toString(),
        vzIndex = 10
    }
    if (type == "existing") {
        icon_path = "http://maps.google.com/mapfiles/kml/paddle/blu-blank.png",
        contentString = '<div id="content">'+
            '<div id="siteNotice">'+
            '</div>'+
            '<div id="bodyContent">'+
            '<p><b>Sensor: </b>' + place[2] + '</p>'+
            '<p><b>Latitude: </b>' + place[0] + '</p>'+
            '<p><b>Longitude: </b>' + place[1] + '</p>'+
            '</div>'+
            '</div>',
        titleString = place[2],
        labelString = " ",
        vzIndex = 1
    }
    if (type == "polluter") {
        icon_path = "http://maps.google.com/mapfiles/kml/shapes/caution.png",
        contentString = '<div id="content">'+
            '<div id="siteNotice">'+
            '</div>'+
            '<div id="bodyContent">'+
            '<p><b>Name: </b>' + place[2] + '</p>'+
            '<p><b>Street: </b>' + place[3] + '</p>'+
            '<p><b>City: </b>' + place[4] + '</p>'+
            '<p><b>PM: </b>' + place[5] + '</p>'+
            '<p><b>Latitude: </b>' + place[0] + '</p>'+
            '<p><b>Longitude: </b>' + place[1] + '</p>'+
            '</div>'+
            '</div>',
        titleString = place[2],
        labelString = " ",
        vzIndex = 1
    }

    // Info for clicks
    var infowindow = new google.maps.InfoWindow({
          content: contentString
        });

    //create markers
    var icon = {
        url: icon_path,
        scaledSize: new google.maps.Size(37, 37),
        labelOrigin: new google.maps.Point(19, 10)
    };

    var marker = new google.maps.Marker({
        position: myloc,
        map: map,
        // label: mCount.toString(),
//            icon: "http://maps.google.com/mapfiles/kml/pal2/icon13.png"
        icon: icon,
        label: {
                  text: labelString,
                  // color: "#eb3a44",
                  // fontSize: "16px",
                  fontWeight: "bold"
                },
        title: titleString,
        zIndex: vzIndex
    });
    marker.addListener('click', function() {
      infowindow.open(map, marker);
    });

    //remember marker for later
    // markers.push(marker);
    // marker.setMap(map);
    if (type == "recommendation") {
        recMarkers.push(marker);
        marker.setMap(map);
    }
    if (type == "existing") {
        sensMarkers.push(marker);
        marker.setMap(null);
    }
    if (type == "polluter") {
        pollMarkers.push(marker);
        marker.setMap(null);
    }
}

/**
 * Configures application.
 */
function configure()
{
    // update UI after map has been dragged
    google.maps.event.addListener(map, "dragend", function() {

        // if info window isn't open
        // http://stackoverflow.com/a/12410385
        if (!info.getMap || !info.getMap())
        {
            update();
        }
    });

    // update UI after zoom level changes
    google.maps.event.addListener(map, "zoom_changed", function() {
        update();
    });

    // hide info window when text box has focus
    $("#q").focus(function(eventData) {
        info.close();
    });

    // re-enable ctrl- and right-clicking (and thus Inspect Element) on Google Map
    // https://chrome.google.com/webstore/detail/allow-right-click/hompjdfbfmmmgflfjdlnkohcplmboaeo?hl=en
    document.addEventListener("contextmenu", function(event) {
        event.returnValue = true;
        event.stopPropagation && event.stopPropagation();
        // event.cancelBubble && event.cancelBubble();
    }, true);

    // update UI
    update();

    // creates arrays for existing sensors and polluters
    createStaticMarkerArrays();

    // give focus to text box
    $("#q").focus();
}

/**
 * Removes markers from map.
 */
function removeMarkers()
{
   // reset counter
   mCount = 0;
   // delete each marker
    for (var i = 0; i < recMarkers.length; i++) {
        recMarkers[i].setMap(null);
    }
    for (var i = 0; i < sensMarkers.length; i++) {
        sensMarkers[i].setMap(null);
    }
    for (var i = 0; i < pollMarkers.length; i++) {
        pollMarkers[i].setMap(null);
    }
    // reset markers
    recMarkers = [];

    // reset heatmap
    heatmap.setMap(null);

}

/**
 * Shows info window at marker with content.
 */
// function showInfo(marker, content)
// {
//     // start div
//     var div = "<div id='info'>";
//     if (typeof(content) == "undefined")
//     {
//         // http://www.ajaxload.info/
//         div += "<img alt='loading' src='/static/ajax-loader.gif'/>";
//     }
//     else
//     {
//         div += content;
//     }
//
//     // end div
//     div += "</div>";
//
//     // set info window's content
//     info.setContent(div);
//
//     // open info window (if not already open)
//     info.open(map, marker);
// }


/**
 * Updates UI's markers.
 */
function update()
{
    // reset counter
    mCount = 0;
    console.log("Count", mCount);
    // get map's bounds
    var bounds = map.getBounds();
    var ne = bounds.getNorthEast();
    var sw = bounds.getSouthWest();

    var toggle_heatmap = $("#toggle_heatmap").is(":checked");
    console.log("Heatmap", toggle_heatmap);

    // get places within bounds (asynchronously)
    var parameters = {
        ne: ne.lat() + "," + ne.lng(),
        q: $("#q").val(),
        sw: sw.lat() + "," + sw.lng(),
        toggle_heatmap: $("#toggle_heatmap").is(":checked")
    };
    $.getJSON(Flask.url_for("update"), parameters)
    .done(function(data, textStatus, jqXHR) {

       // remove old markers from map
       removeMarkers();

       // add new recommendations to map
       for (var i = 0; i < data.recommendations.length; i++)
       {
           mCount += 1;
           addMarker(data.recommendations[i], "recommendation");
       }

        // add existing sensor network to map (if toggled on)
        toggle_existing = $("#toggle_existing").is(":checked")
        console.log("Existing Toggle", toggle_existing)
        if (toggle_existing) {
          console.log("in existing");
          for (var i = 0; i < sensMarkers.length; i++) {
              sensMarkers[i].setMap(map);
          }
        }

       // add polluters to map (if toggled on)
       toggle_polluters = $("#toggle_polluters").is(":checked")
       console.log("Polluter Toggle", toggle_polluters)
       if (toggle_polluters) {
         console.log("in polluters");
         for (var i = 0; i < pollMarkers.length; i++) {
             pollMarkers[i].setMap(map);
         }
       }

       // create heatmap

       if (toggle_heatmap)
       {
         var zoom_factor = map.getZoom();
         var rad = 0.00415039062 * (2 ** zoom_factor); // basically, you want default zoom of 13 to have a radius of about 34

         heatmap = new google.maps.visualization.HeatmapLayer({
               data: getPoints(data.heatmappy),
               radius: rad,
               map: map
             });
       }
    })
    .fail(function(jqXHR, textStatus, errorThrown) {

        // log error to browser's console
        console.log(errorThrown.toString());
    });
};

/**
 * Creates marker arrays for existing sensors and polluters
 */
function createStaticMarkerArrays()
{
    $.getJSON(Flask.url_for("getstaticmarkers"))
    .done(function(data, textStatus, jqXHR) {

       // add existing sensor network to map (if toggled on)
       for (var i = 0; i < data.existing.length; i++)
       {
           addMarker(data.existing[i], "existing");
       }

       // add polluters to map (if toggled on)
       for (var i = 0; i < data.polluters.length; i++)
       {
           addMarker(data.polluters[i], "polluter");
       }
    })
    .fail(function(jqXHR, textStatus, errorThrown) {

        // log error to browser's console
        console.log(errorThrown.toString());
    });
};

// data points for heatmap
function getPoints(heat_list) {

        var heat_points = [];
        for (var i = 0; i < heat_list.length; i++)
        {
            //heat_points.push(new google.maps.LatLng(heat_list[i][0], heat_list[i][1]));
            heat_points.push({location: new google.maps.LatLng(heat_list[i][0], heat_list[i][1]), weight: heat_list[i][2]});
        }

        return heat_points;
      }
