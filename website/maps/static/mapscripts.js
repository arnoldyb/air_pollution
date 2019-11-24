// Google Map
var map;

// markers for map
var markers = [];

// info window
var info = new google.maps.InfoWindow();


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
        center: {lat: 37.773972, lng: -122.431297}, // Washington, DC
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
    console.log(place[0], place[1]);
    if (type == "recommendation") {
        icon_path = "http://maps.google.com/mapfiles/kml/paddle/grn-blank.png"
    }
    if (type == "existing") {
        icon_path = "http://maps.google.com/mapfiles/kml/paddle/blu-blank.png"
    }
    //create markers
    var marker = new google.maps.Marker({
        position: myloc,
        map: map,
        // label: "V",
//            icon: "http://maps.google.com/mapfiles/kml/pal2/icon13.png"
        icon: icon_path
    });

    //remember marker for later
    markers.push(marker);
    marker.setMap(map);
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

    // give focus to text box
    $("#q").focus();
}

/**
 * Removes markers from map.
 */
function removeMarkers()
{
    // delete each marker
    for (var i = 0; i < markers.length; i++) {
        markers[i].setMap(null);
    }
    // reset markers
    markers = [];
}

/**
 * Shows info window at marker with content.
 */
function showInfo(marker, content)
{
    // start div
    var div = "<div id='info'>";
    if (typeof(content) == "undefined")
    {
        // http://www.ajaxload.info/
        div += "<img alt='loading' src='/static/ajax-loader.gif'/>";
    }
    else
    {
        div += content;
    }

    // end div
    div += "</div>";

    // set info window's content
    info.setContent(div);

    // open info window (if not already open)
    info.open(map, marker);
}


/**
 * Updates UI's markers.
 */
function update()
{
    // get map's bounds
    var bounds = map.getBounds();
    var ne = bounds.getNorthEast();
    var sw = bounds.getSouthWest();

    // get places within bounds (asynchronously)
    var parameters = {
        ne: ne.lat() + "," + ne.lng(),
        q: $("#q").val(),
        sw: sw.lat() + "," + sw.lng(),
        toggle: $("#toggle").is(":checked"),
    };
    $.getJSON(Flask.url_for("update"), parameters)
    .done(function(data, textStatus, jqXHR) {

       // remove old markers from map
       removeMarkers();

       // add new recommendations to map
       for (var i = 0; i < data.recommendations.length; i++)
       {
           addMarker(data.recommendations[i], "recommendation");
       }

        // add existing sensor network to map (if toggled on)
       for (var i = 0; i < data.existing.length; i++)
       {
           addMarker(data.existing[i], "existing");
       }
    })
    .fail(function(jqXHR, textStatus, errorThrown) {

        // log error to browser's console
        console.log(errorThrown.toString());
    });
};
