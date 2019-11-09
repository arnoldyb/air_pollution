// Google Map
var map;

// markers for map
var markers = [];

// info window
var info = new google.maps.InfoWindow();


// execute when the DOM is fully loaded
$(function() {
    console.log("function123");

    document.getElementById('form').addEventListener('submit', function(e) {
      console.log("updateSensors1");
      numSensors = document.getElementById("q").value;
      console.log("updateSensors", numSensors);
    e.preventDefault();
    }, false);
    // styles for map
    // https://developers.google.com/maps/documentation/javascript/styling
    console.log("styles");
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
        maxZoom: 14,
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
 * Configures application.
 */
function configure()
{
    console.log("configure");
    // update UI after map has been dragged
    google.maps.event.addListener(map, "dragend", function() {

        // if info window isn't open
        // http://stackoverflow.com/a/12410385
        if (!info.getMap || !info.getMap())
        {
            console.log("dragend");
            update();
        }
    });

    // update UI after zoom level changes
    google.maps.event.addListener(map, "zoom_changed", function() {
        console.log("zoom_changed");
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
    console.log("noevents")
    update();

    // give focus to text box
    $("#q").focus();
}

/**
 * Shows info window at marker with content.
 */
function showInfo(marker, content)
{
    console.log("showInfo");
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
    console.log("update");
    // get map's bounds
    var bounds = map.getBounds();
    console.log("bounds");
    var ne = bounds.getNorthEast();
    var sw = bounds.getSouthWest();

    console.log("update1");
    // get places within bounds (asynchronously)
    var parameters = {
        ne: ne.lat() + "," + ne.lng(),
        q: $("#q").val(),
        sw: sw.lat() + "," + sw.lng()
    };
    console.log("update2");
    console.log("parms: ", parameters);
    $.getJSON(Flask.url_for("update"), parameters)
};
