var map;

function initMap() {
  map = new google.maps.Map(document.getElementById('map'), {
    zoom: 2,
    center: { lat: 30, lng: 5 },
    scrollwheel: false,
    draggable: false,
    mapTypeControl: false,
    zoomControl: false,
    disableDoubleClickZoom: true,
    streetViewControl: false
  });
}

System.config({
  packages: {
    scripts: {
      format: 'register',
      defaultExtension: 'js'
    }
  }
});
System.import('scripts/main')
.then(null, console.error.bind(console));

var socket = io();
socket.on('world.tweet', function(tweet:any) {
  var marker = new google.maps.Marker({
    position: tweet.position,
    map: map
  });
  var infoWindow = new google.maps.InfoWindow({
    content: tweet.content,
    maxWidth: 200
  });
  infoWindow.open(map, marker);
  setTimeout(function() {
    marker.setMap(null);
  }, 10000);
});
