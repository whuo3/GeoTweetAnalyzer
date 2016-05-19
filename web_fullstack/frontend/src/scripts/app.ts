var map;

function initMap() {
  map = new google.maps.Map(document.getElementById('map'), {
    zoom: 2,
    center: { lat: 30, lng: 5 },
    mapTypeControl: false,
    streetViewControl: false
  });
  map.addListener('idle', function() {
    template = _.template('<li><a href="https://twitter.com/hashtag/<%= hashtag %>" target="_blank"><%= hashtag %></a></li>');
    $.getJSON('/api/range_rank/', map.getBounds().toJSON(), function(json:any) {
      $('#local-rank').html(_.reduce(json, function(memo:any, elem:string) {
        return memo + template({ hashtag: elem });
      }, ''));
    });
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
  var bounds = map.getBounds().toJSON();
  var lat = tweet.position.lat;
  var lng = tweet.position.lng;
  if (lat < bounds.south || lat > bounds.north || lng < bounds.west || lng > bounds.east) return;
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
