express = require('express')
app     = express()
http    = require('http').Server(app)
io      = require('socket.io')(http)
redis   = require('redis')
drpc    = require('storm-drpc-node')

subscriber = redis.createClient(7000, '172.22.152.37')
subscriber.subscribe('geoTweet')
subscriber.subscribe('globalRanking')

drpcClient = drpc(
    host: '172.22.152.35'
    port: 3772
    timeout: 1000
    keepAlive: true
    maxConnectionCounts: 30
)

app.set('views', __dirname + '/frontend/build')
app.use(express.static(__dirname + '/frontend/build'))
app.set('view engine', 'ejs')
app.engine('html', require('ejs').renderFile)

app.get('/api/*', (req, res) ->
    if req.params['0'] is 'range_rank/'
        tmp = req.query.west + '&&' + req.query.east + '&&' + req.query.south + '&&' + req.query.north
        drpcClient.execute('DrpcServer', JSON.stringify(tmp), (err, res2) ->
            res.send(res2.split('&&'))
        )
)

app.get('/', (req, res) ->
    res.render('index.html')
)

subscriber.on('message', (channel, message) ->
    #console.log('==================')
    if channel is 'geoTweet'
        tmp = JSON.parse(message)
        message =
            content: tmp.tweetText
            position:
                lat: parseFloat(tmp.latitude)
                lng: parseFloat(tmp.longitude)
        #console.log(message)
        io.emit('world.tweet', message)
    else if channel is 'globalRanking'
        message = message.split('&&')
        message.pop()
        #console.log(message)
        io.emit('world.ranking', message)
)

http.listen(8080)
