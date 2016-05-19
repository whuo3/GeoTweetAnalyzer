express = require('express')
app     = express()
http    = require('http').Server(app)
io      = require('socket.io')(http)
redis   = require('redis')

subscriber = redis.createClient(7000, '172.22.152.37')
subscriber.subscribe('channel')

app.set('views', __dirname + '/frontend/build')
app.use(express.static(__dirname + '/frontend/build'))
app.set('view engine', 'ejs')
app.engine('html', require('ejs').renderFile)

app.get('/api/*', (req, res) ->
    res.send('api')
)

app.get('/', (req, res) ->
    res.render('index.html')
)

subscriber.on('message', (channel, message) ->
    console.log('FROM REDIS: \t' + message)
    io.emit('test', message)
)

http.listen(8080)
