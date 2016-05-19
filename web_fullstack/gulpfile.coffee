gulp = require('gulp')
gls  = require('gulp-live-server')

gulp.task 'serve', ->
    server = gls.new('server.coffee')
    server.start('node_modules/coffee-script/bin/coffee')
    gulp.watch('server.coffee', ->
        server.start.bind(server)()
        console.log 'server reloaded'
    )

gulp.task 'default', ['serve']
