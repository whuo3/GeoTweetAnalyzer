gulp   = require('gulp')
gls    = require('gulp-live-server')
coffee = require('gulp-coffee')

gulp.task 'serve', ->
    server = gls.new('server.coffee')
    server.start('node_modules/coffee-script/bin/coffee')
    gulp.watch('server.coffee', ->
        server.start.bind(server)()
        console.log 'server reloaded'
    )

gulp.task 'production', ->
    gulp.src('./server.coffee')
        .pipe(coffee({bare: true}))
        .pipe(gulp.dest('.'))

gulp.task 'default', ['serve']
