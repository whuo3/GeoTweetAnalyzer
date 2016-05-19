gulp   = require('gulp')
jade   = require('gulp-jade')
scss   = require('gulp-scss')
ts     = require('gulp-typescript')
debug  = require('gulp-debug')
gulpif = require('gulp-if')
argv   = require('yargs').argv
concat = require('gulp-concat')

gulp.task 'jade', ->
    gulp.src('./src/**/*.jade')
        .pipe(jade(
            pretty: true
        ))
        .pipe(gulp.dest('./build'))

gulp.task 'scss', ->
    gulp.src('./src/styles/**/*.scss')
        .pipe(gulpif(argv.debug, debug({ title: 'scss' })))
        .pipe(scss())
        .pipe(gulp.dest('./build/styles'))

gulp.task 'typescript', ->
    tsProject = ts.createProject('./tsconfig.json')
    tsResult = tsProject.src()
                .pipe(ts(tsProject))
    tsResult.js.pipe(gulp.dest('./build/scripts'))

gulp.task 'vendor', ->
    gulp.src([
            './node_modules/jquery/dist/jquery.js',
            './node_modules/socket.io-client/socket.io.js',
            './node_modules/underscore/underscore.js',
            './node_modules/bootstrap/dist/js/bootstrap.js',
            './node_modules/angular2/bundles/angular2-polyfills.js',
            './node_modules/systemjs/dist/system.src.js',
            './node_modules/rxjs/bundles/Rx.js',
            './node_modules/angular2/bundles/angular2.dev.js'
        ])
        .pipe(concat('vendor.js'))
        .pipe(gulp.dest('./build/scripts'))
    gulp.src([
            './node_modules/bootstrap/dist/css/bootstrap.css'
        ])
        .pipe(concat('vendor.css'))
        .pipe(gulp.dest('./build/styles'))

gulp.task 'watch', ->
    gulp.watch('./src/**/*.jade', ['jade'])
    gulp.watch('./src/**/*.scss', ['scss'])
    gulp.watch('./src/**/*.ts', ['typescript'])

gulp.task 'default', ['vendor', 'jade', 'scss', 'typescript', 'watch']
