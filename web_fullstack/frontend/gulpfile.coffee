gulp   = require('gulp')
pug    = require('gulp-pug')
sass   = require('gulp-sass')
ts     = require('gulp-typescript')
debug  = require('gulp-debug')
gulpif = require('gulp-if')
argv   = require('yargs').argv
concat = require('gulp-concat')

gulp.task 'pug', ->
    gulp.src('./src/**/*.pug')
        .pipe(pug(
            pretty: true
        ))
        .pipe(gulp.dest('./build'))

gulp.task 'sass', ->
    gulp.src('./src/styles/**/*.scss')
        .pipe(gulpif(argv.debug, debug({ title: 'sass' })))
        .pipe(sass())
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
    gulp.watch('./src/**/*.pug', ['pug'])
    gulp.watch('./src/**/*.scss', ['sass'])
    gulp.watch('./src/**/*.ts', ['typescript'])

gulp.task 'production', ['vendor', 'pug', 'sass', 'typescript']
gulp.task 'default', ['production', 'watch']
