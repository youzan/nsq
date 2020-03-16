var browserify = require('browserify');
var clean = require('gulp-clean');
var gulp = require('gulp');
var notify = require('gulp-notify');
var path = require('path');
var sass = require('gulp-sass');
var source = require('vinyl-source-stream');
var taskListing = require('gulp-task-listing');
var uglify = require('gulp-uglify');
var sourcemaps = require('gulp-sourcemaps');
var buffer = require('vinyl-buffer');
const { series } = require('gulp');

var ROOT = 'static';

var VENDOR_CONFIG = {
    'src': [
        'backbone',
        'jquery',
        'underscore',
        'bootbox',
    ],
    'target': 'vendor.js',
    'targetDir': './static/build/'
};

function excludeVendor(b) {
    VENDOR_CONFIG.src.forEach(function(vendorLib) {
        b.exclude(vendorLib);
    });
}

function bytesToKB(bytes) { return Math.floor(+bytes/1024); }

function logBundle(filename, watching) {
    return function (err, buf) {
        if (err) {
            console.error(err.toString());
            if (!watching) {
                process.exit(1);
            }
        }
        if (!watching) {
            console.log(filename + ' ' + bytesToKB(buf.length) + ' KB written');
        }
    }
}


function sassTask(cb) {
    root = ROOT; 
    inputFile = '*.*css';
    var onError = function(err) {
        notify({'title': 'Sass Compile Error'}).write(err);
    };
    gulp.src(path.join(root, 'css', inputFile), { allowEmpty: true })
        .pipe(sass({
            'sourceComments': 'map',
            'onError': onError
        }))
        .pipe(gulp.dest(path.join(root, 'build/')));
    cb();
}

function browserifyTask(cb) {
    root = ROOT;
    inputFile = 'main.js';

    var onError = function() {
        var args = Array.prototype.slice.call(arguments);
        notify.onError({
            'title': 'JS Compile Error',
            'message': '<%= error.message %>'
        }).apply(this, args);
        // Keep gulp from hanging on this task
        this.emit('end');
    };

    // Browserify needs a node module to import as its arg, so we need to
    // force the leading "./" to be included.
    var b = browserify({
        entries: './' + path.join(root, 'js', inputFile),
        debug: true
    })

    excludeVendor(b);

    b.bundle()
        .pipe(source(inputFile))
        .pipe(buffer())
        .pipe(sourcemaps.init({'loadMaps': true, 'debug': true}))
            // Add transformation tasks to the pipeline here.
            .pipe(uglify())
            .on('error', onError)
        .pipe(sourcemaps.write('./'))
        .pipe(gulp.dest(path.join(root, 'build/')));
    cb();
}

function watchTask(cb) {
    root = ROOT
    gulp.watch([path.join(root, 'sass/**/*.scss')], sassTask);
    gulp.watch([
        path.join(root, 'js/**/*.js'),
        path.join(root, 'js/**/*.hbs')
    ], browserifyTask);
    gulp.watch([
        path.join(root, 'html/**'),
        path.join(root, 'fonts/**')
    ], syncStaticAssets)
    cb();
}


function cleanTask(cb){
    gulp.src([ROOT + '/build/*/*.*']).pipe(clean());
    cb();
}

gulp.task('vendor-build-js', function(cb) {
    var onError = function() {
        var args = Array.prototype.slice.call(arguments);
        notify.onError({
            'title': 'JS Compile Error',
            'message': '<%= error.message %>'
        }).apply(this, args);
        // Keep gulp from hanging on this task
        this.emit('end');
    };

    var b = browserify()
        .require(VENDOR_CONFIG.src);

    b.bundle(logBundle(VENDOR_CONFIG.target))
        .pipe(source(VENDOR_CONFIG.target))
        .pipe(buffer())
            // Add transformation tasks to the pipeline here.
            .pipe(uglify())
            .on('error', onError)
        .pipe(gulp.dest(VENDOR_CONFIG.targetDir));
    cb();
});

gulp.task('help', taskListing);

function syncStaticAssets(cb) {
    gulp.src([
        path.join(ROOT, 'html/**'),
        path.join(ROOT, 'fonts/**'),
        path.join(ROOT, 'img/**'),
        path.join(ROOT, 'js/jquery.tablesorter.js'),
        path.join(ROOT, 'js/jquery-latest.js')
    ], { allowEmpty: true }).pipe(gulp.dest(path.join(ROOT, 'build')));
    cb();
}

gulp.task('sync-static-assets', syncStaticAssets);

gulp.task('sass', sassTask);
gulp.task('browserify', browserifyTask);
gulp.task('build', series('sass', 'browserify', 'sync-static-assets', 'vendor-build-js'));
gulp.task('watch', series('build', watchTask));
gulp.task('clean', cleanTask);

exports.default = series('help');