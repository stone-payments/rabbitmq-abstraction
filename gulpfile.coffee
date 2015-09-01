gulp = require 'gulp'
args = require('yargs').argv
dotnet = require 'gulp-dotnet-utils'

pkg = require './package.json'

configuration = if args.debug then 'Debug' else 'Release'

gulp.task 'default', ['build']

gulp.task 'build', ['restore'], ->
  dotnet.build configuration, ['Clean', 'Build'], toolsVersion: 14.0

gulp.task 'clean', -> dotnet.build configuration, ['Clean'], toolsVersion: 14.0

gulp.task 'restore', -> dotnet.exec 'nuget restore'

gulp.task 'pack', ->
  dotnet.nuget.pack 'src/RabbitMQ.Abstraction.csproj', pkg.version,
    symbols: true
    configuration: configuration

gulp.task 'bump', ->
  dotnet.bump pkg.version
