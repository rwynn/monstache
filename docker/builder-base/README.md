# Monstache golang builder base image

This image will be used as a golang base image to build the tool

It installs required packages to install dependencies and build the app, creating it and pushing it to docker hub, will save the time it takes to build this image on every machine

It uses `golang:alpine` as a base and installs the following packages:

`go`
`git`

`gcc`
`musl-dev`

`make`
`zip`
