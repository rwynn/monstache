# Monstache golang builder base image

This image will be used as a golang base image to build the tool

It uses `golang:alpine` as a base and installs the following packages:

`go`
`git`

`gcc`
`musl-dev`

`make`
`zip`

## TODO

- [ ] Maybe move this code to it's own repo, and create an automated build on docker hub