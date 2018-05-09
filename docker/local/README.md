# Monstache local builder

You can use the `build.sh` script in this folder to build monstache to your host machine using docker.

Run `build.sh` from this directory and the monstache binaries for all supported platforms (linux,win,mac)
will be built to a `docker-build` folder.

If your host is linux you may need to install `musl` to run the resulting binary. For example, on a debian system

```
sudo apt install musl
```
