# Monstache local builder

The `build.sh` script builds all binaries (for build targets: `linux, win, mac`) in docker
and exports them to a local folder called `docker-build` using the script `build.sh`

Note: the script will remove an existing `docker-build` -if any- before exporting the new one

## Prerequisties

- Docker
- Bash shell

## Steps

- Open a terminal and change directory to this folder using: `cd <path/to/this/folder>`
- Run script `./build.sh`

## Result

You should see a `docker-build` folder with contents:

```bash
darwin-amd64           linux-amd64           monstache-0e52792.zip           windows-amd64
```

All folders will contain 3 files: `md5.txt`, `sha256.txt` & binary: [`monstache` (linux & mac) or `monstache.exe` (windows)]

### Run binaries

- Linux: If your host is linux you may need to install `musl` to run the resulting binary. For example, on a debian system

```bash
sudo apt install musl
```
