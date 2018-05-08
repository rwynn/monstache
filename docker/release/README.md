# Monstache release builder

You can use the `build.sh` script in this folder to build a monstache release using docker.

Begin by updating the `.tag`, `.url`, and `.version` files.  These variables are used to label the resulting
image. The `.version` and `.tag` files should include the version of monstache in monstache.go in the root folder.

Run `build.sh` from this directory to build the docker container.

Run `docker inspect` on the resulting tag name to verify the labels were applied properly.
