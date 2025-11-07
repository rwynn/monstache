# Motion Fork Configuration

## Motion-Specific Changes

### Enhanced Logging Format
- **Improved system log format**: Added structured logging with process ID and UTC timestamps
- **Format**: `monstache[PID]: LEVEL message` with standardized timestamp format  
- **Benefits**: Better integration with system logging and monitoring tools

### Improved Workflows
- **Disable Docker Hub**: Upstream uses DockerHub we use ACR
- **Weekly upstream sync**: Automatically syncs with upstream every Monday
- **Updated datadog build**: Main `monstache/datadog` now uses this fork as base image

## Usage

- **Automatic builds**: Push to `rel6` branch → builds `6.8.1-a1b2c3d4` (upstream version + commit SHA)  
- **Weekly sync**: Mondays at 9 AM UTC - auto-syncs with upstream, triggers build if updated
- **Force build**: Use GitHub Actions "Build Monstache Fork" workflow  
- **Force sync**: Manual "Sync with Upstream" workflow trigger
- **Images**: `{ACR}/monstache/fork:6.8.1-a1b2c3d4` (no latest tag)

## FAQ

**Q: Why rename instead of delete the upstream workflow?**
A: Git's rename detection ensures upstream changes still get applied to the `.disabled` file automatically. This keeps it updated for future reference without conflicts.

**Q: Will the .disabled file run?**  
A: No. GitHub Actions only processes `.yml` and `.yaml` files. The `.disabled` extension prevents execution.

**Q: What if I want to re-enable it later?**
A: Simply rename it back: `git mv release-docker-images.yml.disabled release-docker-images.yml`