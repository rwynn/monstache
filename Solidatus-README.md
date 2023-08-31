# 2023-05-03

Created as

- Currently we have 4 High CVEs from the bundled monstache gobinary @ `6.7.7`
- By updating to `6.7.10` we get down to 2. This version should work fine, I have used it a bit in past when looking at a different issue where I thought monstache update may help
- By updating to `6.7.11` (latest at time of writing) we get down to 1 CVE left. HOWEVER, this version has bug that breaks our sync. I commented about this on GH mid last month, had no response, no fix :disappointed:

SO:

- added branch `v6.7.10.x`
- cherry-picked all commits from `rel6` that update Mongo driver or other dependencies. This fixes all CVEs
- Updated version to `6.7.10.0` in `monstache.go`
- Created new release by using new `./build.sh` script

# 2023-06-20

Golang has 3 vulnerabilities CVE-2023-24540, CVE-2023-29400, CVE-2023-24539 which were fixed in 1.20.4 hence manually updated go.mod to use 1.20. (latest on main branch was 1.19)

# 2023-08-24

Golang has 3 vulnerabilities CVE-2023-29409, CVE-2023-29406, CVE-2023-39533 which were fixed in 1.20.7 hence manually updated go.mod to use 1.21. (latest on main branch was 1.20)
