# How to start new release branch

We'd like to start a new branch, say `0.19.0`

## Create new branch
```bash
git checkout -b 0.19.0
```
## Edit `release` file
```bash
echo "0.19.0" > release
git add .
git commit -m "0.19.0"
```
## Build manifests and binaries
```bash
./dev/go_build_all.sh
git add .
git commit -m "env: manifests"
git push
```
## Check github action progress
Navigate to github **Actions**
![action menu](./img/github_actions_menu.png "action menu")
Check action result.
![action item](./img/github_actions_item.png "action item")
