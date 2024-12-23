# Adding a new key for mounting remote machines (JUDAC, etc)

1. On your local machine, create an ssh key :

```bash
MOUNT_KEY_NAME=YOUR_FAVORITE_KEY_NAME

ssh-keygen -a 100 -t ed25519 -f ~/.ssh/$MOUNT_KEY_NAME
```
2. Add the key to JUDAC and test the key:

a. Copy key over to any of the remote machines

```bash
rsync -v --stats --progress -e ssh  ~/.ssh/$MOUNT_KEY_NAME* user@xx.xxx.xxx.xxx://HOME_REMOTE_PATH/.ssh/
```

b. Go to any remote machine, eg

```bash
REMOTE_MACHINE_KEY=YOUR_REMOTE_MACHINE_KEY
ssh -i .ssh/$REMOTE_MACHINE_KEY user@xx.xxx.xxx.xxx
```

c. Execute 

```bash
nslookup `curl ipinfo.io/ip`
```
and get something like

```bash
name = static.xxx.xxx.xxx.xxx.clients.your-server.de
```

"from=" clause in JUDOOR when adding the key to JUDAC should look like

```bash
from="*.clients.your-server.de"
```

d. Test transfer with rsync:

```bash
USER_JUDAC="YOUR_JUDAC_USERNAME"
echo "Greets from local: $RANDOM" >> test_$RANDOM.local
MOUNT_KEY_NAME=YOUR_MOUNT_KEY_NAME
rsync -v --stats --progress -e "ssh -i /root/.ssh/${MOUNT_KEY_NAME}" ./test_*.local  ${USER_JUDAC}@judac.fz-juelich.de:/p/data1/mmlaion/tmp/test/
```

e. If this works, key is fine and you can proceed with cluster setup, where the process will make sure to copy the key across all remote machines. Follow up [here](../README.md#starting-a-cluster).