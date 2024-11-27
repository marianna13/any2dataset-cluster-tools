MASTER_ADDR=$1
MEM_IN_GB=$2
CLUSTER_TYPE=$3
echo "Mem in GB setup: $MEM_IN_GB"
WORK_DIR="/home/cluster"
mkdir -p $WORK_DIR
REPO_DIR="/home/cluster/img2dataset_docker"
LOGS_DIR="/home/cluster/logs"
mkdir -p $LOGS_DIR
REPO_ID="https://github.com/marianna13/img2dataset_docker.git"
sudo apt-get update -y && sudo apt-get install git -y
rm -rf $REPO_DIR
git clone $REPO_ID $REPO_DIR

setup
bash $REPO_DIR/setup.sh $CLUSTER_TYPE &
wait
echo "Setup done"

# mount
KEY="~/.ssh/mount_key"
LOCAL_DIR="/home/laion/mount"
unount $LOCAL_DIR
mkdir -p $LOCAL_DIR
USER=$MOUNT_USER
HOST=$MOUNT_HOST

cmd="chmod 600 $KEY"
bash -c "$cmd"
# ssh-keyscan judac.fz-juelich.de >> ~/.ssh/known_hosts
apt install sshfs -y

CMD="sshfs -o StrictHostKeyChecking=no -o allow_other,reconnect,auto_cache,idmap=user \
        -o IdentityFile=$KEY $USER@$HOST:$MOUNT_DIR $LOCAL_DIR"

echo $CMD
bash -c "$CMD"

# check if mounted
ls $LOCAL_DIR | wc -l

DOWNLOAD_DIR="/p/data1/mmlaion/audio500m"
LOCAL_DOWNLOAD_DIR="/home/laion/mount-download"

mkdir -p $LOCAL_DOWNLOAD_DIR
umount $LOCAL_DOWNLOAD_DIR
CMD="sshfs -o StrictHostKeyChecking=no -o allow_other,reconnect,auto_cache,idmap=user \
        -o IdentityFile=$KEY $USER@$HOST:$DOWNLOAD_DIR $LOCAL_DOWNLOAD_DIR"

echo $CMD
bash -c "$CMD"
echo "Mount done"
# Start master
LOCAL_IP=$(hostname -I | awk '{print $1}')
echo "Local IP: $LOCAL_IP"

if [ "$LOCAL_IP" == "$MASTER_ADDR" ]; then
    bash $REPO_DIR/start_docker.sh \
        $LOGS_DIR 0 $MASTER_ADDR $REPO_DIR $MEM_IN_GB $CLUSTER_TYPE

    echo "Master started"
fi


echo "Master Address: $MASTER_ADDR"
echo $CLUSTER_TYPE
# Start worker
if [ "$LOCAL_IP" != "$MASTER_ADDR" ]; then
    echo "Worker starting"
    docker rm -v -f $(docker ps -qa)
    bash $REPO_DIR/start_docker.sh \
        $LOGS_DIR 1 $MASTER_ADDR $REPO_DIR $MEM_IN_GB $CLUSTER_TYPE

    echo "Worker started"
fi
