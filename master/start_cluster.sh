
LOGS_DIR=logs
MASTER=0 # means that nodes is a master
MASTER_ADDR=$(hostname -I | awk '{print $1}')
REPO_DIR=img2dataset_docker # directory where the repository is cloned
SCRIPT="${REPO_DIR}/start_docker.sh" # script to start the docker
MEM_IN_GB=30 # memory in GB
CLUSTER_TYPE=ray # cluster type

bash $SCRIPT $LOGS_DIR $MASTER $MASTER_ADDR $REPO_DIR $MEM_IN_GB $CLUSTER_TYPE