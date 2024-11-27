# Any2dataset distributed downloading
Tools to manage any2dataset Ray (or Spark) base cluster for downloading large number of media files in distributed manner.

## Table of contents
- [Definitions](#definitions)
- [Installation](#installation)
- [Environment variables](#environment-variables)
- [Guide for Starting a cluster](#starting-a-cluster)
- [Managing the cluster](#managing-the-cluster)


## Definitions

- **any2datset:** a minimalistic vesrion of a tool based o[img2dataset](https://github.com/rom1504/img2dataset.git) and [any2dataset](https://github.com/rom1504/any2dataset.git) that allows easily download large number of *any* media pairs (audio, video, images, docs etc). 
- **Storage machine:** a remote machine that can be connected via ssh that stores metadata for downloading (URL-text pairs) as well as output (downloaded files). Storage machine cannot be used for downloading, that's why we need another kinf of machine (see below).
- **Cluster machine:** a remote machine that performs downloading.
- **Cluster:** in this setup, it's just a set of remote VMs that perform downloading in distributed manner.
- **Cluster master:** a master (or head) node of the cluster. This is the machine that will run master proces, i.e. it will assign tasks to wokers and keep track of completed tasks.
- **Cluster worker:** a machine that perfoms the task (in out case downloading).
- **Cluster dashboard:** web-based interface that allows cluster mointoring. In Ray case it's should be located http://<master_ip>:8265/. 
- **Setup script:** this is a bash script that should be run on cluster machines that sets everything up - installs all requirements, mounts storage directories etc. See [this file](scripts/ray_audio.sh).

## Installation
Install the requirements:
```bash
pip install -r requirements.txt
```

## Environment variables

```bash
# Storage machine variables
MOUNT_KEY="ssh key to output host that is located on your local machine"
# Note: mount key on cluster machines will always be located at ~/.ssh/mount_key so you don't need to set it separately on cluster machines

# The next three variables should be set on each worker/master node
MOUNT_DIR="output directry on the host machine"
MOUNT_USER="username for the output host"
MOUNT_HOST="hostanme"
# Cluster machines variables
SSH_KEY="ssh key of cluster machines"
USER="username of cluster machines"
```
## Starting a cluster

The cluster should be starting on the master node. 
Steps to start the cluster:
0. Create a csv file with hostnames and save it somewhere:
```csv
,host,user,password,hostname,comment
0,192.81.209.210
1,65.109.9.187
```
1. So first, we select a master node for the list of ip addresses of available machines.
2. The we create two JSON files - a cluster config (see [this config](cluster_configs/ray_audio.json)) and a cluster specific file that should store information about workers and head node (see [this file](clustersr/ay_audio.json)):
```json
{
    "cluster_id": "ray_audio",
    "status": "running",
    "master_node": "master_ip",
    "workers": [],
    "num_workers": 0,
    "mem_gb": 30,
    "master_url": "http://master_ip:8265/nodes?view=summary"
}
```
Clsuter id should be equal to cluster config filename. Master node should be set to the chosen master IP address and master URL should also be set accordingly.
3. Set [environment variables](#environment-variables) accordingly. *Note:* you may need to set the same variables on cluster machines. You can set them by first setting them first on your machine (by modifying in running [this script](scripts/set_env.sh)) and then setting up the cluster and adding more nodes (see below).
4. **Setting the cluster up.** 
In [manage_cluster.sh](manage_cluster.sh) there's an example of setting up a cluster master node (see setup section).
Then you need to ssh into your master node and copy all three files in the [master](/master) directory into ypur work directory on the remote machine (`cd /home/cluster/`). 
5. The last steps are launching the master process (master node will run one worker process as well):
`bash start_cluster`
You will be able to connect to ypour cluster dashboard via opeining http://<master_ip>:8265 in yout browser.
Then if evrything works, run the job: `bash run_job`.
*Note 1:* you can more worker nodes from your host list (a csv file) to your cluster by running command similar to one from [manage_cluster.sh](manage_cluster.sh) (second section), You can do either after you start ypur job or before, but always after you properly set you cluster up.

*Note 2:* you should be able to see your job logs in the `/home/cluster/my.log` file.

## Managing the cluster

Besides adding and killing worker nodes, managing the cluster comes to managing the master node.
We use docker containers to run the master/worker nodes to make it easier to setup everything.
To kill the master process (and the cluster), you can do the following:
1. Get the container ID that master process is running in and copy t:
`docker ps`
2. Kill the container:
`docker kill <docker_id>`
Then you can restart the cluster and the job by running commands from the [step 5](#starting-a-cluster).
