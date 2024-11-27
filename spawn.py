import paramiko
from scp import SCPClient
import requests
from bs4 import BeautifulSoup
import pandas as pd
import multiprocessing as mp 
import argparse
import os
import json
import glob
import socket
import dotenv


# set random seed for reproducibility
import random
random.seed(0)

dotenv.load_dotenv()


username = os.environ.get("USERNAME")
ssh_key= os.environ.get("SSH_KEY")
mount_key = os.environ.get("MOUNT_KEY")

def get_env_vars():
    env_vars_keys = ["MOUNT_DIR", "MOUNT_USER", "MOUNT_HOST"]
    env_vars = {}
    for key in env_vars_keys:
        env_vars[key] = os.environ.get(key)
    return env_vars

def set_env_on_remote(ssh_key, host, username):
    env_vars = get_env_vars()
    client = create_ssh_client(host, username, ssh_key)
    for key, val in env_vars.items():
        stdin, stdout, stderr = client.exec_command(f"echo 'export {key}={val}' >> ~/.bashrc")
    client.close()


def get_hosts(url):
    try:
        if "8080" in url:
            r = requests.get(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            table = soup.find('table')
            print(url)
            df = pd.read_html(str(table))[0]
            print(df)
            df = df[df["State"] == "ALIVE"]
            df["Address"] = df["Address"].apply(lambda x: x.split(":")[0])
            host_list = df["Address"].tolist()
        else:
            headers = {
                    'Accept': 'application/json, text/plain, */*',
                    'Connection': 'keep-alive',
                    'Referer': 'http://127.0.0.1:8265/',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
                    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                }

            params = {
                'view': 'summary',
            }
            print(url)

            r = requests.get(url, params=params, headers=headers)
            data = r.json()["data"]["summary"]
            df = pd.DataFrame(data).dropna()
            host_list = df["ip"].apply(lambda x: x.split(" ")[0]).tolist()

    except Exception as err:
        print(f"Error getting hosts: {err}")
        return [], "Error getting hosts"
    if r.status_code != 200:
        return [], "Error getting hosts"
    
    return host_list, None

def create_ssh_client(server, user, ssh_key):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key_file(ssh_key)
    client.connect(server, username=user, pkey=pkey,timeout=10,allow_agent=False,look_for_keys=False)
    return client


def run_command(command, ssh_key, host, username):
    client = create_ssh_client(host, username, ssh_key)
    stdin, stdout, stderr = client.exec_command(command)
    print(stdout.read().decode())
    print(stderr.read().decode())
    client.close()


def scp_file(ssh_key, host, username, input_file, output_file):
    client = create_ssh_client(host, username, ssh_key)
    with SCPClient(client.get_transport()) as scp:
        scp.put(input_file, output_file)
    client.close()


def setup_node(ssh_key, host, username, master_addr, mem_gb=1, bash_file="setup_node.sh"):
    if bash_file is None:
        bash_file = "setup_node.sh"
    bash_file_name = bash_file.split("/")[-1]
    scp_file(ssh_key, host, username, bash_file, f"/{username}/{bash_file_name}")

    set_env_on_remote(ssh_key, host, username)

    client = create_ssh_client(host, username, ssh_key)
    stdin, stdout, stderr = client.exec_command(f"chmod +x /{username}/{bash_file_name}")
    stdin, stdout, stderr = client.exec_command(f"bash /{username}/{bash_file_name} {master_addr} {mem_gb} ray")
    while True:
        print(stdout.readline())
        if stdout.channel.exit_status_ready():
            break

    stdin, stdout, stderr = client.exec_command(f"rm /{username}/{bash_file_name}")
    client.close()

def resolve_hostname(host):
    return socket.gethostbyname(host)

def check_if_ip(host):

    try:
        socket.inet_aton(host)
        return True
    except socket.error:
        return False


def spawn_workers(ssh_key, hosts, username, master_addr, num_nodes, existing_nodes, bash_file):
    
    print(len(hosts))
    hosts = [host for host in hosts if host not in existing_nodes] 
    hosts = sorted(hosts) 
    hosts = hosts[:num_nodes]
    num_existing_nodes = len(existing_nodes)
    for host_id, host in enumerate(hosts):
        if not check_if_ip(host):
            host = resolve_hostname(host)
        print("=====================================")
        print(f"Setting up node {host}, {host_id+1+num_existing_nodes}/{num_nodes+num_existing_nodes}")
        print("=====================================\n")
        if host == master_addr:
            continue
        scp_file(ssh_key, host, username, mount_key, f"~/.ssh/mount_key")
        setup_node(ssh_key, host, username, master_addr, mem_gb=1, bash_file=bash_file)


def spawn_one_worker(ssh_key, host, username, master_addr, mem_gb=1, bash_file=None):
    print(f"Setting up node {host}")
    try:
        scp_file(ssh_key, host, username, mount_key, f"~/.ssh/mount_key")
        setup_node(ssh_key, host, username, master_addr, mem_gb=mem_gb, bash_file=bash_file)
    except Exception as e:
        print(f"Error setting up node {host}: {e}")
        return

def spawn_workers_parallel(ssh_key, hosts, username, master_addr, num_nodes, mem_gb=1, num_proc=4, bash_file=None):
    hosts = hosts[:num_nodes]
    with mp.Pool(num_proc) as pool:
        pool.starmap(spawn_one_worker, [(ssh_key, host, username, master_addr, mem_gb, bash_file) for host in hosts])

def kill_workers(ssh_key, hosts, username, master_addr):
    for host in hosts:
        if host == master_addr:
            continue
        host = host.split(":")[0]
        print(f"Killing node {host}")
        run_command("docker rm -v -f $(docker ps -qa)", ssh_key, host, username)

def check_ram_usage(ssh_key, host, username):
    client = create_ssh_client(host, username, ssh_key)
    stdin, stdout, stderr = client.exec_command("free -m")
    free_ram = stdout.readlines()[1].split()[3]
    print(f"Free RAM: {free_ram} MB")
    client.close()

def check_running_processes(ssh_key, host, username):
    client = create_ssh_client(host, username, ssh_key)
    stdin, stdout, stderr = client.exec_command("ps -aux")
    num_processes = len(stdout.readlines())
    print(f"Number of running processes: {num_processes}")
    client.close()

def check_htop(ssh_key, host, username):
    client = create_ssh_client(host, username, ssh_key)
    stdin, stdout, stderr = client.exec_command("htop")
    # check continuosly
    client.close()

class Cluster:
    """
    #### Cluster
    ##### Purpose: 
    setup cluster, add N more nodes to the cluster, kill N nodes, restart the cluster, kill cluster.

    ##### Attributes: 
    *master_node* (str) - ip address of the master node
    *num_workers* (int) - number of worker nodes
    *mem_gb* (int) - memory in gb assigned to each worker
    *master_url* (str) - URL of ray cluster UI
    *workers* (List[str]) - list of existing worker ips
    *logs_dir (str)* - a path for the directory where save information about this cluster (master node, worker ips, status) will be saved
    *hosts (List[str])* - list of ips of machines that can be launched cluster worker/master
    *cluster_info (dict)* - dictionary containing information about the cluster
    *cluster_logs_dir (str)* - path to the directory where cluster logs will be saved

    ##### Methods
    *__init__(self, num_workers, mem_gb, hosts) -> None* - constructor for the object

    *add_nodes(self, num_nodes) -> None* - adds num_nodes to the cluster

    *kill_nodes(self, kill_nodes) -> None* - kills num_nodes

    *kill(self) -> None* kills cluster

    *_save_info(self) -> None* - saves cluster info by creating a folder inside logs dir and saving there

    *_get_other_clusters_info(self) -> List[dict]* - gets info for each of already running clusters

    #### Constraints and Requirements
    master_node should be unique for each cluster

    worker nodes for each cluster should also be unique (before setting up nodes we check which nodes are not already running other cluster workers)
    """

    def __init__(self, num_workers, mem_gb, hosts, logs_dir, cluster_id=None, bash_file=None):
        
    
        self.num_workers = num_workers
        self.mem_gb = mem_gb
        self.logs_dir = logs_dir
        self.hosts = hosts
        self.cluster_id = cluster_id
        self.bash_file = bash_file

    def _make_safe_name(self, name):
        return name.replace(".", "_").replace(":", "_")
       

    def setup(self):

        for host in self.hosts:
            node_in_other_clusters = self._check_node_in_other_clusters(host)
            node_is_running = self._check_node_already_running(host)
            if not node_in_other_clusters and not node_is_running:
                self.master_node = host
                self.master_url = f"http://{self.master_node}:8080/"
                break



        self.start_master()

        self.add_nodes(self.num_workers-1, bash_file=self.bash_file)


        self.cluster_info = {
            "cluster_id": self.cluster_id,
            "status": "running",
            "master_node": self.master_node,
            "workers": self.workers,
            "num_workers": len(self.workers),
            "mem_gb": self.mem_gb,
            "master_url": self.master_url
        }

        self.cluster_logs_path = f"{self.logs_dir}/{self.cluster_id}.json"

        self._save_info()

    def start_master(self):
        spawn_one_worker(ssh_key, self.master_node, username, self.master_node)
        self.workers = get_hosts(self.master_url)
        self.cluster_info = {
            "cluster_id": self.cluster_id,
            "status": "running",
            "master_node": self.master_node,
            "workers": self.workers,
            "num_workers": 1,
            "mem_gb": self.mem_gb,
            "master_url": self.master_url
        }
        self.cluster_logs_path = f"{self.logs_dir}/{self.cluster_id}.json"
        self._update_info({"workers": self.workers, "status": "running"})
        self._save_info()

    def _check_node_in_other_clusters(self, host):
        other_clusters_info = self._get_other_clusters_info()
        for cluster_info in other_clusters_info:
            if host in cluster_info["workers"]:
                return True
        return False
    
    def _check_node_already_running(self, host):
        if not hasattr(self, "workers"):
            self.workers = []
        return host in self.workers
    
    def _get_info(self):
        with open(self.cluster_logs_path, "r") as f:
            info = json.load(f)
        self.cluster_info = info
        for key, val in info.items():
            setattr(self, key, val)
        return info
        
    def _update_info(self, new_info):
        self.cluster_info.update(new_info)
        self._save_info()

    def _get_workers_from_master(self):
        self.workers, err = get_hosts(self.master_url)
        self._update_info({"workers": self.workers})
        self._save_info()

    def add_nodes(self, num_nodes, bash_file=None):
        self.workers, err = get_hosts(self.master_url)
        if err is not None:
            print(f"Error getting workers: {err}")
            return
        print(self.workers)
        # select num_nodes nodes that are not already running in other clusters
        random.shuffle(self.hosts)
        free_hosts = []
        for host in self.hosts:
            if not check_if_ip(host):
                host = resolve_hostname(host)
            if host == self.master_node:
                continue
            node_in_other_clusters = self._check_node_in_other_clusters(host)
            node_is_running = self._check_node_already_running(host)
            print(f"Node {host} is running: {node_is_running}")
            if not node_in_other_clusters and not node_is_running:
                free_hosts.append(host)
            if len(free_hosts) == num_nodes:
                break
        
        if len(free_hosts) < num_nodes:
            print("Not enough free hosts")
            return
        self.workers, err = get_hosts(self.master_url)
        spawn_workers_parallel(ssh_key, free_hosts, username, self.master_node, num_nodes, mem_gb=self.mem_gb, num_proc=16, bash_file=bash_file)
        self.workers , err= get_hosts(self.master_url)
        self._update_info({"workers": self.workers, "num_workers": len(self.workers), "status": "running"})
        self._save_info()
    
    def kill_nodes(self, num_nodes):
        hosts_to_kill = self.workers[-num_nodes:]
        kill_workers(ssh_key, hosts_to_kill, username, self.master_node)
        self.workers = get_hosts(self.master_url)
        self._update_info({"workers": self.workers, "num_workers": self.num_workers - num_nodes, "status": "running"})
        self._save_info()

    def kill_spcific_nodes(self, hosts):
        kill_workers(ssh_key, hosts, username, self.master_node)
        self.workers = get_hosts(self.master_url)
        self._update_info({"workers": self.workers, "num_workers": len(self.workers), "status": "running"})
        self._save_info()
    
    def kill(self):
        kill_workers(ssh_key, self.workers, username, self.master_node)
        self._update_info({"workers": [], "num_workers": 0, "status": "killed"})
        self._save_info()

    def _save_info(self):
        """
        saves cluster info 
        """
        with open(self.cluster_logs_path, "w") as f:
            json.dump(self.cluster_info, f, indent=4)
        

    def _get_other_clusters_info(self):
        """
        gets info for each of already running clusters
        """
        cluster_infos = []
        for file in glob.glob(f"{self.logs_dir}/*.json"):
            # if getattr(self, "cluster_logs_path", None) and file == self.cluster_logs_path:
            #     continue
            with open(file, "r") as f:
                cluster_info = json.load(f)
                if cluster_info["status"] == "running":
                    cluster_infos.append(cluster_info)

        return cluster_infos


def main():


    parser = argparse.ArgumentParser()
    parser.add_argument('--hosts_path', type=str)
    parser.add_argument('--logs_dir', type=str)
    parser.add_argument('--cluster_config', type=str)
    parser.add_argument('--action', type=str)
    parser.add_argument('--num_nodes', type=int)
    parser.add_argument('--nodes_to_kill', nargs='+', type=str)

    args = parser.parse_args()

    hosts_path = args.hosts_path
    logs_dir = args.logs_dir
    cluster_config = args.cluster_config
    action = args.action
    num_nodes_to_add_or_kill = args.num_nodes

    with open(cluster_config, "r") as f:
        config = json.load(f)
        num_nodes = config["num_nodes"]
        mem_gb = config["mem_gb"]
        cluster_id = config["id"]
        bash_file = config.get("setup_script", None)
        # if bash_file is not None:
        #     bash_file = os.path.join(os.getcwd(), bash_file)

    df = pd.read_csv(hosts_path)
    # df = df[df["comment"] == "hetzner"]
    hosts = df["host"].tolist()


    cluster = Cluster(num_nodes, mem_gb, hosts, logs_dir, cluster_id, bash_file=bash_file)

    if action == "setup":
        print(f"Setting up cluster {cluster_id}")
        cluster.setup()
        cluster_info = cluster._get_info()
        print("Cluster info: ")

        for key, val in cluster_info.items():
            print(f"{key}: {val}")
        
        print("info saved at: ", cluster.cluster_logs_path)
    
    if action == "add_nodes":
        
        cluster_info_path = f"{logs_dir}/{cluster_id}.json"
        print("Cluster info path: ", cluster_info_path)
        cluster.cluster_logs_path = cluster_info_path
        cluster_info = cluster._get_info()
        print(f"Adding {num_nodes_to_add_or_kill} nodes to cluster {cluster_id}")
        print("Cluster info:")
        for key, val in cluster_info.items():
            print(f"{key}: {val}")
        cluster.add_nodes(num_nodes_to_add_or_kill, bash_file=bash_file)
        # check if nodes were added
        cluster_info = cluster._get_info()
        print("Number of nodes in cluster after adding nodes: ", cluster_info["num_workers"])
        

    if action == "kill_nodes":
        cluster_info_path = f"{logs_dir}/{cluster_id}.json"
        print("Cluster info path: ", cluster_info_path)
        cluster.cluster_logs_path = cluster_info_path
        cluster_info = cluster._get_info()
        print(f"Killing {num_nodes_to_add_or_kill} nodes from cluster {cluster_id}")
        cluster.kill_nodes(num_nodes_to_add_or_kill)
    
    if action == "kill":
        cluster_info_path = f"{logs_dir}/{cluster_id}.json"
        cluster.cluster_logs_path = cluster_info_path
        cluster_info = cluster._get_info()
        cluster.kill()

    if action == "kill_spcific_nodes":
        nodes_to_kill: list = args.nodes_to_kill
        cluster_info_path = f"{logs_dir}/{cluster_id}.json"
        cluster.cluster_logs_path = cluster_info_path
        cluster_info = cluster._get_info()
        print(f"Killing {nodes_to_kill} nodes from cluster {cluster_id}")
        cluster.kill_spcific_nodes(nodes_to_kill)
        print("Cluster info after killing nodes: ")
        for key, val in cluster_info.items():
            print(f"{key}: {val}")
    

if __name__ == "__main__":
    main()