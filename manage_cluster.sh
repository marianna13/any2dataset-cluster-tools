# Setup the cluster
python spawn.py \
    --cluster_config "cluster_configs/ray_audio.json" \
    --action "setup" \
    --logs_dir "clusters" \
    --hosts_path "hosts.csv" \
    --num_nodes 1

# Adding 5 nodes to the cluster
python spawn.py \
    --cluster_config "cluster_configs/ray_audio.json" \
    --action "add_nodes" \
    --logs_dir "clusters" \
    --hosts_path "hosts.csv" \
    --num_nodes 5

# Killing 5 nodes
python spawn.py \
    --cluster_config "cluster_configs/ray_audio.json" \
    --action "kill_nodes" \
    --logs_dir "clusters" \
    --hosts_path "hosts.csv" \
    --num_nodes 5

