MASTER_ADDR=$(hostname -I | awk '{print $1}')
NAME="img2dataset"

URL_LIST=/home/laion/mount # input directory
OUTPUT_DIR=/home/laion/mount-download
NUM_CORES=32
URL_COL=url # url column
CAPTION_COL=alt # text column
SCRIPT="any2dataset.py"
encode_format=mp3 # the column name in the webdataset

CMD="docker exec $NAME \
    python -u $SCRIPT \
    --master_node $MASTER_ADDR \
    --num_proc $NUM_CORES \
    --url_col $URL_COL \
    --caption_col $CAPTION_COL \
    --output_dir $OUTPUT_DIR \
    --encode_format $encode_format \
    --url_list $URL_LIST"

nohup $CMD > my.log 2>&1 &