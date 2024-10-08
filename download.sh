file_master_id=$1
conf_value=$6
cluster_id=$2
workflow_id=$3
process_id=$4
write_batch_id_to_file='Y'
batch_file_path=$5


/usr/lib/spark/bin/spark-submit $conf_value FileDownloadHandler.py $file_master_id  $cluster_id $workflow_id $process_id $write_batch_id_to_file $batch_file_path
