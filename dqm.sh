file_master_id=$1
cluster_id=$2
workflow_id=$3
process_id=$4
batch_id=$5
conf_value=$6" --name DQMCheckHandler_"${file_master_id}${batch_id}
batch_id_list=$7

/usr/lib/spark/bin/spark-submit $conf_value DQMCheckHandler.py $file_master_id $cluster_id $workflow_id $process_id $batch_id $batch_id_list