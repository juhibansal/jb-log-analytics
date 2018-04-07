
loginput_file="log.csv"
inactivityinput_file="inactivity_period.txt"
output_file="sessionization.txt"

/mnt/alpha/3p/spark/bin/spark-submit --executor-memory 16g --total-executor-cores 30 --executor-cores 30 --conf spark.ui.port=4446  src/sessionization.py  "${loginput_file}" "${inactivityinput_file}" "${output_file}"  2>err2
