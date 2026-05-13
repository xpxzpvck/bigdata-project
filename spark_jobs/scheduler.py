import time
import subprocess
import schedule
from datetime import datetime

def run_batch_jobs():
    print(f"[{datetime.now()}] Starting batch jobs...")
    try:
        subprocess.run([
            "spark-submit", 
            "--master", "spark://spark-master:7077", 
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
            "batch/hourly_report_job.py"
        ], check=True)
        
        subprocess.run([
            "spark-submit", 
            "--master", "spark://spark-master:7077", 
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
            "batch/editor_patterns_job.py"
        ], check=True)
        
        print(f"[{datetime.now()}] All batch jobs completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"[{datetime.now()}] Error occurred while executing Spark job: {e}")

if __name__ == "__main__":
    run_batch_jobs()
    
    schedule.every().hour.do(run_batch_jobs)
    
    # schedule.every(5).minutes.do(run_batch_jobs)
    
    print(f"[{datetime.now()}] Scheduler started. Waiting for the next run...")
    
    while True:
        schedule.run_pending()
        time.sleep(1)