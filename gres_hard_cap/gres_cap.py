from pandas import DataFrame as DF
from slurm_analyzer import SLURMAnalyzer
import json
import os
import pandas as pd
from datetime import datetime

import subprocess

base_path = './'
partition = 'pli-c'
resource_script_path = './resource_checker.sh'

exceeding_message = "You have exceeded the High priority GPU hour quota"

def email_hpgres_cap_warning(user, message, job_ids, job_names):
    
    header = "[PLI-CP] High Priority GPU Quota Exceeded"
    
    body = f"Dear user {user},\n\nOur monitoring system detected that you have exceeded the designated quota for high priority GPU (pli-pc). Please cancel jobs launched on pli-cp and use the general partition pli-c instead.\nWe have a 1-day grace period for the quota. Jobs launched tomorrow will be scanceled directly.\n\nThank you for your cooperation.\n\n{message}\n"
    
    for job_id, job_name in zip(job_ids, job_names):
        body += f"Job ID: {job_id}: \t {job_name}\n"
    print(header, body)
    #TODO: Email the user

def email_hpgres_cap_canceling(user, message, job_ids, job_names):
    header = "[PLI-CP] High Priority GPU Quota Exceeded"
    
    body = f"Dear user {user},\n\nOur monitoring system detected that you have exceeded the designated quota for high priority GPU (pli-pc). We have canceled the following jobs. Please use the pli-c partition instead. Thanks for your cooperation\n\n{message}\n"
    for job_id, job_name in zip(job_ids, job_names):
        body += f"Job ID: {job_id}: \t {job_name}\n"
    print(header, body)
    #TODO: Email the user

def cancel_job(job_id):
    print(f"Canceling job {job_id}")
    #TODO: Cancel job
    # os.system(f"scancel {job_id}")

if __name__ == '__main__':

    tmp_path = os.path.join(base_path, '.4h_log.json')
    os.system(f"sacct --starttime=$(date -d \"4 hour ago\" +\"%Y-%m-%dT%H:%M:%S\") --allusers --partition {partition} --json >| {tmp_path}")
    df = SLURMAnalyzer().parse(json.load(open(tmp_path)), short_jobs_mask=False)

    df['submission_time'] = pd.to_datetime(df['submission_time'])
    df = df[df['submission_time'] >= (datetime.now() - pd.Timedelta(hours=4))]
    users = set(df['user'].unique())

    for user in users:
        result = subprocess.run([f"bash {resource_script_path} -u {user}"], shell=True, check=True, text=True, capture_output=True)
        if exceeding_message in result.stdout:
            print(f"User {user} has exceeded the High priority GPU hour quota")
            job_ids = df[df['user'] == user]['job_id'].values
            job_names = df[df['user'] == user]['job_name'].values

            yesterday_result = subprocess.run([f"bash {resource_script_path} -u {user} -y"], shell=True, check=True, text=True, capture_output=True)
            if exceeding_message in yesterday_result.stdout:
                email_hpgres_cap_canceling(user, result.stdout, job_ids, job_names)
                for job_id in job_ids:
                    continue
                    # os.system(f"scancel {job_id}")
            else:
                email_hpgres_cap_warning(user, result.stdout, job_ids, job_names)

    os.system(f"rm {tmp_path}")
