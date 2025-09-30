## Resource Checker
`resource_checker.sh` serves as a bash script for users to get real-time remaining quota.

### Options

- `-u`, `--user` `<username>`  
  Specify the username to check usage for. Default: `$USER`

- `-c`, `--cap` `<cap>`  
  Set the maximum GPU hour limit (quota) for the user. Default: `500.0`

- `-p`, `--partition` `<partition_name>`  
  Specify the partition name to check GPU usage within. Default: `pli-c` **(need modification for the new qos name)**

- `-y`, `--yesterday`  
  Calculate usage from the first day of the current month until yesterday. If not set, calculates up to the current date.

### Sample Output
```
Start Date: 2024-11-01
End Date: 2024-11-03-20:45:57
User: some-user-name

High Priority GPU hrs since 2024-11-01:  116.97 hours
Remaining HP GPU hrs before 2024-11-30:  383.03 hours
███████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  23.39% (117.0/500.0)

The quota will be reset to 500.0 on 2024-12-01
```

## High Priority GPU Cap

Execute `python gres_cap.py` will scan the queue for active jobs submitted in a particular QoS and email / scancel the jobs of the user if they exceed the quota.

**TODO:** implement the email sending function and job canceling function.

The current script scans over all users launching jobs in the past 4 hours. Users will be sent a warning email if they have usage of pli-pc GPU hours exceeding the designated threshold.

If they exceeded the threshold one day ago. The jobs will also be automatically canceled once detected.