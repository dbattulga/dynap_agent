import requests


# exposed metrics:
# input data rate of a Job (count per second)
# output data rate of a Job (count per second)
# total number of records in
# total number of records out
# total number of task slot for JM (assuming a node has only 1 JM)
# number of available task slot for JM (assuming a node has only 1 JM)


def get_input_data_rate(base_url, job_name):
    response = requests.get('http://localhost:9090/api/v1/query', params={'query': 'flink_taskmanager_job_task_operator_numRecordsInPerSecond'})
    # response.json()['data']['result'][0] to [N] is the source, sink operators of each job, weird!
    result = response.json()['data']['result']
    for op_count in range(len(result)):
        if "Sink" in result[op_count]['metric']['operator_name']: 
            if result[op_count]['metric']['job_name'] == job_name:
                return result[op_count]['value'][1]


def get_output_data_rate(base_url, job_name):
    response = requests.get('http://localhost:9090/api/v1/query', params={'query': 'flink_taskmanager_job_task_operator_numRecordsOutPerSecond'})
    # response.json()['data']['result'][0] to [N] is the source, sink operators of each job, weird!
    result = response.json()['data']['result']
    for op_count in range(len(result)):
        if "Source" in result[op_count]['metric']['operator_name']: 
            if result[op_count]['metric']['job_name'] == job_name:
                return result[op_count]['value'][1]


def get_input_records_count(base_url, job_name):
    response = requests.get('http://localhost:9090/api/v1/query', params={'query': 'flink_taskmanager_job_task_operator_numRecordsIn'})
    # response.json()['data']['result'][0] to [N] is the source, sink operators of each job, weird!
    result = response.json()['data']['result']
    for op_count in range(len(result)):
        if "Sink" in result[op_count]['metric']['operator_name']: 
            if result[op_count]['metric']['job_name'] == job_name:
                return result[op_count]['value'][1]


def get_output_records_count(base_url, job_name):
    response = requests.get('http://localhost:9090/api/v1/query', params={'query': 'flink_taskmanager_job_task_operator_numRecordsOut'})
    # response.json()['data']['result'][0] to [N] is the source, sink operators of each job, weird!
    result = response.json()['data']['result']
    for op_count in range(len(result)):
        if "Source" in result[op_count]['metric']['operator_name']: 
            if result[op_count]['metric']['job_name'] == job_name:
                return result[op_count]['value'][1]


def get_total_task_slots(base_url):
    response = requests.get('http://localhost:9090/api/v1/query', params={'query': 'flink_jobmanager_taskSlotsTotal'})
    # response.json()['data']['result'][0] to [N] is the source, sink operators of each job, weird!
    result = response.json()['data']['result']
    return result[0]['value'][1]


def get_available_task_slots(base_url):
    response = requests.get(base_url+':9090/api/v1/query', params={'query': 'flink_jobmanager_taskSlotsAvailable'})
    # response.json()['data']['result'][0] to [N] is the source, sink operators of each job, weird!
    result = response.json()['data']['result']
    return result[0]['value'][1]


#print('input data rate of a_job: ', get_input_data_rate('A_job'))
#print('input data rate of b_job: ', get_input_data_rate('B_job'))
#print('output data rate of a_job: ', get_output_data_rate('A_job'))
#print('output data rate of b_job: ', get_output_data_rate('B_job'))
#print('input count of a_job: ', get_input_records_count('A_job'))
#print('input count of b_job: ', get_input_records_count('B_job'))
#print('output count of a_job: ', get_output_records_count('A_job'))
#print('output count of b_job: ', get_output_records_count('B_job'))

#print('total # of TS: ', get_total_task_slots())
#print('available # of TS: ', get_available_task_slots())

# will be enriched with other metric functions