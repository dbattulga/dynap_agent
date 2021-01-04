# Flink Composer API

### Run 
`docker-compose up -d`

## Usage

**2 endpoints:**

- `/jobs` GET and POST
- `/jobs/<name>` GET and DELETE 

### Register a new job

**Definition**

`POST /jobs`

**Arguments**

- `"job_name":string` unique name for this job
- `"spe_address":string` the IP address of the Flink instance
- `"source_broker":string` the IP address of source MQTT broker
- `"sink_broker":string` the IP address of sink MQTT broker
- `"source_topic":string` subscriber topic of the job
- `"sink_topic":string` publisher topic of the job
- `"entry_class":string` entry class of the JAR if it contains multiple
- `"job_path":string` link to local directory of the existing JAR file, indicates from where should the JAR file be loaded to Flink

**Example**

```json
  {
    "job_name": "A_unique",
    "flink_address": "http://10.188.166.98:8081",
    "source_broker": "tcp://10.188.166.98:1883",
    "sink_broker": "tcp://10.188.166.98:1883",
    "source_topic": "T-1",
    "sink_topic": "T-N",
    "entry_class": "flinkpackage.FlowCheck",
    "job_path": "./jars/flinktest-1.jar"
  }
```

**Response**

- `201 Created` on success

responses from local Flink Job manager are: jobname, JarID, JobID, location and class
additional fields from agent are: source_broker, sink_broker, source_topic and sink_topic
additionally
those fields should be saved in local DB

```json
  {
    "jobname": "A_unique",
    "jarid": "97888c54-1d69-44b7-8586-15ba0ae1b7b3_flinktest-1.jar",
    "jobid": "a5cf9ff2bab498f755f49c144e5044c7",
    "flink_address": "http://10.188.166.98:8081",
    "source_broker": "tcp://10.188.166.98:1883",
    "sink_broker": "tcp://10.188.166.98:1883",
    "source_topic": "T-1",
    "sink_topic": "T-N",
    "class": "flinkpackage.FlowCheck",
    "job_path": "./jars/flinktest-1.jar"
  }
```

Other APIs:

### List all Jobs
`GET /jobs`

## Lookup jobs details
`GET /jobs/<name>`

## Delete a job
`DELETE /jobs/<name>`
