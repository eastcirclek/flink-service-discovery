# flink-service-discovery
Discover your Flink clusters on Hadoop YARN and let Prometheus know.
flink-service-discovery runs in two different modes:

* Single mode
  * Given an application ID, it discovers just a single Flink cluster
  identified by the ID.
* Service mode
  * Otherwise, it runs as a standalone service which monitors new
  applications on YARN and for each newly launched application,
  does the same as in the single mode.

flink-service-discovery communicates with YARN ResourceManager and
Flink JobManager via REST APIs, and communicates with Prometheus via
its file-based service discovery mechanism.

## Getting Started

### Prerequisites
Make sure you have all of the following prerequisites installed:
* python 3.4 or higher
* [requests](https://github.com/requests/requests)

### Usage
    usage: discovery.py [-h] [--app-id APP_ID] [--name-filter NAME_FILTER]
                        [--target-dir TARGET_DIR] [--poll-interval POLL_INTERVAL]
                        rm_addr

    Discover Flink clusters on Hadoop YARN for Prometheus

    positional arguments:
      rm_addr               (required) Specify yarn.resourcemanager.webapp.address
                            of your YARN cluster.

    optional arguments:
      -h, --help            show this help message and exit
      --app-id APP_ID       If specified, this program runs once for the
                            application. Otherwise, it runs as a service.
      --name-filter NAME_FILTER
                            A regex to specify applications to watch.
      --target-dir TARGET_DIR
                            If specified, this program writes the target
                            information to a file on the directory. Files are
                            named after the application ids. Otherwise, it prints
                            to stdout.
      --poll-interval POLL_INTERVAL
                            Polling interval to YARN in seconds to check
                            applications that are newly added or recently
                            finished. Default is 5 seconds.

## Examples
### Single mode
Let's discover all the Prometheus exporters of a Flink cluster
identified by "application_1528160315197_0081".
Here I assume that the cluster consists of one JobManager and three
TaskManagers.
#### Output to stdout
```bash
$ python discovery.py http://master:8088 --app-id application_1528160315197_0081
[{"targets": ["slave03:9249", "slave01:9249", "slave02:9250", "slave02:9249"]}]
```
#### Output to a directory
Let's say I have configured prometheus.yml as shown in
[this link](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#%3Cfile_sd_config%3E):
```yaml
scrape_configs:
  - job_name: 'flink-service-discovery'
    file_sd_configs:
      - refresh_interval: 10s
        files:
        - /etc/prometheus/flink-service-discovery/application_*.json
```

As Prometheus is configured to watch
/etc/prometheus/flink-service-discovery/application_*.json,
I want to set --targer-directory to
/etc/prometheus/flink-service-discovery.
When --target-directory is specified, a file named after the application
id is created under the specified target directory.
```bash
$ python discovery.py http://master:8088 --app-id application_1528160315197_0081 --target-directory /etc/prometheus/flink-service-discovery
/etc/prometheus/flink-service-discovery/application_1528160315197_0081.json : [{"targets": ["slave03:9249", "slave01:9249", "slave02:9250", "slave02:9249"]}]
```

Prometheus starts scrapping from the new Flink cluster soon after
application_1528160315197_0081.json is created under
/etc/prometheus/flink-service-discovery/


### Service mode
flink-service-discovery in a service mode checks whether a new job is
submitted or a previously running job is finished every 5 seconds.
You can adjust the polling interval by giving other values to the
--poll-interval argument.
The --name-filter argument is quite necessary in production Hadoop
clusters because you only want to discover Flink clusters out of the
whole list of applications.

In this example, I am going to submit a Flink cluster to YARN and shut
it down after a while to see whether flink-service-discovery detects
changes in the list of applications and accordingly takes actions.
```bash
$ python discovery.py http://master:8088 --target-dir /etc/prometheus/flink-service-discovery
start polling every 5 seconds.
==== Wed Aug  1 16:53:00 2018 ====
# running apps :  3
# added        :  {'application_1528160315197_0082'}
# removed      :  set()
runningContainers(4) != jobmanager(1)+taskmanagers(0)
runningContainers(4) != jobmanager(1)+taskmanagers(2)
/etc/prometheus/flink-service-discovery/application_1528160315197_0082.json : [{"targets": ["slave01:9250", "slave02:9251", "slave03:9250", "slave03:9251"]}]

==== Wed Aug  1 16:55:00 2018 ====
# running apps :  2
# added        :  set()
# removed      :  {'application_1528160315197_0082'}
/etc/prometheus/flink-service-discovery/application_1528160315197_0082.json deleted
```

