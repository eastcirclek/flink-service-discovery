import argparse, errno, json, os, re, sys, time
import urllib.request
from collections import namedtuple
from functools import partial


def yarn_application_info(app_id, rm_addr):
    url = rm_addr + '/ws/v1/cluster/apps/' + app_id
    with urllib.request.urlopen(url) as response:
        if response.getcode() == 200:
            text = response.read().decode('utf8')
            decoded = json.loads(text)
            return decoded['app']


def flink_taskmanagers(jobmanager_url):
    url = jobmanager_url + '/taskmanagers'
    with urllib.request.urlopen(url) as response:
        if response.getcode() == 200:
            text = response.read().decode('utf8')
            decoded = json.loads(text)
            return decoded['taskmanagers']

    return []


def flink_jobmanager_prometheus_addr(jobmanager_url):
    addr = None
    port = None

    url = jobmanager_url + '/jobmanager/log'
    with urllib.request.urlopen(url) as response:
        if response.getcode() == 200:
            lines = response.read().decode('utf8').splitlines()
            for line in lines:
                if "YARN assigned hostname for application master" in line:
                    m = re.search("application master: ([0-9A-Za-z-_]+)", line)
                    if m:
                        addr = m.group(1)
                
                if "Started PrometheusReporter HTTP server on port" in line:
                    m = re.search('on port (\d+)', line)
                    if m:
                        port = m.group(1)

                if addr is not None and port is not None:
                    return addr+':'+port
            
    return ''


def flink_taskmanager_prometheus_addr(taskmanager_id, jobmanager_url):
    addr = None
    port = None

    url = jobmanager_url + '/taskmanagers/' + taskmanager_id + '/log'
    with urllib.request.urlopen(url) as response:
        if response.getcode() == 200:
            lines=response.read().decode('utf8').splitlines()
            for line in lines:
                if "TaskManager will use hostname/address" in line:
                    m = re.search("address '([0-9A-Za-z-_]+)' \(([\d\.]+)\)", line)
                    if m:
                        hostname = m.group(1)
                        ipaddr = m.group(2)
                        addr = hostname

                if "Started PrometheusReporter HTTP server on port" in line:
                    m = re.search('on port (\d+)', line)
                    if m:
                        port = m.group(1)

                if addr is not None and port is not None:
                    return addr+':'+port

    return ''


def prometheus_addresses(app_id, rm_addr):
    while True:
        app_info = yarn_application_info(app_id, rm_addr)
        if app_info['runningContainers'] == 1:
            print("runningContainers(%d) is still 1" % (app_info['runningContainers'],))
            time.sleep(1)
            continue

        jobmanager_url = app_info['trackingUrl']
        taskmanagers = flink_taskmanagers(jobmanager_url)
        if app_info['runningContainers'] != len(taskmanagers)+1:
            print("runningContainers(%d) != taskmanagers(%d)+1" % (app_info['runningContainers'], len(taskmanagers)))
            time.sleep(1)
            continue

        taskmanager_ids = [container['id'] for container in taskmanagers]
        prometheus_addresses = map(partial(flink_taskmanager_prometheus_addr, jobmanager_url=jobmanager_url), taskmanager_ids)
        prometheus_addresses = list(filter(lambda x: len(x)>0, prometheus_addresses))
        if len(taskmanagers) != len(prometheus_addresses):
            print("taskmanagers(%d) != addresses(%d)" % (len(taskmanagers), len(prometheus_addresses)))
            time.sleep(1)
            continue

        break

    while True:
        jobmanager_prometheus_addr = flink_jobmanager_prometheus_addr(jobmanager_url)
        if len(jobmanager_prometheus_addr) == 0:
            time.sleep(1)
            continue
        prometheus_addresses.append(jobmanager_prometheus_addr)
        break

    encoded = json.JSONEncoder().encode([{'targets': prometheus_addresses}])

    return encoded


def main():
    parser = argparse.ArgumentParser(description='Discovery for Flink per-job clusters on Hadoop YARN for Prometheus')
    parser.add_argument('rm_addr', type=str,
                        help='YARN resource manager address')
    parser.add_argument('--app-id', type=str,
                        help='If specified, this program runs once for the application id')
    parser.add_argument('--target-dir', type=str,
                        help='If specified, this program writes target information into the directory')
    parser.add_argument('--poll-interval', type=int, default=5,
                        help='How often to check added or removed applications from YARN')

    args = parser.parse_args()
    args.rm_addr = args.rm_addr if "://" in args.rm_addr else "http://" + args.rm_addr

    app_id = args.app_id
    rm_addr = args.rm_addr
    target_dir = args.target_dir

    if target_dir is not None and not os.path.isdir(target_dir):
        print('cannot find', target_dir)
        sys.exit()

    if app_id is not None:
        target_string = prometheus_addresses(app_id, rm_addr)
        if target_dir is not None:
            path = os.path.join(target_dir, app_id+".json")
            with open(path, 'w') as f:
                print("create a new file :", path)
                f.write(target_string)
        else:
            print("- target string :", target_string)
    else:
        print("Let's start poll every " + str(args.poll_interval) + " seconds.")
        running_prev = None
        while True:
            running_cur = {}
            added = set()
            removed = set()
            
            url = rm_addr+'/ws/v1/cluster/apps'
            with urllib.request.urlopen(url) as response:
                if response.getcode() == 200:
                    text = response.read().decode('utf8')
                    decoded = json.loads(text) # a dictionary of dictionaries
                    for app in decoded['apps']['app']:
                        app = namedtuple("App", app.keys())(*app.values())
                        if app.state.lower() == 'running':
                            running_cur[app.id] = app

                    if running_prev is not None:
                        added = set(running_cur.keys()) - set(running_prev.keys())
                        removed = set(running_prev.keys()) - set(running_cur.keys())

                    if len(added) + len(removed) > 0:
                        print('====',time.strftime("%c"),'====')
                        print('# running apps : ', len(running_cur))
                        print('# added        : ', added)
                        print('# removed      : ', removed)

                        for app_id in added:
                            target_string = prometheus_addresses(app_id, rm_addr)
                            if target_dir is not None:
                                path = os.path.join(target_dir, app_id+".json")
                                with open(path, 'w') as f:
                                    print("- create a new file ", path, "with", target_string)
                                    f.write(target_string)
                            else:
                                print("- target string :", target_string)

                        for app_id in removed:
                            if target_dir is not None:
                                path = os.path.join(target_dir, app_id+".json")
                                print("- remove an exiting file :", path)
                                try:
                                    os.remove(path)
                                except OSError as e:
                                    if e.errno != errno.ENOENT:
                                        # re-raise exception if a different error occurred
                                        raise

                    running_prev = running_cur
                else:
                    print('status code :', response.getcode())

                time.sleep(args.poll_interval)


if __name__ == '__main__':
    main()
