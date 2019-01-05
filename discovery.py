import argparse
import errno
import json
import os
import re
import requests
import sys
import time
from functools import partial


def flink_cluster_overview(jm_url):
    r = requests.get(jm_url+'/overview')
    if r.status_code != 200:
        return {}
    decoded = r.json()
    return decoded


def flink_jobmanager_prometheus_addr(jm_url):
    addr = None
    port = None

    r = requests.get(jm_url+'/jobmanager/config')
    if r.status_code != 200:
        return ''
    dic = {}
    for obj in r.json():
        dic[obj['key']] = obj['value']
    addr = dic['jobmanager.rpc.address']

    r = requests.get(jm_url + '/jobmanager/log', stream=True)
    if r.status_code != 200:
        return ''
    for line in r.iter_lines(decode_unicode=True):
        if "Started PrometheusReporter HTTP server on port" in line:
            m = re.search('on port (\d+)', line)
            if m:
                port = m.group(1)
                break

    cond1 = addr is not None
    cond2 = port is not None
    if cond1 and cond2:
        return addr+':'+port
    else:
        return ''


def flink_taskmanager_prometheus_addr(tm_id, jm_url, version):
    addr = None
    port = None

    r = requests.get(jm_url+'/taskmanagers/'+tm_id+'/log', stream=True)
    if r.status_code != 200:
        return ''

    for line in r.iter_lines(decode_unicode=True):
        if "hostname/address" in line:
            if version.startswith("1.4"):
                m = re.search("address '([0-9A-Za-z-_]+)' \([\d.]+\)", line)
                if m:
                    hostname = m.group(1)
                    addr = hostname
            else:
                m = re.search("TaskManager: ([0-9A-Za-z-_]+)", line)
                if m:
                    hostname = m.group(1)
                    addr = hostname

        if "Started PrometheusReporter HTTP server on port" in line:
            m = re.search('on port (\d+)', line)
            if m:
                port = m.group(1)

        cond1 = addr is not None
        cond2 = port is not None
        if cond1 and cond2:
            return addr+':'+port

    return ''


def yarn_application_info(app_id, rm_addr):
    r = requests.get(rm_addr + '/ws/v1/cluster/apps/' + app_id)
    if r.status_code != 200:
        return {}

    decoded = r.json()
    return decoded['app'] if 'app' in decoded else {}


def taskmanager_ids(jm_url):
    r = requests.get(jm_url + '/taskmanagers')
    if r.status_code != 200:
        return []

    decoded = r.json()
    if 'taskmanagers' not in decoded:
        return []

    return [tm['id'] for tm in decoded['taskmanagers']]


def prometheus_addresses(app_id, rm_addr):
    prom_addrs = []
    while True:
        app_info = yarn_application_info(app_id, rm_addr)
        if 'trackingUrl' not in app_info:
            time.sleep(1)
            continue

        jm_url = app_info['trackingUrl']
        jm_url = jm_url[:-1] if jm_url.endswith('/') else jm_url

        overview = flink_cluster_overview(jm_url)
        if 'flink-version' not in overview:
            time.sleep(1)
            continue
        version = overview['flink-version']

        if 'taskmanagers' not in overview:
            time.sleep(1)
            continue
        taskmanagers = overview['taskmanagers']

        if app_info['runningContainers'] == 1:
            print(f'runningContainers({app_info["runningContainers"]}) is 1')
            time.sleep(1)
            continue

        if app_info['runningContainers'] != taskmanagers+1:
            print(f'runningContainers({app_info["runningContainers"]}) != jobmanager(1)+taskmanagers({taskmanagers})')
            time.sleep(1)
            continue

        tm_ids = taskmanager_ids(jm_url)
        prom_addrs = map(partial(flink_taskmanager_prometheus_addr, jm_url=jm_url, version=version), tm_ids)
        prom_addrs = list(filter(lambda x: len(x) > 0, prom_addrs))
        if len(tm_ids) != len(prom_addrs):
            print(f'Not all taskmanagers open prometheus endpoints. {len(tm_ids)} of {len(prom_addrs)} opened')
            time.sleep(1)
            continue
        break

    while True:
        jm_prom_addr = flink_jobmanager_prometheus_addr(jm_url)
        if len(jm_prom_addr) == 0:
            time.sleep(1)
            continue
        prom_addrs.append(jm_prom_addr)
        break

    encoded = json.JSONEncoder().encode([{'targets': prom_addrs}])
    return encoded


def create_json_file(rm_addr, target_dir, app_id):
    target_string = prometheus_addresses(app_id, rm_addr)
    if target_dir is not None:
        path = os.path.join(target_dir, app_id + ".json")
        with open(path, 'w') as f:
            print(f'{path} : {target_string}')
            f.write(target_string)
    else:
        print(target_string)    


def delete_json_file(target_dir, app_id):
    if target_dir is not None:
        path = os.path.join(target_dir, app_id + ".json")
        print(f'{path} deleted')
        try:
            os.remove(path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                # re-raise exception if a different error occurred
                raise


def main():
    parser = argparse.ArgumentParser(description='Discover Flink clusters on Hadoop YARN for Prometheus')
    parser.add_argument('rm_addr', type=str,
                        help='(required) Specify yarn.resourcemanager.webapp.address of your YARN cluster.')
    parser.add_argument('--app-id', type=str,
                        help='If specified, this program runs once for the application. '
                             'Otherwise, it runs as a service.')
    parser.add_argument('--name-filter', type=str,
                        help='A regex to specify applications to watch.')
    parser.add_argument('--target-dir', type=str,
                        help='If specified, this program writes the target information to a file on the directory. '
                             'Files are named after the application ids. '
                             'Otherwise, it prints to stdout.')
    parser.add_argument('--poll-interval', type=int, default=5,
                        help='Polling interval to YARN in seconds '
                             'to check applications that are newly added or recently finished. '
                             'Default is 5 seconds.')

    args = parser.parse_args()
    app_id = args.app_id
    name_filter_regex = None if args.name_filter is None else re.compile(args.name_filter)
    rm_addr = args.rm_addr if "://" in args.rm_addr else "http://" + args.rm_addr
    rm_addr = rm_addr[:-1] if rm_addr.endswith('/') else rm_addr
    target_dir = args.target_dir

    if target_dir is not None and not os.path.isdir(target_dir):
        print(f'cannot find {target_dir}')
        sys.exit(1)

    if app_id is not None:
        create_json_file(rm_addr, target_dir, app_id)
    else:
        print(f'start polling every {args.poll_interval} seconds.')
        running_prev = None
        while True:
            running_cur = {}
            added = set()
            removed = set()

            r = requests.get(rm_addr+'/ws/v1/cluster/apps')
            decoded = r.json()
            apps = decoded['apps']['app']
            if name_filter_regex is not None:
                apps = list(filter(lambda app: name_filter_regex.match(app['name']), apps))

            for app in apps:
                # populate [running_cur]
                if app['state'].lower() == 'running':
                    r = requests.get(rm_addr+'/ws/v1/cluster/apps/'+app['id']+'/appattempts')
                    decoded = r.json()
                    attempts = decoded['appAttempts']['appAttempt']
                    # add 'lastAttemptId' to check whether JM is newly launched (when in HA mode)
                    app['lastAttemptId'] = max(map(lambda attempt: int(attempt['id']), attempts))
                    running_cur[app['id']] = app

            if running_prev is not None:
                added = set(running_cur.keys()) - set(running_prev.keys())
                removed = set(running_prev.keys()) - set(running_cur.keys())

                for app_id in running_cur:
                    if app_id in running_prev:
                        last_attempt_cur  = running_cur[app_id]['lastAttemptId']
                        last_attempt_prev = running_prev[app_id]['lastAttemptId']
                        if last_attempt_cur > last_attempt_prev:
                            print(f'a new attempt id detected for {app_id} ({last_attempt_prev}->{last_attempt_cur})')
                            delete_json_file(target_dir, app_id)
                            create_json_file(rm_addr, target_dir, app_id)
                
            if len(added) + len(removed) > 0:
                print(f'==== {time.strftime("%c")} ====')
                print(f'# running apps : {len(running_cur)}')
                print(f'# added        : {added}')
                print(f'# removed      : {removed}')

                for app_id in added:
                    create_json_file(rm_addr, target_dir, app_id)

                for app_id in removed:
                    delete_json_file(target_dir, app_id)

            running_prev = running_cur
            time.sleep(args.poll_interval)


if __name__ == '__main__':
    main()
