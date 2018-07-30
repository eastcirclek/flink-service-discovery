import argparse
import errno
import json
import os
import re
import requests
import sys
import time
from collections import namedtuple
from functools import partial


def flink_jobmanager_prometheus_addr(jm_url):
    addr = None
    port = None

    r = requests.get(jm_url+'/jobmanager/log', stream=True)
    if r.status_code != 200:
        return ''

    for line in r.iter_lines(decode_unicode=True):
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


def flink_taskmanager_prometheus_addr(tm_id, jm_url):
    addr = None
    port = None

    r = requests.get(jm_url+'/taskmanagers/'+tm_id+'/log', stream=True)
    if r.status_code != 200:
        return ''

    for line in r.iter_lines(decode_unicode=True):
        if "TaskManager will use hostname/address" in line:
            m = re.search("address '([0-9A-Za-z-_]+)' \(([\d.]+)\)", line)
            if m:
                hostname = m.group(1)
                # ipaddr = m.group(2)
                addr = hostname

        if "Started PrometheusReporter HTTP server on port" in line:
            m = re.search('on port (\d+)', line)
            if m:
                port = m.group(1)

        if addr is not None and port is not None:
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
    while True:
        app_info = yarn_application_info(app_id, rm_addr)
        if ('runningContainers' not in app_info) or ('trackingUrl' not in app_info):
            time.sleep(1)
            continue

        if app_info['runningContainers'] == 1:
            print("runningContainers(%d) is still 1" % (app_info['runningContainers'],))
            time.sleep(1)
            continue

        tm_ids = taskmanager_ids(app_info['trackingUrl'])
        if app_info['runningContainers'] != len(tm_ids)+1:
            print("runningContainers(%d) != taskmanagers(%d)+1" % (app_info['runningContainers'], len(tm_ids)))
            time.sleep(1)
            continue

        prom_addrs = map(partial(flink_taskmanager_prometheus_addr, jm_url=app_info['trackingUrl']), tm_ids)
        prom_addrs = list(filter(lambda x: len(x) > 0, prom_addrs))
        if len(tm_ids) != len(prom_addrs):
            print("Not all taskmanagers open prometheus endpoints. %d of %d opened" % (len(tm_ids), len(prom_addrs)))
            time.sleep(1)
            continue
        break

    while True:
        jm_prom_addr = flink_jobmanager_prometheus_addr(app_info['trackingUrl'])
        if len(jm_prom_addr) == 0:
            time.sleep(1)
            continue

        prom_addrs.append(jm_prom_addr)
        break

    encoded = json.JSONEncoder().encode([{'targets': prom_addrs}])
    return encoded


def main():
    parser = argparse.ArgumentParser(description='Discovery for Flink per-job clusters on Hadoop YARN for Prometheus')
    parser.add_argument('rm_addr', type=str,
                        help='YARN resource manager address')
    parser.add_argument('--app-name', type=str,
                        help='')
    parser.add_argument('--app-id', type=str,
                        help='If specified, this program runs once for the application id')
    parser.add_argument('--target-dir', type=str,
                        help='If specified, this program writes target information into the directory')
    parser.add_argument('--poll-interval', type=int, default=5,
                        help='Polling interval to YARN')

    args = parser.parse_args()
    args.rm_addr = args.rm_addr if "://" in args.rm_addr else "http://" + args.rm_addr

    app_id = args.app_id
    rm_addr = args.rm_addr
    target_dir = args.target_dir

    if target_dir is not None and not os.path.isdir(target_dir):
        print('cannot find', target_dir)
        sys.exit(1)

    if app_id is not None:
        target_string = prometheus_addresses(app_id, rm_addr)
        if target_dir is not None:
            path = os.path.join(target_dir, app_id+".json")
            with open(path, 'w') as f:
                print(path, " : ", target_string)
                f.write(target_string)
        else:
            print(target_string)
    else:
        print("start polling every " + str(args.poll_interval) + " seconds.")
        running_prev = None
        while True:
            running_cur = {}
            added = set()
            removed = set()
            
            r = requests.get(rm_addr+'/ws/v1/cluster/apps')
            if r.status_code != 200:
                print("Failed to connect to the server")
                print("The status code is " + r.status_code)
                break

            decoded = r.json()
            for app in decoded['apps']['app']:
                print(app)
                app = namedtuple("App", app.keys())(*app.values())
                if app.state.lower() == 'running':
                    running_cur[app.id] = app

            if running_prev is not None:
                added = set(running_cur.keys()) - set(running_prev.keys())
                removed = set(running_prev.keys()) - set(running_cur.keys())

            if len(added) + len(removed) > 0:
                print('====', time.strftime("%c"), '====')
                print('# running apps : ', len(running_cur))
                print('# added        : ', added)
                print('# removed      : ', removed)

                for app_id in added:
                    target_string = prometheus_addresses(app_id, rm_addr)
                    if target_dir is not None:
                        path = os.path.join(target_dir, app_id + ".json")
                        with open(path, 'w') as f:
                            print(path, " : ", target_string)
                            f.write(target_string)
                    else:
                        print(target_string)

                for app_id in removed:
                    if target_dir is not None:
                        path = os.path.join(target_dir, app_id + ".json")
                        print(path + " deleted")
                        try:
                            os.remove(path)
                        except OSError as e:
                            if e.errno != errno.ENOENT:
                                # re-raise exception if a different error occurred
                                raise

            running_prev = running_cur
            time.sleep(args.poll_interval)


if __name__ == '__main__':
    main()
