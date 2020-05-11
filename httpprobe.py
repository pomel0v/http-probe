#!/usr/bin/python

import argparse
import time
import socket
import csv
import logging
import queue
import threading

from datetime import datetime


def parse_args():
    """ parse arguments and returns dictionary with script parameters"""
    parser = argparse.ArgumentParser(description='httpprobe LR task')
    parser.add_argument(
        '-s', type=str, dest='urls', metavar='URLs', help='URL list for probing separated by semicolons', required=True)
    parser.add_argument(
        '-o', type=str, dest='csv', metavar='file_name.csv', help='output CSV file', required=True)
    parser.add_argument(
        '-t', type=int, dest='iter_sleep', help='time between iterations (msec)', default=5000, required=False)
    parser.add_argument(
        '-n', type=int, dest='iter_count', help='number of iterations', default=3, required=False)
    parser.add_argument(
        '-log', type=str, help='log name', required=True)
    parser.add_argument(
        '-loglevel', type=int, help='increase log verbosity', choices=[1, 2], default=1, required=False)
    args = parser.parse_args()
    return vars(args)


def measure_times(server, timeout):
    """measure tcp+http server response time and return results

    Args:
        server (str): URL for probing
        timeout (int): interval between iterations is used also as timeout for socket

    Function tries to create a TCP socket, establish connection with server, send GET and parse response.
    Response is continuously read from socket with fixed size chunks (4 KB),
    until </html> tag is found or socket.timeout raised.
    """
    try:
        logging.debug("Thread started")

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(timeout)

        tcp_time, http_time, pagesize, is_tcp_success, is_success = 0, 0, 0, False, False
        # some websites reject requests without User-Agent
        http_request = "GET / HTTP/1.1\r\nHost: " + server + \
                       "\r\nUser-Agent: Chrome/50.0.2661.102\r\n\r\n"

        response = b''

        # measure TCP time
        logging.info('Trying connect to %s', server)
        start_time = time.time()
        client.connect((server, 80))
        is_tcp_success = True
        logging.info('Connection to %s successful.', server)
        tcp_time = time.time() - start_time

        # measure HTTP time
        start_time = time.time()
        client.send(str.encode(http_request))
        logging.debug("Reading from socket chunk by chunk")
        while True:
            chunk = client.recv(4096)
            pagesize += len(chunk)
            response += chunk
            if chunk.find(b'</html>') or chunk.find(b'</HTML>') != -1:
                http_time = time.time() - start_time
                is_success = True
                break
        total_time = tcp_time + http_time

        # put probing result in a queue
        q.put([get_transaction_id(), server, http_request.replace('\r\n', '\\r\\n'),
               int(is_tcp_success), round(tcp_time, 3), round(http_time, 3), round(total_time, 3),
               pagesize, int(is_success), response])
        logging.debug("Thread results added to queue")

    except socket.gaierror:
        client.close()
        q.put([None])
        logging.warning("Hostname lookup failed: %s", server)

    except socket.timeout:
        client.close()
        q.put([None])
        logging.warning("Connection with %s timed out, continue within next iteration.", server)

    except OSError as e:
        client.close()
        q.put([None])
        logging.warning("Caught exception while probing %s : %s", server, e)


    except socket.error as e:
        logging.warning('%s exception caught while creating socket: ', server, e)

    finally:
        client.close()
        logging.debug("Thread terminated")


def write_to_csv(csv_name, data, mode='a'):
    """write to csv file in specified mode"""
    with open(csv_name, mode=mode, newline='') as f:
        writer = csv.writer(f, delimiter=';', dialect='excel')
        writer.writerow(data)


def prepare_urls(raw):
    """cut out http:// prefix and return list with servers ready for probing"""
    servers = []
    for r in raw.split('; '):
        pos = r.find('http://')
        if pos != -1:
            servers.append(r[pos+7:])
        else:
            servers.append(r)
    return servers


def spawn_workers(urls):
    threads = [threading.Thread(target=measure_times, args=(u, iter_time)) for u in urls]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

def get_transaction_id():
    """return thread id"""
    return threading.current_thread().getName()[7:]


if __name__ == "__main__":
    params = parse_args()

    # initialize logger
    if params['loglevel'] == 1:
        logging.basicConfig(filename=params['log'], level=logging.INFO,
                            format='%(asctime)s [%(levelname)-7s] %(message)s')
    else:
        logging.basicConfig(filename=params['log'], level=logging.DEBUG,
                            format='%(asctime)s [%(levelname)-7s] [%(threadName)s] %(message)s')
    logging.debug("Script started with parameters: %s", params)

    # add csv header
    out_csv = params['csv']
    csv_header = ['iter_number', 'transaction_id', 'datetime', 'server', 'http_request',
                  'tcp_success', 'tcp_time (msec)', 'http_time (msec)', 'total_time (msec)',
                  'pagesize', 'is_success']
    write_to_csv(out_csv, csv_header, mode='w')

    iter_count = params['iter_count']
    iter_time = params['iter_sleep'] / 1000
    urls = prepare_urls(params['urls'])

    q = queue.Queue()

    for i in range(iter_count):
        iter_start_time = datetime.now().strftime('%Y/%m/%d %H:%M:%S,%f')[:-3]
        spawn_workers(urls)
        print(iter_start_time, 'Iteration', i)
        logging.debug('Iteration %d', i)
        try:
            while True:
                result = q.get(timeout=iter_time)
                if result != [None]:
                    result.insert(0, i)
                    result.insert(2, iter_start_time)
                write_to_csv(out_csv, result)
                print(datetime.now().strftime('%Y/%m/%d %H:%M:%S,%f')[:-3], result)

        except queue.Empty:
            continue

