#!/usr/bin/python3
"""
Crete datapoints of metrics for anomaly detection from NEMEA IFC.

author: Josef Koumar
e-mail: koumajos@fit.cvut.cz, koumar@cesnet.cz

Copyright (C) 2023 CESNET

LICENSE TERMS

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
    1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
    2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
    3. Neither the name of the Company nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

ALTERNATIVELY, provided that this notice is retained in full, this product may be distributed under the terms of the GNU General Public License (GPL) version 2 or later, in which case the provisions of the GPL apply INSTEAD OF those given above.

This software is provided as is'', and any express or implied warranties, including, but not limited to, the implied warranties of merchantability and fitness for a particular purpose are disclaimed. In no event shall the company or contributors be liable for any direct, indirect, incidental, special, exemplary, or consequential damages (including, but not limited to, procurement of substitute goods or services; loss of use, data, or profits; or business interruption) however caused and on any theory of liability, whether in contract, strict liability, or tort (including negligence or otherwise) arising in any way out of the use of this software, even if advised of the possibility of such damage.
"""
# Standard libraries imports
import os
import sys
import csv
from datetime import datetime
import argparse
from argparse import RawTextHelpFormatter
import yaml

# Third part application imports
import pyasn
import geoip2.database
import geoip2.errors
import ipaddress

# NEMEA system library
import pytrap

# Threads
import logging
import threading
import copy

# The above code is importing the `psycopg2` module, which is a PostgreSQL adapter for Python. It
# allows Python programs to connect to and interact with PostgreSQL databases.
import psycopg2


# The `DataPointIP` class is used to store and update various statistics and averages related to IP
# addresses and their associated attributes.
class DataPointIP:
    def __init__(self, ip: str):
        """
        The above function is a constructor that initializes various attributes for an object.

        :param ip: The `ip` parameter is a string that represents the IP address associated with an
        object
        :type ip: str
        """
        self.ip = ip
        self.ip_version = None
        self.n_flows = 0
        self.n_packets = 0
        self.n_bytes = 0
        self.n_dest_ip_pri = []
        self.n_dest_ip_pub = []
        self.n_dest_asn = 0
        self.n_dest_countries = 0
        self.n_dest_ports = []
        self.tcp_udp_ratio_packets = 0
        self.tcp_udp_ratio_bytes = 0
        self.dir_ratio_packets = 0
        self.dir_ratio_bytes = 0
        self.avg_duration = 0
        self.avg_ttl = 0

    def update(
        self,
        _ip: str,
        _port: int,
        packets: int,
        packets_rev: int,
        bytes: int,
        bytes_rev: int,
        protocol: int,
        time_first: float,
        time_last: float,
        ttl: int,
    ):
        """
        The function updates various statistics based on the input parameters.

        :param _ip: The `_ip` parameter represents the IP address of the network flow
        :type _ip: str
        :param _port: The `_port` parameter represents the port number of the network connection
        :type _port: int
        :param packets: The parameter "packets" represents the number of packets sent in a network flow
        :type packets: int
        :param packets_rev: The parameter "packets_rev" represents the number of packets received in the
        reverse direction
        :type packets_rev: int
        :param bytes: The parameter "bytes" represents the number of bytes transmitted in a network flow
        :type bytes: int
        :param bytes_rev: The parameter "bytes_rev" represents the number of bytes received in the
        network traffic
        :type bytes_rev: int
        :param protocol: The "protocol" parameter represents the protocol number of the network packet.
        In this code snippet, it is used to calculate the TCP/UDP ratio of packets and bytes. If the
        protocol is 6 (TCP), the packets and bytes are added to the TCP/UDP ratio counters
        :type protocol: int
        :param time_first: The parameter "time_first" represents the timestamp of the first packet in
        the flow
        :type time_first: float
        :param time_last: The parameter "time_last" represents the timestamp of the last packet in the
        flow
        :type time_last: float
        :param ttl: The "ttl" parameter stands for Time to Live. It is a field in the IP header of a
        packet that specifies the maximum number of hops (routers) that the packet can pass through
        before being discarded
        :type ttl: int
        """
        # Simple Numbers
        self.n_flows += 1
        self.n_packets += packets + packets_rev
        self.n_bytes += bytes + bytes_rev
        # Destination IP adreses statistics
        ip_addr = ipaddress.ip_address(_ip)
        if ip_addr.is_private:
            if _ip not in self.n_dest_ip_pri:
                self.n_dest_ip_pri.append(_ip)
        else:
            if _ip not in self.n_dest_ip_pub:
                self.n_dest_ip_pub.append(_ip)
        if _ip not in self.n_dest_ip_pub:
            self.n_dest_ip_pub.append(_ip)

        if _port not in self.n_dest_ports:
            self.n_dest_ports.append(_port)
        # Ratios
        if protocol == 6:
            self.tcp_udp_ratio_packets += packets + packets_rev
            self.tcp_udp_ratio_bytes += bytes + bytes_rev
        self.dir_ratio_packets += packets
        self.dir_ratio_bytes += bytes
        # Averages
        self.avg_duration += time_last - time_first
        self.avg_ttl += ttl

    def pre_export(self, asndb: pyasn.pyasn, geoip2_reader: geoip2.database.Reader):
        """
        The function calculates various statistics and ratios related to destination IP addresses and
        ports, and calculates averages for duration and TTL.

        :param asndb: The `asndb` parameter is an instance of the `pyasn.pyasn` class, which is used for
        looking up the Autonomous System Number (ASN) for a given IP address
        :type asndb: pyasn.pyasn
        :param geoip2_reader: The `geoip2_reader` parameter is an instance of the
        `geoip2.database.Reader` class. It is used for performing IP geolocation lookups based on the
        MaxMind GeoIP2 database
        :type geoip2_reader: geoip2.database.Reader
        :return: The function does not explicitly return anything.
        """
        if self.n_flows == 0:
            self.n_dest_ip = 0
            self.n_dest_ports = 0
            return
        # Ratios
        self.tcp_udp_ratio_packets /= self.n_packets
        self.tcp_udp_ratio_bytes /= self.n_bytes
        self.dir_ratio_packets /= self.n_packets
        self.dir_ratio_bytes /= self.n_bytes
        # Filter non active IP address
        if self.dir_ratio_packets == 0:
            self.n_flows = 0
            return
        # Destination IP adreses statistics
        _asns = []
        try:
            [_asns.append(asndb.lookup(_ip)[0]) for _ip in self.n_dest_ip_pub]
        except:
            pass
        _asns = set(_asns)
        if None in _asns:
            self.n_dest_asn = len(_asns) - 1
        else:
            self.n_dest_asn = len(_asns)
        # for enable number of destination countries comment the next row and uncomment the following section
        self.n_dest_countries = -1
        # _countries = []
        # try:
        #     [
        #         _countries.append(geoip2_reader.country(_ip).country.iso_code)
        #         for _ip in self.n_dest_ip_pub
        #     ]
        # except:
        #     pass
        # _countries = set(_countries)
        # if None in _countries:
        #     self.n_dest_countries = len(_countries) - 1
        # else:
        #     self.n_dest_countries = len(_countries)
        self.n_dest_ip_pub = len(self.n_dest_ip_pub)
        self.n_dest_ip_pri = len(self.n_dest_ip_pri)
        self.n_dest_ports = len(self.n_dest_ports)
        # Averages
        self.avg_duration /= self.n_flows
        self.avg_ttl /= self.n_flows


def get_networks(networks_file: str):
    """
    The function `get_networks` reads a file containing network addresses and returns a list of IP
    networks.

    :param networks_file: The `networks_file` parameter is a string that represents the file path of the
    file containing the list of networks
    :type networks_file: str
    :return: a list of IP networks.
    """
    networks = []
    with open(networks_file, "r") as f:
        reader = csv.reader(f, delimiter="\n")
        for row in reader:
            networks.append(ipaddress.ip_network(row[0]))
    return networks


def check_ip(ip: ipaddress.IPv4Address, networks: list):
    """
    The function checks if an IPv4 address is within any of the given networks.

    :param ip: The `ip` parameter is of type `ipaddress.IPv4Address` and represents an IPv4 address. It
    is the IP address that needs to be checked against the list of networks
    :type ip: ipaddress.IPv4Address
    :param networks: The `networks` parameter is a list of IP networks. Each network is represented as
    an `ipaddress.IPv4Network` object
    :type networks: list
    :return: a boolean value. It returns True if the given IP address is found in any of the networks in
    the list, and False otherwise.
    """
    for net in networks:
        if ip in net:
            return True
    return False


def aggregate_interval(
    IPs: dict, asndb: pyasn.pyasn, geoip2_reader: geoip2.database.Reader
):
    """
    The function `aggregate_interval` takes in a dictionary of IP addresses, a pyasn.pyasn object, and a
    geoip2.database.Reader object, and calls the `pre_export` method on each IP address in the
    dictionary.

    :param IPs: The `IPs` parameter is a dictionary that contains IP addresses as keys and objects as
    values. Each object represents some information related to the IP address
    :type IPs: dict
    :param asndb: The `asndb` parameter is an instance of the `pyasn.pyasn` class. This class is used
    for performing IP address to Autonomous System (AS) number lookups. It provides methods to retrieve
    the AS number and related information for a given IP address
    :type asndb: pyasn.pyasn
    :param geoip2_reader: The `geoip2_reader` parameter is an instance of the `geoip2.database.Reader`
    class. It is used for reading and querying the GeoIP2 database, which contains information about the
    geographical location of IP addresses
    :type geoip2_reader: geoip2.database.Reader
    """
    for ip in IPs.keys():
        IPs[ip].pre_export(asndb, geoip2_reader)


def send_ts(i: int, IPs: dict, db_conf: dict, start_time: float):
    """
    The function `send_ts` is responsible for connecting to a database, loading IP address codes,
    creating SQL queries, and pushing new IP addresses and ad metrics into the database.

    :param i: The parameter `i` is an integer representing the thread number. It is used for logging
    purposes to identify which thread is executing the code
    :type i: int
    :param IPs: IPs is a dictionary that contains IP addresses as keys and their corresponding metrics
    as values. The metrics include n_flows, n_packets, n_bytes, n_dest_ip_pri, n_dest_ip_pub,
    n_dest_asn, n_dest_countries, n_dest_ports, tcp_udp_ratio_packets, tcp
    :type IPs: dict
    :param db_conf: The `db_conf` parameter is a dictionary that contains the configuration details for
    connecting to the database. It includes the following keys:
    :type db_conf: dict
    :param start_time: The `start_time` parameter is a float value representing the start time of the
    process. It is used to generate a timestamp for the SQL queries
    :type start_time: float
    """
    if IPs != {}:
        logging.info(f"   Thread {i}  : connect to DB")
        logging.info("  Load IP address codes")
        ip_codes, next_ip_code = get_ips(db_conf)
        logging.info(
            f"   Thread {i}  : Start creating SQL queries ({next_ip_code} - {len(IPs)})"
        )
        tmp = f"INSERT INTO ip_address (id_ip, ip_address, note) VALUES"
        tmp_data = f"INSERT INTO ad_metrics (time, id_ip, n_flows, n_packets, n_bytes, n_dest_ip_pri, n_dest_ip_pub, n_dest_asn, n_dest_countries, n_dest_ports, tcp_udp_ratio_packets, tcp_udp_ratio_bytes, dir_ratio_packets, dir_ratio_bytes, avg_duration, avg_ttl) VALUES"
        data = []
        _t = datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
        for ip in IPs:
            if IPs[ip].n_flows == 0:
                continue
            if ip not in ip_codes:
                ip_codes[ip] = next_ip_code
                tmp += f"({next_ip_code}, '{ip}', ''),"
                next_ip_code += 1
            tmp_data += (
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),"
            )
            data += [
                _t,
                ip_codes[ip],
                IPs[ip].n_flows,
                IPs[ip].n_packets,
                IPs[ip].n_bytes,
                IPs[ip].n_dest_ip_pri,
                IPs[ip].n_dest_ip_pub,
                IPs[ip].n_dest_asn,
                IPs[ip].n_dest_countries,
                IPs[ip].n_dest_ports,
                IPs[ip].tcp_udp_ratio_packets,
                IPs[ip].tcp_udp_ratio_bytes,
                IPs[ip].dir_ratio_packets,
                IPs[ip].dir_ratio_bytes,
                IPs[ip].avg_duration,
                IPs[ip].avg_ttl,
            ]
        logging.info(
            f"   Thread {i}  : End creating SQL queries ({next_ip_code} - {len(tmp)} - {len(tmp_data)} - {data[1]})"
        )
        if tmp[-1] == ",":
            logging.info(f"   Thread {i}  : Start pushing new IP addresses into DB")
            conn = psycopg2.connect(
                f"host='{db_conf['host']}' \
                dbname='{db_conf['dbname']}' \
                user='{db_conf['user']}' \
                password='{db_conf['password']}' \
                sslmode='{db_conf['sslmode']}' \
                sslrootcert={db_conf['sslrootcert']} \
                sslcert={db_conf['sslcert']} \
                sslkey={db_conf['sslkey']} \
                port='{db_conf['port']}'"
            )
            cursor = conn.cursor()
            cursor.execute(tmp[:-1] + ";")
            conn.commit()
            cursor.close()
            conn.close()
            logging.info(f"   Thread {i}  : End pushing new IP addresses into DB")
        logging.info(f"   Thread {i}  : ------")
        if tmp_data[-1] == ",":
            tmp_data = tmp_data[:-1] + ";"
            logging.info(f"   Thread {i}  : start pushing ad_metrics SQL quieries")
            conn = psycopg2.connect(
                f"host='{db_conf['host']}' \
                dbname='{db_conf['dbname']}' \
                user='{db_conf['user']}' \
                password='{db_conf['password']}' \
                sslmode='{db_conf['sslmode']}' \
                sslrootcert={db_conf['sslrootcert']} \
                sslcert={db_conf['sslcert']} \
                sslkey={db_conf['sslkey']} \
                port='{db_conf['port']}'"
            )
            cursor = conn.cursor()
            cursor.execute(tmp_data, tuple(data))
            conn.commit()
            cursor.close()
            conn.close()
            logging.info(f"   Thread {i}  : end pushing ad_metrics SQL quieries")


def paralel_func(
    i: int,
    IPs: dict,
    db_conf: dict,
    start_time: float,
    asndb: pyasn.pyasn,
    geoip2_reader: geoip2.database.Reader,
):
    """
    The function `paralel_func` performs aggregation on a dictionary of IP addresses, using the provided
    ASNDb and GeoIP2 reader, and then sends the aggregated data to a database.

    :param i: The parameter `i` is an integer representing the thread number or identifier. It is used
    to differentiate between different threads when logging or performing other operations
    :type i: int
    :param IPs: The `IPs` parameter is a dictionary that contains IP addresses as keys and their
    corresponding data as values
    :type IPs: dict
    :param db_conf: The `db_conf` parameter is a dictionary that contains configuration information for
    the database. It likely includes details such as the database host, port, username, password, and
    database name
    :type db_conf: dict
    :param start_time: The `start_time` parameter is a float value representing the start time of the
    process. It is used to calculate the total execution time of the process
    :type start_time: float
    :param asndb: The `asndb` parameter is an instance of the `pyasn.pyasn` class. It is used for
    performing IP to ASN (Autonomous System Number) lookups
    :type asndb: pyasn.pyasn
    :param geoip2_reader: The parameter `geoip2_reader` is of type `geoip2.database.Reader`. It is
    likely an instance of the `Reader` class from the `geoip2` library, which is used for reading
    MaxMind GeoIP2 databases. This parameter is used in the `paralel
    :type geoip2_reader: geoip2.database.Reader
    """
    logging.info(f"   Thread {i}  : start process of finishing aggregation ")
    aggregate_interval(IPs, asndb, geoip2_reader)
    logging.info(f"   Thread {i}  : end process of finishing aggregation ")
    send_ts(
        i,
        IPs,
        db_conf,
        start_time,
    )


def get_ips(db_conf: dict):
    """
    The function `get_ips` retrieves IP addresses and their corresponding IDs from a PostgreSQL database
    and returns a dictionary of IP addresses and their IDs, as well as the next available ID.

    :param db_conf: The `db_conf` parameter is a dictionary that contains the configuration details for
    connecting to a PostgreSQL database. It should have the following keys:
    :type db_conf: dict
    :return: The function `get_ips` returns a tuple containing two values. The first value is a
    dictionary `ip_codes` which maps IP addresses to their corresponding ID_ip values. The second value
    is the maximum ID_ip value in the `ip_codes` dictionary incremented by 1.
    """
    conn = psycopg2.connect(
        f"host='{db_conf['host']}' \
        dbname='{db_conf['dbname']}' \
        user='{db_conf['user']}' \
        password='{db_conf['password']}' \
        sslmode='{db_conf['sslmode']}' \
        sslrootcert={db_conf['sslrootcert']} \
        sslcert={db_conf['sslcert']} \
        sslkey={db_conf['sslkey']} \
        port='{db_conf['port']}'"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT ID_ip, ip_address FROM ip_address;")
    table = cursor.fetchall()
    ip_codes = {}
    for row in table:
        ip_codes[row[1]] = row[0]
    conn.commit()
    cursor.close()
    conn.close()
    return ip_codes, max(ip_codes.values()) + 1


def load_pytrap(argv: list):
    """
    The function `load_pytrap` initializes a `TrapCtx` object and sets the required fields for received
    messages. Init nemea libraries and set format of IP flows.

    :param argv: The `argv` parameter is a list of command-line arguments that are passed to the
    `load_pytrap` function. It is used to initialize the `TrapCtx` object and configure its settings
    :type argv: list
    :return: The function `load_pytrap` returns two values: `rec` and `trap`.
    """
    trap = pytrap.TrapCtx()
    trap.init(argv, 1, 0)  # argv, ifcin - 1 input IFC, ifcout - 1 output IFC
    # Set the list of required fields in received messages.
    # This list is an output of e.g. flow_meter - basic flow.
    inputspec = "ipaddr DST_IP,ipaddr SRC_IP,time TIME_FIRST,time TIME_LAST,uint32 PACKETS,uint32 PACKETS_REV,uint64 BYTES,uint64 BYTES_REV,uint16 DST_PORT,uint16 SRC_PORT,uint8 PROTOCOL"
    trap.setRequiredFmt(0, pytrap.FMT_UNIREC, inputspec)
    rec = pytrap.UnirecTemplate(inputspec)
    return rec, trap


def parse_arguments():
    """
    The `parse_arguments` function is used to parse command line arguments and return the parsed
    arguments.
    :return: The function `parse_arguments` returns the parsed command-line arguments as an
    `argparse.Namespace` object.
    """
    parser = argparse.ArgumentParser(
        description="""

    Usage:""",
        formatter_class=RawTextHelpFormatter,
    )

    parser.add_argument(
        "-i",
        help='Specification of interface types and their parameters, see "-h trap" (mandatory parameter).',
        type=str,
        metavar="IFC_SPEC",
    )

    parser.add_argument("-v", help="Be verbose.", action="store_true")

    parser.add_argument("-vv", help="Be more verbose.", action="store_true")

    parser.add_argument("-vvv", help="Be even more verbose.", action="store_true")

    parser.add_argument(
        "--database_conf",
        help="Postgres TimescaleDB database connection configuration YAML file.",
        type=str,
        metavar="STR",
        default="database_conf.yaml",
    )
    parser.add_argument(
        "--networks_file",
        help="Path to allowed networks file",
        type=str,
        metavar="PATH/FILENAME",
        default=None,
    )
    parser.add_argument(
        "--asn_file",
        help="Path to pyasn database file",
        type=str,
        metavar="PATH/FILENAME",
        default=None,
    )
    parser.add_argument(
        "--geoip2_database",
        help="Path to GeoIP2 database",
        type=str,
        metavar="PATH/FILENAME",
        default=None,
    )
    parser.add_argument(
        "--Aggregation",
        help="Windows size in seconds.",
        type=int,
        metavar="SECONDS",
        default=600,
    )
    parser.add_argument(
        "--logs",
        help="Log file for Python logs.",
        type=str,
        metavar="PATH",
        default="/tmp/logs_create_adp",
    )
    arg = parser.parse_args()
    if arg.networks_file is None:
        print("Set --networks_file parameter!")
        sys.exit(1)
    if arg.asn_file is None:
        print("Set --asn_file parameter!")
        sys.exit(1)
    if arg.geoip2_database is None:
        print("Set --geoip2_database parameter!")
        sys.exit(1)
    return arg


def main():
    """
    The main function collects data points for anomaly detection from IP flows and processes them in
    parallel.
    """
    arg = parse_arguments()
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(
        format=format,
        level=logging.INFO,
        datefmt="%H:%M:%S",
        filename=arg.logs,
        filemode="a",
    )
    logging.info("START OF COLLECTIONS OF DATAPOINTS FOR ANOMALY DETECTION")
    logging.info("  Load pytrap")
    rec, trap = load_pytrap(sys.argv)
    logging.info("  Load networks")
    networks = get_networks(arg.networks_file)
    logging.info("  Load pyasn")
    asndb = pyasn.pyasn(arg.asn_file)
    logging.info("  Load GeoIP2 database")
    geoip2_reader = geoip2.database.Reader(arg.geoip2_database)
    logging.info("  Load DB connection configuration")
    db_conf = yaml.load(open(arg.database_conf, "r"))
    logging.info("  Start measurement")
    IPs = {}
    start_time = None
    i = 0
    try:
        while True:  # main loop for load ip-flows from interfaces
            try:  # load IP flow from IFC interface
                data = trap.recv()
            except pytrap.FormatChanged as e:
                fmttype, inputspec = trap.getDataFmt(0)
                rec = pytrap.UnirecTemplate(inputspec)
                data = e.data
                biflow = None
            if len(data) <= 1:
                break
            rec.setData(data)  # set the IP flow to created tempalte
            if start_time is None:
                start_time = int(float(rec.TIME_LAST))
            # proces flow to add to time series
            SRC_IP = str(rec.SRC_IP)
            DST_IP = str(rec.DST_IP)
            src_ip = ipaddress.ip_address(SRC_IP)
            dst_ip = ipaddress.ip_address(DST_IP)
            if (
                check_ip(src_ip, networks) is True
                and check_ip(dst_ip, networks) is True
            ):
                if SRC_IP not in IPs:
                    IPs[SRC_IP] = DataPointIP(SRC_IP)
                IPs[SRC_IP].update(
                    DST_IP,
                    rec.DST_PORT,
                    rec.PACKETS,
                    rec.PACKETS_REV,
                    rec.BYTES,
                    rec.BYTES_REV,
                    rec.PROTOCOL,
                    float(rec.TIME_FIRST),
                    float(rec.TIME_LAST),
                    rec.TTL,
                )
                if DST_IP not in IPs:
                    IPs[DST_IP] = DataPointIP(DST_IP)
                IPs[DST_IP].update(
                    SRC_IP,
                    rec.SRC_PORT,
                    rec.PACKETS_REV,
                    rec.PACKETS,
                    rec.BYTES_REV,
                    rec.BYTES,
                    rec.PROTOCOL,
                    float(rec.TIME_FIRST),
                    float(rec.TIME_LAST),
                    rec.TTL,
                )
            elif check_ip(src_ip, networks) is True:
                if SRC_IP not in IPs:
                    IPs[SRC_IP] = DataPointIP(SRC_IP)
                IPs[SRC_IP].update(
                    DST_IP,
                    rec.DST_PORT,
                    rec.PACKETS,
                    rec.PACKETS_REV,
                    rec.BYTES,
                    rec.BYTES_REV,
                    rec.PROTOCOL,
                    float(rec.TIME_FIRST),
                    float(rec.TIME_LAST),
                    rec.TTL,
                )
            elif check_ip(dst_ip, networks) is True:
                if DST_IP not in IPs:
                    IPs[DST_IP] = DataPointIP(DST_IP)
                IPs[DST_IP].update(
                    SRC_IP,
                    rec.SRC_PORT,
                    rec.PACKETS_REV,
                    rec.PACKETS,
                    rec.BYTES_REV,
                    rec.BYTES,
                    rec.PROTOCOL,
                    float(rec.TIME_FIRST),
                    float(rec.TIME_LAST),
                    rec.TTL,
                )
            else:
                continue
            if (start_time + arg.Aggregation) < float(rec.TIME_LAST):
                logging.info("Main    : before creating thread")
                threading.Thread(
                    target=paralel_func,
                    args=(
                        i,
                        copy.deepcopy(IPs),
                        db_conf,
                        start_time,
                        asndb,
                        geoip2_reader,
                    ),
                ).start()
                IPs.clear()
                start_time += arg.Aggregation
                i += 1
                logging.info("Main    : continue collecting data")

    except KeyboardInterrupt:
        logging.info(f"End creating time series")

    trap.finalize()  # Free allocated TRAP IFCs


if __name__ == "__main__":
    main()
