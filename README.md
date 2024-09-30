# Create datapoints modules for the dataset CESNET-TimeSeries24

Cite as:

Josef Koumar, Karel Hynek, Tomáš Čejka, Pavel Šiška, "CESNET-TimeSeries24: Time Series Dataset for Network Traffic Anomaly Detection and Forecasting", arXiv e-prints (2024): [https://arxiv.org/abs/2409.18874](https://arxiv.org/abs/2409.18874)

```
@misc{koumar2024cesnettimeseries24timeseriesdataset,
      title={CESNET-TimeSeries24: Time Series Dataset for Network Traffic Anomaly Detection and Forecasting}, 
      author={Josef Koumar and Karel Hynek and Tomáš Čejka and Pavel Šiška},
      year={2024},
      eprint={2409.18874},
      archivePrefix={arXiv},
      primaryClass={cs.LG},
      url={https://arxiv.org/abs/2409.18874}, 
}
```

Josef Koumar, Karel Hynek, Pavel Šiška & Tomáš Čejka. (2024). CESNET-TimeSeries24: Time Series Dataset for Network Traffic Anomaly Detection and Forecasting [Data set]. Zenodo. <https://doi.org/10.5281/zenodo.13382427>

```
@dataset{koumar_2024_13382427,
  author       = {Koumar, Josef and
                  Hynek, Karel and
                  Čejka, Tomáš and
                  Šiška, Pavel},
  title        = {{CESNET-TimeSeries24: Time Series Dataset for 
                   Network Traffic Anomaly Detection and Forecasting}},
  month        = aug,
  year         = 2024,
  publisher    = {Zenodo},
  doi          = {10.5281/zenodo.13382427},
  url          = {https://doi.org/10.5281/zenodo.13382427}
}
```

# Installation prerequisities

### [Install NEMEA](https://github.com/CESNET/Nemea)

### [Install NEMEA Framework](https://github.com/CESNET/Nemea-Framework)

### [Install NEMEA Supervisor](https://github.com/CESNET/Nemea-Supervisor)

### [Setup data collection](https://nemea.liberouter.org/doc/tutorial/)

### Install Python

Install Python on your system

### Install pip dependencies

[psycopg2](https://pypi.org/project/psycopg2/)

[pyasn](https://pypi.org/project/pyasn/)

[geoip2](https://pypi.org/project/geoip2/)

# [Install TimescaleDB on Linux](https://docs.timescale.com/self-hosted/latest/install/installation-linux/#install-timescaledb-on-linux)

You can host TimescaleDB yourself, on your Debian-based, Red Hat-based, or Arch Linux-based systems. These instructions use the `apt`, `yum`, and `pacman` package manager on these distributions:

1. At the command prompt, as root, add the PostgreSQL third party repository to get the latest PostgreSQL packages:

```
sudo yum install https://download.postgresql.org/pub/repos/yum/reporpms/EL-$(rpm -E %{rhel})-x86_64/pgdg-redhat-repo-latest.noarch.rpm
```

2. Create the TimescaleDB repository:

```
sudo tee /etc/yum.repos.d/timescale_timescaledb.repo <<EOL
[timescale_timescaledb]
name=timescale_timescaledb
baseurl=https://packagecloud.io/timescale/timescaledb/el/$(rpm -E %{rhel})/\$basearch
repo_gpgcheck=1
gpgcheck=0
enabled=1
gpgkey=https://packagecloud.io/timescale/timescaledb/gpgkey
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300
EOL
```

3. Update your local repository list:

```
sudo yum update
```

4. Install TimescaleDB:

```
sudo yum install timescaledb-2-postgresql-14
```

5. Initialize the database

```
sudo /usr/pgsql-14/bin/postgresql-14-setup initdb
```

6. Configure your database by running the `timescaledb-tune` script, which is included with the `timescaledb-tools` package. Run the `timescaledb-tune` script using the `sudo timescaledb-tune --pg-config=/usr/pgsql-14/bin/pg_config` command. For more information, see the [configuration](https://docs.timescale.com/self-hosted/latest/configuration/) section.

## [Set up the TimescaleDB extension](https://docs.timescale.com/self-hosted/latest/install/installation-linux/#set-up-the-timescaledb-extension)

When you have PostgreSQL and TimescaleDB installed, you can connect to it from your local system using the `psql` command-line utility.

### [Install psql on Linux](https://docs.timescale.com/self-hosted/latest/install/installation-linux/#install-psql-on-linux)

You can use the `apt` on Debian-based systems, `yum` on Red Hat-based systems, and `pacman` package manager to install the `psql` tool.

1. Make sure your `yum` repository is up to date:

```
sudo yum update
```

2. Install the `postgresql-client` package:

```
sudo dnf install postgresql14
```

### [Setting up the TimescaleDB extension on Red Hat-based systems](https://docs.timescale.com/self-hosted/latest/install/installation-linux/#setting-up-the-timescaledb-extension-on-red-hat-based-systems)

1. Enable and start the service:

```
sudo systemctl enable postgresql-14
sudo systemctl start postgresql-14
```

2. Connect to the PostgreSQL instance as the `postgres` superuser:

```
sudo -i
sudo -u postgres psql
```

3. Set the password for the `postgres` user using:

```
\password postgres
```

password set to: postgres
4. Exit from PostgreSQL using the command `\q`.
5. Use `psql` client to connect to PostgreSQL:

```
psql -U postgres -h localhost
```

6. At the `psql` prompt, create an empty database. Our database is called `tsdb`:

```
CREATE database tsdb;
```

7. Connect to the database you created:

```
\c tsdb
```

8. Add the TimescaleDB extension:

```
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

it can end with:

```
FATAL:  extension "timescaledb" must be preloaded
HINT:  Please preload the timescaledb library via shared_preload_libraries.

This can be done by editing the config file at: /var/lib/pgsql/14/data/postgresql.conf
and adding 'timescaledb' to the list in the shared_preload_libraries config.
# Modify postgresql.conf:
shared_preload_libraries = 'timescaledb'

Another way to do this, if not preloading other libraries, is with the command:
echo "shared_preload_libraries = 'timescaledb'" >> /var/lib/pgsql/14/data/postgresql.conf 

(Will require a database restart.)


If you REALLY know what you are doing and would like to load the library without preloading, you can disable this check with:
SET timescaledb.allow_install_without_preload = 'on';
server closed the connection unexpectedly
This probably means the server terminated abnormally
before or while processing the request.
The connection to the server was lost. Attempting reset: Succeeded.
```

so perform:

```
sudo vim /var/lib/pgsql/14/data/postgresql.conf
```

and add following line to config:

```
shared_preload_libraries = 'timescaledb'
```

then restart the postgresql:

```
sudo systemctl restart postgresql-14
```

Then perform the add TimescaleDB extension:

```
sudo psql -U postgres -h localhost
\c tsdb
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

9. Check that the TimescaleDB extension is installed by using the `\dx` command at the `psql` prompt. Output is similar to:

```
tsdb-# \dx
List of installed extensions
    Name     | Version |   Schema   | Description

-------------+---------+------------+-----------------------------
 plpgsql     | 1.0     | pg_catalog | PL/pgSQL procedural language
 timescaledb | 2.7.0   | public     | Enables scalable inserts and complex queries for time-series data
(2 rows)
```

After you have created the extension and the database, you can connect to your database directly using this command:

```
psql -U postgres -h localhost -d tsdb
```

Now that you have your first Timescale database up and running, you can check out the [Use Timescale](https://docs.timescale.com/use-timescale/latest/) section, and find out what you can do with it.

<https://docs.timescale.com/getting-started/latest/tables-hypertables/>

### [Creating your first hypertable](https://docs.timescale.com/getting-started/latest/tables-hypertables/#creating-your-first-hypertable)

1. At the command prompt, use the `psql` connection string from the cheat sheet you downloaded to connect to your database.
2. Create a regular PostgreSQL table to store the real-time stock trade data using `CREATE TABLE`:

   ```
   CREATE TABLE AD_METRICS (

  time TIMESTAMPTZ NOT NULL,
  ID_ip INT NOT NULL,
  n_flows INT NOT NULL,
  n_packets INT NOT NULL,
  n_bytes BIGINT NOT NULL,
  n_dest_ip_pri INT NOT NULL,
  n_dest_ip_pub INT NOT NULL,
  n_dest_asn INT NOT NULL,
  n_dest_countries INT NOT NULL,
  n_dest_ports INT NOT NULL,
  tcp_udp_ratio_packets DOUBLE PRECISION NOT NULL,
  tcp_udp_ratio_bytes DOUBLE PRECISION NOT NULL,
  dir_ratio_packets DOUBLE PRECISION NOT NULL,
  dir_ratio_bytes DOUBLE PRECISION NOT NULL,
  avg_duration DOUBLE PRECISION NOT NULL,
  avg_ttl DOUBLE PRECISION NOT NULL
   );

   ```

3. Convert the regular table into a hypertable partitioned on the `time` column using the `create_hypertable()` function provided by Timescale. You must provide the name of the table (`AD_METRICS`) and the column in that table that holds the timestamp data to use for partitioning (`time`):

   ```

   SELECT create_hypertable('AD_METRICS','time');

   ```

4. Create an index to support efficient queries on the `ID_ip` and `time` columns:

```

   CREATE INDEX ix_ID_ip_time ON AD_METRICS (ID_ip, time DESC);

```

## [Create regular PostgreSQL tables for relational data](https://docs.timescale.com/getting-started/latest/tables-hypertables/#create-regular-postgresql-tables-for-relational-data)

Timescale isn't just for hypertables. When you have other relational data that enhances your time-series data, you can create regular PostgreSQL tables just as you would normally. For this dataset, there is one other table of data called `IP_ADDRESS`.

1. Add a table to store the company name and symbol for the stock trade data:

```

CREATE TABLE IP_ADDRESS (
  ID_IP INT NOT NULL,
  ip_address TEXT NOT NULL,
  note TEXT NOT NULL
);

```

2. You now have two tables within your Timescale database. One hypertable named `AD_METRICS`, and one normal PostgreSQL table named `IP_ADDRESS`. You can check this by running this command at the `psql` prompt:

```

\dt

```

 This command returns information about your tables, like this:

```

List of relations
 Schema |       Name       | Type  |   Owner
--------+------------------+-------+-----------
 public | IP_ADDRESS       | table | tsdbadmin
 public | AD_METRICS       | table | tsdbadmin
(2 rows)

```

Test with insert test IP address:

```

INSERT INTO ip_address (id_ip, ip_address, note)  VALUES (0, 'test', 'test');

```

display it:

```

SELECT * FROM ip_address;

```

Test with insert data for test IP address:

```

INSERT INTO ad_metrics (time, id_ip, n_flows, n_packets, n_bytes, n_dest_ip_pri, n_dest_ip_pub, n_dest_asn, n_dest_countries, n_dest_ports, tcp_udp_ratio_packets, tcp_udp_ratio_bytes, dir_ratio_packets, dir_ratio_bytes, avg_duration, avg_ttl)  VALUES ('2016-01-25 10:10:10.555555-05:00', (SELECT id_ip FROM ip_address WHERE ip_address.ip_address = 'test'), 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);

```

display it:

```

SELECT * FROM ad_metrics;

```

and:

```

SELECT * FROM ad_metrics JOIN ip_address ON ad_metrics.ID_ip = ip_address.ID_ip;

```

Add port number:

```

sudo vim /var/lib/pgsql/14/data/postgresql.conf

```

uncomment line:

```

port = 5432

```

and change:

```

listen_addresses = '*'

```

then open file:

```

sudo vim /var/lib/pgsql/14/data/pg_hba.conf

```

add line:

```

host  all  all 0.0.0.0/0 scram-sha-256

```

restart postgresql:

```

sudo systemctl restart postgresql-14

```

add to firewall:

```

 sudo systemctl unmask firewalld
 sudo systemctl start firewalld
 sudo systemctl enable firewalld
 sudo firewall-cmd --add-port 5432/tcp --permanent
 sudo firewall-cmd --add-port 5432/udp --permanent
 sudo firewall-cmd --reload

```

## Add SSL/TLS

Create a certificate signing request:

```

umask u=rw,go= && openssl req -days 3650 -new -text -nodes -subj '/CN=localhost' -keyout server.key -out server.csr

```

Generate self-signed certificate:

```

umask u=rw,go= && openssl req -days 3650 -x509 -text -in server.csr -key server.key -out server.crt

```

Also make the server certificate to be the root-CA certificate:

```

umask u=rw,go= && cp server.crt root.crt

```

Remove the now-refunfant CSR:

```

rm server.csr

```

Create a certificate signing request (CN=db-user) where <host-url> is template:

```

umask u=rw,go= && openssl req -days 3650 -new -nodes -subj '/CN=<host-url>' -keyout client.key -out client.csr

```

Create a signed certificate for the client using our root certificate:

```

umask u=rw,go= && openssl x509 -days 3650 -req  -CAcreateserial -in client.csr -CA root.crt -CAkey server.key -out client.crt

```

Remove the now-redundant CSR:

```

rm client.csr

```

Edit postgresql.conf:

```

sudo vim /var/lib/pgsql/14/data/postgresql.conf

```

rewrite SSL section on confuguration to match following (the certificate's files are stored in `/postgres_ssl_certs/`):

```

# - SSL -

ssl = on
ssl_ca_file = '/postgres_ssl_certs/root.crt'
ssl_cert_file = '/postgres_ssl_certs/server.crt'
ssl_key_file = '/postgres_ssl_certs/server.key'

```

Rewrite pg_hba.conf:

```

sudo vim /var/lib/pgsql/14/data/pg_hba.conf

```

on:

```

# TYPE  DATABASE        USER            ADDRESS                 METHOD

hostssl all     all     0.0.0.0/0               scram-sha-256

```

Set permissions of certificates:

```

sudo chown root *
sudo chmod 777*
sudo chmod 640 server.key
sudo chgrp postgres server.key

```

Restart posgres:

```

sudo systemctl restart postgresql-14

```

On the client the files `client.crt`, `client.key` and  `root.crt`. Change the permissions of file `client.key`:

```

sudo chmod 600 client.key

```

### Ad compression for large data

```

ALTER TABLE ad_metrics SET (timescaledb.compress, timescaledb.compress_segmentby = 'id_ip');

SELECT add_compression_policy('ad_metrics', INTERVAL '7 days');

```

## Use DB

Připojení k DB na serveru:

```

psql -h <IP_Address> -p <port_no> -d <database_name> -U <DB_username> -W

```

Nebo pomocí Python3 z hosta:

```

import psycopg2
conn = psycopg2.connect(
       "host='<host-url>' \
       dbname='tsdb' user='postgres' \
       password='postgres' \
       sslmode='require' \
       sslrootcert='root.crt' \
       sslcert=client.crt \
       sslkey=client.key \
       port='5432'"
    )
cursor = conn.cursor()
cursor.execute("SELECT * FROM ip_address;")
cursor.fetchone()
conn.commit()
cursor.close()

```

### Get data to csv

Get data for each IP addresses into separate csv file with following bash script:

```

# !/bin/bash

sudo psql postgresql://postgres:postgres@localhost:5432/tsdb -c "\COPY (SELECT ip_address.ip_address FROM ip_address) TO tmp/ip_addresses.csv DELIMITER ',' CSV"

while read p; do
  echo "$p"
  sudo psql postgresql://postgres:postgres@localhost:5432/tsdb -c "\COPY (SELECT ad_metrics.time, ad_metrics.n_flows, ad_metrics.n_packets, ad_metrics.n_bytes, ad_metrics.n_dest_ip_pri, ad_metrics.n_dest_ip_pub, ad_metrics.n_dest_asn, ad_metrics.n_dest_countries, ad_metrics.n_dest_ports, ad_metrics.tcp_udp_ratio_packets, ad_metrics.tcp_udp_ratio_bytes, ad_metrics.dir_ratio_packets, ad_metrics.dir_ratio_bytes, ad_metrics.avg_duration, ad_metrics.avg_ttl FROM ad_metrics JOIN ip_address ON ad_metrics.ID_ip = ip_address.ID_ip WHERE ip_address.ip_address = '$p') TO tmp/$p.csv DELIMITER ',' CSV"
  tar -czvf "tmp/$p.csv.tar.gz" "tmp/$p.csv"
  rm -rf "tmp/$p.csv"
done < tmp/ip_addresses.csv

```
