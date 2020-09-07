FROM debian:stretch-slim

WORKDIR /app

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
 python3-pip ca-certificates wget rsync telnet net-tools procps \
 pv mysql-client shunit2 vim nginx

# By copying over requirements first, we make sure that Docker will cache
# our installed requirements rather than reinstall them on every build
COPY requirements.txt /app/requirements.txt
RUN pip3 install -r requirements.txt

# Configure MySQL client
COPY containers/common/mysql.cnf /etc/mysql/conf.d/
COPY containers/common/mysqldump.cnf /etc/mysql/conf.d/
# Configure gunicorn
RUN mkdir /etc/gunicorn && chmod 755 /etc/gunicorn
COPY containers/addressapi/gunicorn_config.py /etc/gunicorn/config.py
# Configure nginx
COPY containers/addressapi/nginx.conf /etc/nginx/nginx.conf

# Now copy in our code
COPY . /app

EXPOSE 8001

CMD ["bash", "containers/addressapi/launch.sh"]

