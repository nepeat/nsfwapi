FROM pypy:3

WORKDIR /app

COPY requirements.txt /app
RUN DEBIAN_FRONTEND=noninteractive CASS_DRIVER_NO_CYTHON=1 apt-get -y update \
  && apt-get -y install build-essential \
  && pip install --no-cache-dir -v -r requirements.txt \
  && apt-get -y remove build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY . /app
RUN pypy3 setup.py install

CMD ["pypy3", "worker"]