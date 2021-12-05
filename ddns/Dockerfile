FROM python:3.8.12
WORKDIR /opt

ENV ACCESS_KEY_ID ''
ENV ACCESS_KEY_SECRET ''
ENV INTERVAL 60
ENV DOMAIN ''
ENV RR ''

RUN pip install aliyun-python-sdk-core aliyun-python-sdk-alidns requests

COPY ddns.py /opt/ddns.py

CMD python ddns.py --access_key_id ${ACCESS_KEY_ID} --access_key_secret ${ACCESS_KEY_SECRET} --interval ${INTERVAL} --domain ${DOMAIN} -RR ${RR}