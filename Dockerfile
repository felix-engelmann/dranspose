FROM python:3

WORKDIR /tmp

COPY . /tmp

RUN python -m pip --no-cache-dir install .

CMD ["dranspose"]