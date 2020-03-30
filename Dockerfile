FROM python:3.7-slim
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN pip3 install -r requirements.txt
EXPOSE 8000
CMD ["/usr/local/bin/python3 /app/nbexecutor.py"]
