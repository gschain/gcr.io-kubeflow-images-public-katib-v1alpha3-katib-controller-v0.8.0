FROM python:3.7-slim
RUN mkdir /app
ADD nbexecutor.py /app
ADD requirements.txt /app
WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 8000
CMD ["python nbexecutor.py"]
