FROM python:3.7.8
ADD . /app
WORKDIR /app
RUN pip3 install -r requirements.txt
CMD ["python", "app.py"]