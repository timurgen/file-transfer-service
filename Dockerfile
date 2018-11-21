FROM python:3-alpine
MAINTAINER Timur Samkharadze "timur.samkharadze@sysco.no"
COPY ./service /service
WORKDIR /service
RUN pip install -r requirements.txt
EXPOSE 5000/tcp
ENTRYPOINT ["python"]
CMD ["service.py"]