FROM python:3.9.0
RUN apt-get update && apt-get install -y unixodbc unixodbc-dev
WORKDIR /app
RUN mkdir /app/WcheckFile
RUN mkdir /app/WcheckArchieve
RUN mkdir /app/WcheckError
ENV FILE_PATH=/app/WcheckFile
ENV ARCHIEVE_PATH=/app/WcheckArchieve
ENV ERROR_PATH=/app/WcheckError
COPY main.py worldcheck.py worldcheckCsv.py env.py config.py test.env mySql.env sql.env  ./
RUN pip install Flask
RUN pip install requests
RUN pip install numpy
RUN pip install pandas
RUN pip install pyodbc
RUN pip install jwt
RUN pip install mysqlclient
RUN pip install DateTime
RUN pip install memory-profiler
RUN pip install mysql-connector-python
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install python-dotenv
CMD ["python","./main.py"]
