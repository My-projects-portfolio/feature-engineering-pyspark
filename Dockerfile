FROM bitnami/spark:3.1.3

# Set working directory inside container
WORKDIR /app

# Copy your PySpark script and data into the container
COPY ./spark-app /app/spark-app
COPY ./data /app/data

# Run your PySpark script when the container starts
CMD ["/opt/bitnami/spark/bin/spark-submit", "/app/spark-app/feature_engineering.py"]
