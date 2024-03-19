# python-snippets
 
# TODO
- https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b
- TODO: Finish local setup in Windows
- TODO: Try Spark Docker image and install PySpark

All developed using Windows 10 Pro

## Versions
```bash
python 3.12
java 11
```

## Windows things

Set IDE to use LF as Line separators for `docker-entrypoint.sh`.

After running the pyspark scripts (Docker or pipenv), you might get:
```bash
    > SUCCESS: The process with PID 22616 (child process of PID 4280) has been terminated.
SUCCESS: The process with PID 4280 (child process of PID 4380) has been terminated.
SUCCESS: The process with PID 4380 (child process of PID 21016) has been terminated.
```
This is probably due to some java incompatibility on my Windows machine.
