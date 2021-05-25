# How to build a lambda that can connect to MSSQL RDS

## Build environment
The easiest way to ensure that your lambda execution environment matches the runtime environment is to use an Amazon provided image.

1. Create a new directory that will contain the lambda function and dependencies eg. `/home/kev/projects/lambda`
2. Run docker with the latest version of amazonlinux with your lambda working directory mounted as a volume
`sudo docker run -v <path to lambda dir>:/working -it --rm amazonlinux:2017.12`
3. From in the docker environment install the dependencies eg.
..1 `yum install python-pip`
..2 `cd working`
..3 `pip install -t . pymssql`
4. Exit the docker environment
5. Zip your working directory from inside the working directory eg. `zip -r9 ~/pymssql-lambda.zip *`
6. Upload the zip `pymssql-lambda.zip` to the lambda console
