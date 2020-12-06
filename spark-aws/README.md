# Set up Spark with AWS

Create a Ubuntu VM on AWS EC2.

Connect to it via ssh `ssh -i "MYPRIVATEKEY.pem" ubuntu@MYVM.compute.amazonaws.com`

Inside VM, set up our toolbox environment:
```
wget http://repo.continuum.io/archive/Anaconda3-4.1.1-Linux-x86_64.sh
bash Anaconda3-4.1.1-Linux-x86_64.sh
```

Check Python version:
```
which python3
source .bashrc
which python3
```
### Juypter Notebook
```
jupyter notebook --generate-config
```

Generate certificate in home folder ~/:
```

mkdir certs
cd certs
sudo openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mycert.pem -out mycert.pem
```

Edit config:
```
cd ~/.jupyter/
vi jupyter_notebook_config.py
```

```
c = get_config()
 
# Notebook config this is where you saved your pem cert
c.NotebookApp.certfile = u'/home/ubuntu/certs/mycert.pem' 
### this can be optional 

# Run on all IP addresses of your instance
c.NotebookApp.ip = '*'
 
# Don't open browser by default
c.NotebookApp.open_browser = False 
 
# Fix port to 8888
c.NotebookApp.port = 8888
```

Launch Jupyter notebook to test: `jupyter notebook`

### Set py4j:

```
export PATH=$PATH:$HOME/anaconda3/bin
conda install pip
which pip
pip install py4j
```

### Set Spark y Hadoop:

```
wget http://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.7.tgz
 
sudo tar -zxvf spark-2.0.0-bin-hadoop2.7.tgz
```

## Set Python in order to find Spark:

```
export SPARK_HOME='/home/ubuntu/spark-2.0.0-bin-hadoop2.7'
export PATH=$SPARK_HOME:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
```

### Launch Jupyter Notebook:

`jupyter notebook`

### Launch Spark:

```
from pyspark import SparkContext
sc = SparkContext()
```