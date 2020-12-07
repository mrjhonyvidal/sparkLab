# INTRODUCTION

You may find different projects of Spark and Python on each sub folder. 

#### Set up python Jupyter notebook and Spark

Below is one posible way of setting up your dev environment, a very useful and quick way of test is use: Codelab or Databricks for set up a cluster environment, the tools and run all the tests. 
```
wget http://repo.continuum.io/archive/Anaconda3-4.1.1-Linux-x86_64.sh

which python3

source .bashrc

Instalar py4j:
```

```
export PATH=$PATH:$HOME/anaconda3/bin
conda install pip
which pip
pip install py4j
```

```
    package                    |            build
    ---------------------------|-----------------
    conda-env-2.6.0            |                0          502 B
    requests-2.14.2            |           py35_0         723 KB
    ruamel_yaml-0.11.14        |           py35_1         395 KB
    pip-9.0.1                  |           py35_1         1.7 MB
    pyopenssl-16.2.0           |           py35_0          70 KB
    conda-4.3.30               |   py35hf9359ed_0         516 KB
    ------------------------------------------------------------
                                           Total:         3.4 MB

The following packages will be UPDATED:

    conda:       4.1.6-py35_0  --> 4.3.30-py35hf9359ed_0
    conda-env:   2.5.1-py35_0  --> 2.6.0-0              
    pip:         8.1.2-py35_0  --> 9.0.1-py35_1         
    pyopenssl:   0.16.0-py35_0 --> 16.2.0-py35_0        
    requests:    2.10.0-py35_0 --> 2.14.2-py35_0        
    ruamel_yaml: 0.11.7-py35_0 --> 0.11.14-py35_1 
```
