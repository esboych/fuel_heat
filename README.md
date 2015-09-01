# fuel_heat
This code allows to deploy MOS conguration using FUEL with one command and one config file.

    $ python fuel_heat.py -a LOGIN:PASSWD:TENANT -u FUEL_URL config.yaml

Simple procedure to start deploying Cluster in Fuel 7.0

From the fuel-heat directory:

- Virtualenv and requirements

$ virtualenv venv

$ source venv/bin/activate

$ pip install -r requirements.txt

-Now launch: tenant/login, Master node IP, config file

$ python fuel_heat.py -a admin:admin:admin -u http://172.16.52.112:8000 config_GRE_Perf1_7.yaml
