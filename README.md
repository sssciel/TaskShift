# TaskShift
Subsystem of dynamic task launch for the "HPC TaskMaster".


## Description

**TaskShift** is designed to increase the utilization efficiency of a supercomputer cluster managed by the SLURM scheduler — particularly for the **cHARISMa** supercomputer at the Higher School of Economics (HSE).

The system operates as a background service (daemon), triggered via a cron-like timer. Every Friday, it trains a forecasting model using **NeuralProphet**, and throughout the weekend it periodically evaluates the predicted load to decide whether to promote specific tasks for early execution.


## Installation
Choose either Docker-based deployment or manual setup.
### Via Docker
1. Clone repository
```bash
git clone https://github.com/sssciel/TaskShift.git
cd TaskShift
```
2. Configure service with .yml and .env files in ``source/configs`` folder.
3. (Optional) Remove the influxdb service from docker-compose.yml if using an external InfluxDB host.
4. Run docker-compose
```bash
docker-compose up --build
```
### Manual installation
1. Install ``InfluxDB v2`` and ``make`` manually or via your package manager.
2. Clone repository
```bash
git clone https://github.com/sssciel/TaskShift.git
cd TaskShift
```
3. Install pip dependencies using Python 3.12.7. You can use pyenv and venv
```bash
pyenv install 3.12.7 && pyenv local 3.12.7
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
4. Configure service with .yml and .env files under ``source/configs`` folder.
5. Run the prophet using make file
```shell
make
```

### After installation
1. Save load data to InfluxDB to selected in .env file bucket. It must has ``cpu_load`` and ``gpu_load`` measurement names.
2. Wait for Friday or adjust the timer time in the main file yourself.

## Test
- With manual installation, tests are automatically run via make.
- For Docker:
```shell
docker-compose exec python-service pytest
```

## API
You can disable the service and change the predicted workload while the scheduler is running. For this, the REST API is used from the host where the service is running, other connections are ignored.