# Pasos para correr el entorno de trabajo Django

python3 -m venv venv
pip install -r requirements.txt
python -m manage.py runserver

# Pasos para instalación de R en el server

sudo apt update
sudo apt install --no-install-recommends software-properties-common dirmngr -y

wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | sudo tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc

sudo add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu noble-cran40/"

sudo apt update
sudo apt install r-base r-base-dev -y

# Paso con conda
ir a https://docs.conda.io/en/latest/miniconda.html
https://www.anaconda.com/download

conda --version

conda create -n r_env python=3.10 r-base rpy2 -c conda-forge

conda activate r_env

python script.py

##############################

conda install -c conda-forge r-base [package]
conda activate r_env


