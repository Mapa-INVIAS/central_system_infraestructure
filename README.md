# Pasos para correr el entorno de trabajo Django

- python3 -m venv venv
- pip install -r requirements.txt
- python -m manage.py runserver

# Pasos para instalación de R en el server

- sudo apt update
- sudo apt install --no-install-recommends software-properties-common dirmngr -y

- wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | sudo tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc

- sudo add-apt-repository "deb https://cloud.r-project.org/bin/linux/ubuntu noble-cran40/"

- sudo apt update
- sudo apt install r-base r-base-dev -y

# Paso con conda
- ir a https://docs.conda.io/en/latest/miniconda.html
https://www.anaconda.com/download

- conda --version

- conda create -n r_env python=3.10 r-base rpy2 -c conda-forge

- conda activate r_env

- python script.py

# Instalación de paquetes del proyecto con Conda

- conda install -c conda-forge r_env [package] <!-- verificar cuales son las librerias que se requieren instalar o correr el archivo requirements.txt en conda -->
- conda activate r_env

# Creación de archivo de requisitos

- pip freeze > requirements.txt
- conda list --export > requirements.txt

# Permisos de acceso

- earthengine authenticate <!-- Esta línea se debe ejecutar en la terminal -->

<!-- Verificar en el folder que se crearon las credenciales -->

- ~/.config/earthengine/credentials

