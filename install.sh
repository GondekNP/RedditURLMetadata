#!/bin/bash 

# 1. First check to see if the correct version of Python is installed on the local machine 
echo "1. Checking Python version..."
REQ_PYTHON_V="380"

ACTUAL_PYTHON_V=$(python -c 'import sys; version=sys.version_info[:3]; print("{0}{1}{2}".format(*version))')
ACTUAL_PYTHON3_V=$(python3 -c 'import sys; version=sys.version_info[:3]; print("{0}{1}{2}".format(*version))')

if [[ $ACTUAL_PYTHON_V > $REQ_PYTHON_V ]] || [[ $ACTUAL_PYTHON_V == $REQ_PYTHON_V ]];  then 
    PYTHON="python"
elif [[ $ACTUAL_PYTHON3_V > $REQ_PYTHON_V ]] || [[ $ACTUAL_PYTHON3_V == $REQ_PYTHON_V ]]; then 
    PYTHON="python3"
else
    echo -e "\tPython 3.7 is not installed on this machine. Please install Python 3.7 before continuing."
    exit 1
fi

echo -e "\t--Python 3.8+ is installed"

# 2. What OS are we running on?
PLATFORM=$($PYTHON -c 'import platform; print(platform.system())')

echo -e "2. Checking OS Platform..."
echo -e "\t--OS=Platform=$PLATFORM"

# 3. Create Virtual environment/Install requirements
echo -e "3. Checking for Conda installation..."

# If installing conda, accept the terms of agreement and initialize it when asked
which conda
CONDA=$?
if [[ $CONDA == 1 ]] && [[ $PLATFORM == 'Linux' ]]; then
    echo -e '\t--Installing miniconda for Linux...'
    mkdir ~/.conda
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh
elif [[ $CONDA == 1 ]] && [[ $PLATFORM == 'Darwin' ]]; then
    echo -e '\t--Installing miniconda for OSX...'
    mkdir ~/.conda
    curl -sL \
    "https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh" > \
    "Miniconda3.sh"
    bash Miniconda3.sh
elif [[ $CONDA == 1 ]] && [[ $PLATFORM == 'Windows' ]]; then
    echo -e '\t--Installing miniconda for Windows...'
    mkdir ~/.conda
    powershell -command "Invoke-WebRequest -Uri https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe -OutFile ~\miniconda.exe"
else
    echo -e '\t--Conda already installed.'
fi

exec bash

echo -e "4. Creating Conda environment and installing dependencies..."
conda init bash
conda env create -f CS122_Project_Env.yml
conda activate CS122_Env

echo -e "Environment CS_122 Env active - Install is complete."

