sudo apt update
sudo apt install -y protobuf-compiler python3-pip
cd ~
git clone https://github.com/satorikoishi/cloudburst.git
cd cloudburst
pip3 install --upgrade pip
pip3 install -r requirements.txt
git submodule update --init --recursive
./common/scripts/install-dependencies.sh
sudo ./scripts/install-anna.sh
