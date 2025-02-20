sudo apt update
sudo apt install -y protobuf-compiler python3-pip
cd ~

# cloudburst
git clone https://github.com/satorikoishi/cloudburst.git
cd cloudburst
git checkout feature/redis-support # temporary for this branch
pip3 install -r requirements_backup.txt
git submodule update --init --recursive
./common/scripts/install-dependencies.sh
sudo ./scripts/install-anna.sh
./scripts/build.sh
cd ..

# anna
git clone --recurse-submodules https://github.com/satorikoishi/anna.git
cd anna
git checkout f3ae84d
./scripts/build.sh -g
cd ..
