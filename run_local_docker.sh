sudo docker-compose -f local.yml down
# docker system prune -a
# sudo docker-compose -f local.yml up --scale open_keeper_sandbox=10 --build --remove-orphans
sudo docker-compose -f local.yml up --build --remove-orphans