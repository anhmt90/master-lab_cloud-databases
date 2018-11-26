if [[ $# -eq 0 ]] ; then
  echo "Number of containers to spawn is not given, spawning 10 containers"
  N=10
else
  N=$1
fi

OUT_FILE="docker-compose.yml"
CONFIG_FILE="./config/server-info"

docker-compose down

NETWORK="172.172.1.0"
NETWORK_HEX=$(printf '%.2X%.2X%.2X%.2X\n' `echo $NETWORK | sed -e 's/\./ /g'`)
START_PORT=4000

function inc_ip {
  ip=$1
  inc=$2

  ip_hex=$(printf %.8X `echo $(( 0x$ip + $inc ))`)
  r_ip=$(printf '%d.%d.%d.%d\n' `echo $ip_hex | sed -r 's/(..)/0x\1 /g'`)
  echo "$r_ip"
}

function write_service {
  idx=$1
  port=$((START_PORT + idx))
  ip_inc=$((idx + 1))
  host=$(inc_ip $NETWORK_HEX $ip_inc)

  echo "node$idx $host $port" >> "$CONFIG_FILE"

  cat >> "$OUT_FILE" << EOF
  kv$idx:
    build: ./docker/.
    expose:
      - "22"
      - "$port"
    networks:
      performance_test_ntwrk:
        ipv4_address: $host
    volumes:
      - "./:/opt/app"

EOF
}

echo -n "" > "$CONFIG_FILE"

echo "version: '3'" > "$OUT_FILE"
echo "services:" >> "$OUT_FILE"
for i in `seq 1 $N`; do
  write_service $i
done
cat >> "$OUT_FILE" << EOF

networks:
  performance_test_ntwrk:
    ipam:
      driver: default
      config:
        - subnet: $NETWORK/24
EOF

docker-compose up -d
