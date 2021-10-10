rp:
  docker run -d --name=redpanda-1 --rm \
    -p 9092:9092 \
    -p 9644:9644 \
    docker.vectorized.io/vectorized/redpanda:latest \
    redpanda start \
    --smp 1  \
    --memory 1G \
    --reserve-memory 256M \
    --node-id 0 \
    --check=false

worker:
  RUST_LOG=debug cargo run --bin worker

client $name:
  RUST_LOG=debug cargo run --bin client $name
