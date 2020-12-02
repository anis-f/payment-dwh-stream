docker build -t payment-dwh-stream-service .
helm delete payment-flink-stream
sleep 10
cd helm
helm install payment-flink-stream flink
cd ..
