all : consumer producer
.PHONY:all

consumer : consumer.cpp
	g++ consumer.cpp -o consumer  -I/usr/local/include/librdkafka/ -L/usr/local/lib -lrdkafka++ -I/usr/local/src/zabbix_data_moniotr_system/include -L/usr/local/src/zabbix_data_moniotr_system/lib -lpthread -lglog

producer : producer.cpp producer.h
	g++ -c producer.cpp -o producer  -I/usr/local/include/librdkafka/ -L/usr/local/lib -lrdkafka++ -lpthread
