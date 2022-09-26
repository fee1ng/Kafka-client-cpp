#include <iostream>
#include <string>
#include <list>
#include <stdint.h>
#include "rdkafka.h"
#include "rdkafkacpp.h"
#include <thread>
#include <vector>
#include <atomic>
#include "glog/logging.h"
#include "glog/raw_logging.h"
using namespace std;

static int partition_cnt = 0;
static int eof_cnt = 0;

class ExampleRebalanceCb : public RdKafka::RebalanceCb {
 private:
  static void part_list_print(
      const std::vector<RdKafka::TopicPartition *> &partitions) {
    for (unsigned int i = 0; i < partitions.size(); i++)
      std::cerr << partitions[i]->topic() << "[" << partitions[i]->partition()
                << "], ";
    std::cerr << "\n";
  }

 public:
  void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                    RdKafka::ErrorCode err,
                    std::vector<RdKafka::TopicPartition *> &partitions) {
    std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

    part_list_print(partitions);

    RdKafka::Error *error      = NULL;
    RdKafka::ErrorCode ret_err = RdKafka::ERR_NO_ERROR;

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      if (consumer->rebalance_protocol() == "COOPERATIVE")
        error = consumer->incremental_assign(partitions);
      else
        ret_err = consumer->assign(partitions);
      partition_cnt += (int)partitions.size();
    } else {
      if (consumer->rebalance_protocol() == "COOPERATIVE") {
        error = consumer->incremental_unassign(partitions);
        partition_cnt -= (int)partitions.size();
      } else {
        ret_err       = consumer->unassign();
        partition_cnt = 0;
      }
    }
    eof_cnt = 0; /* FIXME: Won't work with COOPERATIVE */

    if (error) {
      std::cerr << "incremental assign failed: " << error->str() << "\n";
      delete error;
    } else if (ret_err)
      std::cerr << "assign failed: " << RdKafka::err2str(ret_err) << "\n";
  }
};

void dump_config(RdKafka::Conf* conf) 
{
    std::list<std::string> *dump = conf->dump();
    printf("config dump(%d):\n", (int32_t)dump->size());
    for (auto it = dump->begin(); it != dump->end();) 
    {
        std::string name = *it++;
        std::string value = *it++;
        printf("%s = %s\n", name.c_str(), value.c_str());
    }
    printf("---------------------------------------------\n");
}

void msg_consume(RdKafka::Message* msg,atomic<int>& msg_cnt)
{
    if (msg->err() == RdKafka::ERR_NO_ERROR)
    {
        //std::cout << "Read msg at offset " << msg->offset() << std::endl;
        LOG(INFO) << "msg at offset = " << msg->offset();
        LOG(INFO) << "msg_cnt = " << msg_cnt;
        if (msg->key())
        {
            //std::cout << "Key: " << *msg->key() << std::endl;
            LOG(INFO) << *msg->key();
        }
        printf("%.*s\n", static_cast<int>(msg->len()), static_cast<const char *>(msg->payload()));
        msg_cnt++;
    }
    else if (msg->err() == RdKafka::ERR__TIMED_OUT)
    {
        // 读完了 再读取 状态码为RdKafka::ERR__TIMED_OUT
        printf("error[%s]\n", "ERR__TIMED_OUT");
    }
    else
    {
        printf("error[%s]\n", "other");
    }
}

int consume(int _partition)
{   
    atomic<int> msg_cnt(0);
    string err_string;
    int32_t partition = RdKafka::Topic::PARTITION_UA;
    partition = _partition;

    std::string broker_list = "localhost:9092";

    RdKafka::Conf* global_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf* topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    // 从指定offset开始消费
    int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;
    //int64_t start_offset = 100;

    ExampleRebalanceCb ex_rebalance_cb;
    global_conf->set("metadata.broker.list", broker_list, err_string);
    global_conf->set("rebalance_cb", &ex_rebalance_cb, err_string);

    //dump_config(global_conf);
    //dump_config(topic_conf);

    // create consumer
    RdKafka::Consumer* consumer = RdKafka::Consumer::create(global_conf, err_string);
    if (!consumer) {
        printf("failed to create consumer, %s\n", err_string.c_str());
        return -1;
    }
    printf("created consumer %s\n", consumer->name().c_str());

    // create topic
    std::string topic_name = "test02";
    RdKafka::Topic* topic = RdKafka::Topic::create(consumer, topic_name, topic_conf, err_string);
    if (!topic) {
        printf("try create topic[%s] failed, %s\n", topic_name.c_str(), err_string.c_str());
        return -1;
    }

    // Start consumer for topic+partition at start offset
    RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
    if (resp != RdKafka::ERR_NO_ERROR) {
        printf("Failed to start consumer: %s\n",RdKafka::err2str(resp).c_str());
        return -1;
    }

    while (true) 
    {
        RdKafka::Message *msg = consumer->consume(topic, partition, 2000);
        msg_consume(msg,msg_cnt);
        //cout << "msg_cnt = " << msg_cnt << endl;
        delete msg;
    }

    // stop consumer
    consumer->stop(topic, partition);
    consumer->poll(1000);

    delete topic;
    delete consumer;

    return 0;
}

int main(int argc,char** argv) {

    FLAGS_log_dir = "logs";
    // 初始化
    google::InitGoogleLogging(argv[0]);
    // 设置输出颜色
    FLAGS_colorlogtostderr = true;
    // 配置日志保存15天
    google::EnableLogCleaner(15);

    vector<thread> vec_thread(3);
    for(int i = 0;i < 3;i++) {
        thread t(consume,i);
        vec_thread[i] = move(t);
    }
    for(auto& th:vec_thread) {
        th.join();
    }

    google::ShutdownGoogleLogging();
    return 0;
}