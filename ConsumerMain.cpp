
#include "KafkaConsumer.h"
#include <csignal>

static bool run = true;
void sigterm(int sig)
{
    run = false;
}

int main(int argc, char const *argv[])
{
    //signal(SIGINT, sigterm);
    //signal(SIGTERM, sigterm);
    //signal(SIGKILL, sigterm);

    KafkaConsumer consumer;
    //std::string topic = "heima";
    std::vector<std::string> topics = {"heima","test02"};
    consumer.SetTopic(topics);    
    consumer.Init("localhost", 9092, "101");
    consumer.Consume();
    //consumer.Recv(1000);
    if(run == false)
    {
        std::cout << "stop consumer" << std::endl;
        consumer.Stop();
    }
    return 0;
}

