#include <random>
#include <atomic>
#include <thread>
#include <vector>
#include "../mwkafka_producer.h"

std::atomic<int64_t> _offset(0);
static const std::string brokers = "192.169.6.251:9092,192.169.6.251:9093,192.169.6.251:9094";
static const std::string topic_nameg = "TEST_01";
static const std::string topic_namec = "TEST_0";
#define _LOOP(index) for(int index =0;index<10;index++)

void producer_on_msg(void* pInvoker, const MessagePtr& pMsg)
{	
	/*printf("date: %s\n", pMsg->data.c_str());
	printf("status: %d\n", pMsg->status);
	printf("errcode: %d\n", pMsg->errcode);
	printf("errmsg: %s\n", pMsg->errmsg.c_str());
	printf("partition: %d\n", pMsg->partition);
	printf("offset: %ld\n", pMsg->offset);
	printf("\n");*/
	
	printf("%ld\n", ++_offset);
}
void multi_producer()//批量多线程提交
{	
	mwKafka_Producer producer(brokers, true);
	producer.AddCallBack(DELIVERED, producer_on_msg);

	auto _fun = [&](const std::string& topic_,int partition)
	{
		int pos = 0;
		while (pos < 10000)
		{
			std::string _text = u8"编号：" + std::to_string(pos++) + u8" 我是分区" + std::to_string(partition);
			producer.Produce({ topic_,partition }, _text);
		}
	};

	std::vector<std::thread>_thread;
	_LOOP(i)
	{
		std::string topic = topic_namec + std::to_string(i);
		producer.AddTopics(topic);
		_thread.push_back(std::thread(_fun, topic,0));
		_thread.push_back(std::thread(_fun, topic,1));
		_thread.push_back(std::thread(_fun, topic,2));
	}	
	
	_LOOP(i)
	{
		_thread[i].join();
	}

	while (true)
	{
		producer.Poll(500);
	}
	
}
void sigle_producer()//单线程提交
{	
	mwKafka_Producer producer(brokers, true);	
	producer.AddCallBack(DELIVERED, producer_on_msg);

	producer.AddTopics( topic_nameg);

	const char* text = u8"阿萨德接电话爱仕达按实际活动即可拉伸活动空间来说都阿萨德好可怜爱仕达卢卡斯";
	int pos = 100000;
	while (pos-- > 0)
	{		
		std::string _text = text +  std::to_string(pos);
		producer.Produce(topic_nameg, _text);
	}
	while (true)
	{
		producer.Poll(500);
	}
}
#if 0
int main()
{	
	sigle_producer();
	//multi_producer();
	
	return 0;
}
#endif

