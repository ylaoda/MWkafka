#include <random>
#include "../mwkafka_consumer.h"
#include <atomic>
#include <stdio.h>
#include <thread>
#include <chrono>
std::atomic<int64_t> _offsetd(0);

static const std::string brokers = "192.169.6.251:9092,192.169.6.251:9093,192.169.6.251:9094";
static const std::string topic_nameg = "TEST_05";
static const std::string group_id = "groupA";

//错误回调
void consum_on_err(void* pInvoker, const MessagePtr& cb_msg_ptr)
{	
	printf("err:%s[%d] \n", cb_msg_ptr->errmsg.c_str(), cb_msg_ptr->errcode);
}

//已经提交完毕回调
void CommitOffsetcb(void* pInvoker, const MessagePtr& pMsg)
{
	//已经提交完毕，方便对账
	//printf("%s\n", cb_msg_ptr->data.c_str());
	printf("%s[%ld][%ld]\n", pMsg->topic.c_str(), ++_offsetd, pMsg->offset);
	
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

//消费回调	
void consum_on_msg(void* pInvoker, const MessagePtr& pMsg)
{
	
	printf("date: %s\n",      pMsg->data.c_str());
	printf("status: %d\n",    pMsg->status);
	printf("errcode: %d\n",   pMsg->errcode);
	printf("errmsg: %s\n",    pMsg->errmsg.c_str());
	printf("partition: %d\n", pMsg->partition);
	printf("offset: %ld\n",    pMsg->offset);
	printf("\n");

	if (pInvoker)
	{
		mwKafka_Consumer* consume = reinterpret_cast<mwKafka_Consumer*>(pInvoker);
		if (consume)
		{
			consume->CommitOffset(pMsg);//手动提交			
		}
	}
}

void test_consumer()
{
	mwKafka_Consumer consume(brokers, group_id);

	consume.AddCallBack(CONSUMED, consum_on_msg);//添加消费回调
	consume.AddCallBack(OFFSETCOMMIT, CommitOffsetcb);//添加手动提交回调，如果没有添加此回调，将会自动提交offset
	consume.AddCallBack(ONERROR, consum_on_err);//添加错误回调

	//bool bRet = consume.Subscribe({ { "TEST_08",1,1000},{"TEST_08",2,5000 } },true);//开始订阅,指定分区 和offset
	bool bRet = consume.Subscribe({ "TEST_08" });//开始订阅
	
	
	if (!bRet)
	{
		printf("%s\n", "Subscribe err");
	}
	while (true)
	{		
		consume.Poll(500);		//循环消费所有
	}
	//可以根据自己的需求 直接通过POLL来获取消费数据
	//MessagePtr pMsg = consume.Poll(1000);
	
}
#if 1
int main()
{	
	test_consumer();	
	return 0;
}
#endif
