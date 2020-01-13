
##基于librdkafka封装
#####可以指定分区生产和消费，可以指定offset消费
使用例子

###//消费回调	
>void consum_on_msg(void* pInvoker, const MessagePtr& pMsg)
>{
>	
>	printf("date: %s\n",      pMsg->data.c_str());
>	printf("status: %d\n",    pMsg->status);
>	printf("errcode: %d\n",   pMsg->errcode);
>	printf("errmsg: %s\n",    pMsg->errmsg.c_str());
>	printf("partition: %d\n", pMsg->partition);
>	printf("offset: %ld\n",    pMsg->offset);
>	printf("\n");
>
>}

>void test_consumer()
>{
>	mwKafka_Consumer consume(brokers, group_id);
>
>	consume.AddCallBack(CONSUMED, consum_on_msg);//添加消费回调
>	consume.AddCallBack(ONERROR, consum_on_err);//添加错误回调
>
>	//bool bRet = consume.Subscribe({ { "TEST_08",1,1000},{"TEST_08",2,5000 } },true);//开始订阅,指定分区 和>offset
>	bool bRet = consume.Subscribe({ "TEST_08" });//开始订阅
>	
>	
>	if (!bRet)
>	{
>		printf("%s\n", "Subscribe err");
>	}
>	while (true)
>	{		
>		consume.Poll(500);		//循环消费所有
>	}
>	
>}


###//生产
>void producer_on_msg(void* pInvoker, const MessagePtr& pMsg)
>{	
>	/*printf("date: %s\n", pMsg->data.c_str());
>	printf("status: %d\n", pMsg->status);
>	printf("errcode: %d\n", pMsg->errcode);
>	printf("errmsg: %s\n", pMsg->errmsg.c_str());
>	printf("partition: %d\n", pMsg->partition);
>	printf("offset: %ld\n", pMsg->offset);
>	printf("\n");*/
>	
>	printf("%ld\n", ++_offset);
>}

>void sigle_producer()//单线程提交
>{	
>	mwKafka_Producer producer(brokers, true);	
>	producer.AddCallBack(DELIVERED, producer_on_msg);
>
>	producer.AddTopics( topic_nameg);
>
>	const char* text = u8"阿萨德接电话爱仕达按实际活动即可拉伸活动空间来说都阿萨德好可怜爱仕达卢卡斯";
>	int pos = 100000;
>	while (pos-- > 0)
>	{		
>		std::string _text = text +  std::to_string(pos);
>		producer.Produce(topic_nameg, _text);
>	}
>	while (true)
>	{
>		producer.Poll(500);
>	}
>}




