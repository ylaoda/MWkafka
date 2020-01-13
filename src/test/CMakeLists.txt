
set(SRCS
	consume.cpp
	producer.cpp)
	
set(LIB_SET	
	mwkfk
	rdkafka
	ssl
	crypto
	sasl2
	z
	dl
	pthread)


add_executable(test_kafka ${SRCS})
target_link_libraries(test_kafka ${LIB_SET})



