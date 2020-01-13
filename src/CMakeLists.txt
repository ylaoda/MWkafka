
set(SRCS
    mwkafka.cpp)	
	
	
set(LIB_SET	
	rdkafka)	
	
if(NOT CMAKE_BUILD_DYNAMIC_LIB)
	add_library(mwkfk ${SRCS})
else()
	add_library(mwkfk SHARED ${SRCS})
endif()

target_link_libraries(mwkfk ${LIB_SET})
install(TARGETS mwkfk DESTINATION lib)
