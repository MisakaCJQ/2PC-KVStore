/*
 * 2PC协议协调者类定义
 */
#include <iostream>
#include <algorithm>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <random>
#include <ctime>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "MessageProcessor.h"

#ifndef KVSTORE_COORDINATOR_H
#define KVSTORE_COORDINATOR_H

struct addr
{
    std::string ip;
    int port;
    bool vaild;
};

class Coordinator {
public:
    Coordinator(const std::string &_ip,int _port,const std::vector<addr> &_participantAddr)
        : curAddr({_ip,_port,true}),participantAddr(_participantAddr),participantCount((int)_participantAddr.size()){}
    void startup();
private:
    void handleConnection(int connfd, sockaddr_in cliaddr);
    std::string handleSET(const std::vector<std::string> &split_message);
    std::string handleGET(const std::vector<std::string> &split_message);
    std::string handleDEL(const std::vector<std::string> &split_message);
    int waitForPrepared(std::vector<int> &participantSocket,int &aliveParticipantCount,const std::string &preparedRequestMsg);//一阶段处理
    int waitForCommitOrAbort(std::vector<int> &participantSocket,int &aliveParticipantCount,const std::string &requestMsg,std::vector<std::string> &response_split_msg);//二阶段处理
    int connectAllParticipants(std::vector<int> &participantSocket);//连接所有参与者，返回连接成功数量
    int connectParticipant(int i);//连接序号为i的参与者，返回得到的套接字，连接失败返回-1
    int getRandomID();//生成随机事务id
    int getMessage(int connfd,std::string &data) const;
    int sendMessage(int connfd,const std::string &data);
    bool checkParticipantPreparedResponse(const std::vector<int> &response);//用于在一阶段中检查所有的参与者返回是否正确
private:
    addr curAddr;
    std::vector<addr> participantAddr;//保存所有参与者的地址以及是否可用
    int participantCount;
    const unsigned int MAX_BUF_SIZE=512;
    const unsigned int TIME_OUT_SECS=1;//IO复用超时参数
    const unsigned int TIME_OUT_US=0;//IO复用超时参数
    std::random_device randev;//用于生成随机id
};


#endif //KVSTORE_COORDINATOR_H
